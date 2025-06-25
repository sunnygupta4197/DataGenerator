import os
import logging
import pandas as pd
from abc import ABC, abstractmethod

class BaseWriter(ABC):
    def __init__(self, data, table_metadata, output_config, batch_size=1000, logger=None):
        self.data = pd.DataFrame(data)
        self.columns = self.data.columns.tolist()
        self.table_metadata = table_metadata
        self.output_config = output_config
        self.batch_size = batch_size
        self.logger = logger
        if self.logger is None:
            logger = logging.getLogger()
            self.logger = logger

    def _prepare_file_name(self, chunk_id=0):
        self.logger.info('Preparing file name')
        filename = self.output_config.get_output_path(self.table_metadata.get('table_name', 'unknown'))
        if filename is None:
            filename = [self.table_metadata["table_name"]]
            if chunk_id:
                filename.append(f"_part{chunk_id}")
            filename.append(f".{self.output_config.format}")
            filename = ''.join(filename)
        self.logger.info(f'File name: {filename}')
        return filename

    def generate_batch(self):
        if len(self.data) // self.batch_size == 0:
            yield self.data, self._prepare_file_name()
        else:
            chunk_id = 0
            for start in range(0, len(self.data), self.batch_size):
                batch = self.data.iloc[start:start + self.batch_size]
                chunk_id += 1
                yield batch, self._prepare_file_name(chunk_id)

    @abstractmethod
    def write_batch(self, batch_data, file_name):
        pass

    def write_data(self):
        self.logger.info(f'Writing data in {self.output_config.format} format')
        path = self._prepare_file_name()
        for index, (batch_data, file_name) in enumerate(self.generate_batch()):
            self.write_batch(batch_data, file_name)
        self.logger.info(f"Data written to {path} with {len(self.data)} records")


class ParquetWriter(BaseWriter):
    def __init__(self, data, table_metadata, output_config, batch_size, logger=None):
        super().__init__(data, table_metadata, output_config, batch_size, logger)

    def write_batch(self, batch_data, file_name):
        batch_data.to_parquet(file_name, index=False, compression=self.output_config.compression or 'snappy')


class SQLQueryWriter(BaseWriter):
    def __init__(self, data, table_metadata, output_config, batch_size, logger=None):
        super().__init__(data, table_metadata, output_config, batch_size, logger)

    def write_batch(self, batch_data, sql_file):
        with open(sql_file, 'w') as file:
            values_list = []
            for _, row in batch_data.iterrows():
                values = ', '.join(f"""'{str(x).replace("'", "''")}'""" if pd.notnull(x) else "NULL" for x in row)
                values_list.append(f"({values})")
            insert_stmt = f"INSERT INTO {self.table_metadata['table_name']} ({', '.join(self.columns)}) VALUES ({', '.join(values_list)});\n"
            file.write(insert_stmt)


class CSVWriter(BaseWriter):
    def __init__(self, data, table_metadata, output_config, batch_size, logger=None):
        super().__init__(data, table_metadata, output_config, batch_size, logger)

    def write_batch(self, batch_data, file_name):
        batch_data.to_csv(file_name, index=False, sep=self.output_config.csv_delimiter, quotechar=self.output_config.csv_quotechar, na_rep="NaN", compression=self.output_config.compression, encoding=self.output_config.encoding, header=self.output_config.include_header)


class JsonWriter(BaseWriter):
    def __init__(self, data, table_metadata, output_config, batch_size, logger=None):
        super().__init__(data, table_metadata, output_config, batch_size, logger)

    def write_batch(self, batch_data, file_name):
        batch_data.to_json(file_name, index=False, orient='records', date_format='iso', lines=True, indent=4)