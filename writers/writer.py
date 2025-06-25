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
        batch_data.to_csv(file_name, index=False, sep=self.output_config.csv_delimiter,
                          quotechar=self.output_config.csv_quotechar, na_rep="NaN",
                          compression=self.output_config.compression, encoding=self.output_config.encoding,
                          header=self.output_config.include_header)


class JsonWriter(BaseWriter):
    def __init__(self, data, table_metadata, output_config, batch_size, logger=None):
        super().__init__(data, table_metadata, output_config, batch_size, logger)

    def write_batch(self, batch_data, file_name):
        batch_data.to_json(file_name, index=False, orient='records', date_format='iso', lines=True, indent=4)


class ExcelWriter(BaseWriter):
    def __init__(self, data, table_metadata, output_config, batch_size, logger=None):
        super().__init__(data, table_metadata, output_config, batch_size, logger)

        # Excel-specific configuration options
        self.sheet_name = getattr(output_config, 'sheet_name', self.table_metadata.get('table_name', 'Sheet1'))
        self.include_header = getattr(output_config, 'include_header', True)
        self.freeze_panes = getattr(output_config, 'freeze_panes', None)  # e.g., (1, 0) to freeze header row
        self.auto_adjust_width = getattr(output_config, 'auto_adjust_width', True)

        # Excel engine ('openpyxl' for .xlsx, 'xlwt' for .xls)
        self.engine = getattr(output_config, 'excel_engine', 'openpyxl')

        # Date format for Excel
        self.date_format = getattr(output_config, 'date_format', 'YYYY-MM-DD')

        # Number format for numeric columns
        self.number_format = getattr(output_config, 'number_format', '#,##0.00')

        # Header formatting options
        self.header_format = getattr(output_config, 'header_format', {
            'bold': True,
            'bg_color': '#D7E4BC',
            'font_color': '#000000'
        })

    def _apply_formatting(self, writer, batch_data, sheet_name):
        """Apply formatting to the Excel file using xlsxwriter or openpyxl."""
        try:
            if self.engine == 'xlsxwriter':
                workbook = writer.book
                worksheet = writer.sheets[sheet_name]

                # Create header format
                header_format = workbook.add_format(self.header_format)

                # Create number format
                number_format = workbook.add_format({'num_format': self.number_format})

                # Apply header formatting
                if self.include_header:
                    for col_num, col_name in enumerate(batch_data.columns):
                        worksheet.write(0, col_num, col_name, header_format)

                # Apply number formatting to numeric columns
                for col_num, col_name in enumerate(batch_data.columns):
                    if pd.api.types.is_numeric_dtype(batch_data[col_name]):
                        col_letter = chr(65 + col_num)  # Convert to Excel column letter
                        worksheet.set_column(f'{col_letter}:{col_letter}', None, number_format)

                # Auto-adjust column widths
                if self.auto_adjust_width:
                    for col_num, col_name in enumerate(batch_data.columns):
                        # Calculate max width needed
                        max_width = max(
                            len(str(col_name)) if self.include_header else 0,
                            batch_data[col_name].astype(str).str.len().max() if not batch_data.empty else 0
                        )
                        # Add some padding and set reasonable limits
                        width = min(max(max_width + 2, 10), 50)
                        worksheet.set_column(col_num, col_num, width)

                # Freeze panes if specified
                if self.freeze_panes:
                    worksheet.freeze_panes(*self.freeze_panes)

        except Exception as e:
            self.logger.warning(f"Could not apply Excel formatting: {e}")

    def write_batch(self, batch_data, file_name):
        """Write a batch of data to Excel format."""
        try:
            # For Excel files, we'll use different engines based on file extension
            file_extension = os.path.splitext(file_name)[1].lower()

            if file_extension == '.xlsx':
                engine = 'xlsxwriter' if hasattr(pd, 'ExcelWriter') else 'openpyxl'
            elif file_extension == '.xls':
                engine = 'xlwt'
            else:
                # Default to xlsx
                engine = 'xlsxwriter' if hasattr(pd, 'ExcelWriter') else 'openpyxl'
                if not file_name.endswith(('.xlsx', '.xls')):
                    file_name = file_name + '.xlsx'

            # Write to Excel
            with pd.ExcelWriter(file_name, engine=engine,
                                date_format=self.date_format,
                                options={'remove_timezone': True} if engine == 'xlsxwriter' else {}) as writer:

                batch_data.to_excel(writer,
                                    sheet_name=self.sheet_name,
                                    index=False,
                                    header=self.include_header,
                                    na_rep='')

                # Apply formatting if using xlsxwriter
                if engine == 'xlsxwriter':
                    self._apply_formatting(writer, batch_data, self.sheet_name)

        except ImportError as e:
            self.logger.error(f"Required Excel library not installed: {e}")
            self.logger.error("Please install openpyxl (pip install openpyxl) or xlsxwriter (pip install xlsxwriter)")
            raise
        except Exception as e:
            self.logger.error(f"Error writing Excel file: {e}")
            raise

    def write_data(self):
        """Override write_data for Excel to handle multiple sheets if needed."""
        self.logger.info(f'Writing data in Excel format')

        # For Excel, we might want to handle large datasets differently
        # If data is too large, we can split into multiple sheets
        max_rows_per_sheet = getattr(self.output_config, 'max_rows_per_sheet', 1048576)  # Excel limit

        if len(self.data) <= max_rows_per_sheet:
            # Single sheet
            path = self._prepare_file_name()
            for index, (batch_data, file_name) in enumerate(self.generate_batch()):
                self.write_batch(batch_data, file_name)
            self.logger.info(f"Data written to {path} with {len(self.data)} records")
        else:
            # Multiple sheets
            self.logger.info(f"Large dataset detected ({len(self.data)} rows). Splitting into multiple sheets.")
            sheet_num = 1
            for start in range(0, len(self.data), max_rows_per_sheet):
                end = min(start + max_rows_per_sheet, len(self.data))
                sheet_data = self.data.iloc[start:end]

                # Temporarily change sheet name for this batch
                original_sheet_name = self.sheet_name
                self.sheet_name = f"{original_sheet_name}_Part{sheet_num}"

                file_name = self._prepare_file_name(sheet_num)
                self.write_batch(sheet_data, file_name)

                # Restore original sheet name
                self.sheet_name = original_sheet_name
                sheet_num += 1

            self.logger.info(f"Data written to {sheet_num - 1} Excel files with {len(self.data)} total records")


class FixedWidthWriter(BaseWriter):
    def __init__(self, data, table_metadata, output_config, batch_size, logger=None):
        super().__init__(data, table_metadata, output_config, batch_size, logger)

        # Define column widths - you can configure these in output_config
        self.column_widths = getattr(output_config, 'column_widths', {})
        self.default_width = getattr(output_config, 'default_column_width', 20)
        self.padding_char = getattr(output_config, 'padding_char', ' ')
        self.alignment = getattr(output_config, 'alignment', 'left')  # 'left', 'right', 'center'
        self.include_header = getattr(output_config, 'include_header', True)

    def _format_value(self, value, width, alignment='left'):
        """Format a single value to fixed width with specified alignment."""
        if pd.isnull(value):
            str_value = ''
        else:
            str_value = str(value)

        # Truncate if too long
        if len(str_value) > width:
            str_value = str_value[:width]

        # Apply alignment and padding
        if alignment == 'left':
            return str_value.ljust(width, self.padding_char)
        elif alignment == 'right':
            return str_value.rjust(width, self.padding_char)
        elif alignment == 'center':
            return str_value.center(width, self.padding_char)
        else:
            return str_value.ljust(width, self.padding_char)

    def _get_column_width(self, column_name):
        """Get the width for a specific column."""
        return self.column_widths.get(column_name, self.default_width)

    def _format_header(self):
        """Format the header row."""
        header_parts = []
        for col in self.columns:
            width = self._get_column_width(col)
            formatted_col = self._format_value(col, width, self.alignment)
            header_parts.append(formatted_col)
        return ''.join(header_parts) + '\n'

    def _format_row(self, row):
        """Format a single data row."""
        row_parts = []
        for i, (col, value) in enumerate(zip(self.columns, row)):
            width = self._get_column_width(col)
            # For numeric columns, you might want right alignment
            col_alignment = self.alignment
            if pd.api.types.is_numeric_dtype(self.data[col]):
                col_alignment = getattr(self.output_config, 'numeric_alignment', 'right')

            formatted_value = self._format_value(value, width, col_alignment)
            row_parts.append(formatted_value)
        return ''.join(row_parts) + '\n'

    def write_batch(self, batch_data, file_name):
        """Write a batch of data to fixed-width format."""
        with open(file_name, 'w', encoding=getattr(self.output_config, 'encoding', 'utf-8')) as file:
            # Write header if requested
            if self.include_header:
                file.write(self._format_header())

            # Write data rows
            for _, row in batch_data.iterrows():
                file.write(self._format_row(row.values))