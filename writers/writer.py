import os
import logging
import pandas as pd
import json
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional


class FixedWidthFormatter:
    """Handles fixed-width formatting for any data format"""

    def __init__(self, output_config):
        self.column_widths = getattr(output_config, 'column_widths', {})
        self.default_width = getattr(output_config, 'default_column_width', 20)
        self.padding_char = getattr(output_config, 'padding_char', ' ')
        self.alignment = getattr(output_config, 'alignment', 'left')
        self.numeric_alignment = getattr(output_config, 'numeric_alignment', 'right')
        self.truncate_char = getattr(output_config, 'truncate_char', '…')
        self.enable_fixed_width = getattr(output_config, 'enable_fixed_width', False)

        # Auto-sizing options
        self.auto_size_columns = getattr(output_config, 'auto_size_columns', False)
        self.max_auto_width = getattr(output_config, 'max_auto_width', 50)
        self.min_auto_width = getattr(output_config, 'min_auto_width', 8)

        self._analyzed_widths = {}

    def analyze_data_for_widths(self, df: pd.DataFrame):
        """Analyze data to determine optimal column widths"""
        if not self.auto_size_columns or not self.enable_fixed_width:
            return

        for col in df.columns:
            if col not in self._analyzed_widths:
                # Calculate max width needed for this column
                col_data = df[col].astype(str)
                max_content_width = col_data.str.len().max() if len(col_data) > 0 else 0
                header_width = len(str(col))

                # Use the maximum of content width and header width
                optimal_width = max(max_content_width, header_width)

                # Apply min/max constraints
                optimal_width = max(self.min_auto_width, min(optimal_width, self.max_auto_width))

                self._analyzed_widths[col] = optimal_width

    def get_column_width(self, column_name: str) -> int:
        """Get width for a specific column"""
        # Priority: explicit config > auto-analyzed > default
        if column_name in self.column_widths:
            return self.column_widths[column_name]
        elif column_name in self._analyzed_widths:
            return self._analyzed_widths[column_name]
        else:
            return self.default_width

    def format_value(self, value: Any, width: int, alignment: str = None, is_numeric: bool = False) -> str:
        """Format a single value to fixed width"""
        if not self.enable_fixed_width:
            return str(value) if not pd.isna(value) else ''

        if pd.isna(value):
            str_value = ''
        else:
            str_value = str(value)

        # Use numeric alignment for numbers if not specified
        if alignment is None:
            alignment = self.numeric_alignment if is_numeric else self.alignment

        # Handle truncation with indicator
        if len(str_value) > width:
            if width > 1:
                str_value = str_value[:width - 1] + self.truncate_char
            else:
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

    def format_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply fixed-width formatting to entire DataFrame"""
        if not self.enable_fixed_width or df.empty:
            return df

        # Analyze data for auto-sizing
        self.analyze_data_for_widths(df)

        # Create formatted copy
        formatted_df = df.copy()

        for col in df.columns:
            width = self.get_column_width(col)
            is_numeric = pd.api.types.is_numeric_dtype(df[col])

            # Format each value in the column
            formatted_df[col] = df[col].apply(
                lambda x: self.format_value(x, width, is_numeric=is_numeric)
            )

        return formatted_df

    def format_header_row(self, columns: List[str]) -> List[str]:
        """Format header columns to fixed width"""
        if not self.enable_fixed_width:
            return columns

        formatted_headers = []
        for col in columns:
            width = self.get_column_width(col)
            formatted_col = self.format_value(col, width, self.alignment)
            formatted_headers.append(formatted_col)

        return formatted_headers


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

        # Initialize fixed-width formatter
        self.formatter = FixedWidthFormatter(output_config)

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
        # For Parquet, we can store fixed-width metadata but don't format the actual data
        # since it's a binary format optimized for compression

        if self.formatter.enable_fixed_width:
            # Analyze data for metadata
            self.formatter.analyze_data_for_widths(batch_data)

            # Store formatting info as metadata (if using pyarrow)
            try:
                import pyarrow as pa
                import pyarrow.parquet as pq

                # Convert to Arrow table
                table = pa.Table.from_pandas(batch_data, preserve_index=False)

                # Add fixed-width metadata
                metadata = table.schema.metadata or {}
                width_metadata = {}
                for col in batch_data.columns:
                    width_metadata[f"fw_width_{col}"] = str(self.formatter.get_column_width(col))
                    width_metadata[
                        f"fw_alignment_{col}"] = self.formatter.numeric_alignment if pd.api.types.is_numeric_dtype(
                        batch_data[col]) else self.formatter.alignment

                metadata.update({k.encode(): v.encode() for k, v in width_metadata.items()})
                table = table.replace_schema_metadata(metadata)

                # Write with metadata
                pq.write_table(table, file_name, compression=self.output_config.compression or 'snappy')

            except ImportError:
                # Fallback to pandas if pyarrow not available
                batch_data.to_parquet(file_name, index=False, compression=self.output_config.compression or 'snappy')
        else:
            # Standard parquet writing
            batch_data.to_parquet(file_name, index=False, compression=self.output_config.compression or 'snappy')


class SQLQueryWriter(BaseWriter):
    def __init__(self, data, table_metadata, output_config, batch_size, logger=None):
        super().__init__(data, table_metadata, output_config, batch_size, logger)

    def write_batch(self, batch_data, sql_file):
        # Apply fixed-width formatting if enabled
        if self.formatter.enable_fixed_width:
            formatted_data = self.formatter.format_dataframe(batch_data)
        else:
            formatted_data = batch_data

        with open(sql_file, 'w') as file:
            values_list = []
            for _, row in formatted_data.iterrows():
                values = ', '.join(f"""'{str(x).replace("'", "''")}'""" if pd.notnull(x) else "NULL" for x in row)
                values_list.append(f"({values})")

            # Format column names if fixed-width is enabled
            if self.formatter.enable_fixed_width:
                formatted_columns = self.formatter.format_header_row(self.columns)
                column_names = ', '.join(formatted_columns)
            else:
                column_names = ', '.join(self.columns)

            insert_stmt = f"INSERT INTO {self.table_metadata['table_name']} ({column_names}) VALUES\n"
            insert_stmt += ',\n'.join(values_list) + ";\n"
            file.write(insert_stmt)


class CSVWriter(BaseWriter):
    def __init__(self, data, table_metadata, output_config, batch_size, logger=None):
        super().__init__(data, table_metadata, output_config, batch_size, logger)

    def write_batch(self, batch_data, file_name):
        # Apply fixed-width formatting if enabled
        if self.formatter.enable_fixed_width:
            formatted_data = self.formatter.format_dataframe(batch_data)

            # Handle header formatting
            if self.output_config.include_header:
                formatted_headers = self.formatter.format_header_row(self.columns)
                # Create a temporary DataFrame with formatted headers for consistent output
                header_df = pd.DataFrame([dict(zip(self.columns, formatted_headers))])

                # Write header first
                header_df.to_csv(file_name, index=False, sep=self.output_config.csv_delimiter,
                                 quotechar=self.output_config.csv_quotechar, na_rep="NaN",
                                 compression=self.output_config.compression,
                                 encoding=self.output_config.encoding,
                                 header=False, mode='w')

                # Write data without header
                formatted_data.to_csv(file_name, index=False, sep=self.output_config.csv_delimiter,
                                      quotechar=self.output_config.csv_quotechar, na_rep="NaN",
                                      compression=self.output_config.compression,
                                      encoding=self.output_config.encoding,
                                      header=False, mode='a')
            else:
                formatted_data.to_csv(file_name, index=False, sep=self.output_config.csv_delimiter,
                                      quotechar=self.output_config.csv_quotechar, na_rep="NaN",
                                      compression=self.output_config.compression,
                                      encoding=self.output_config.encoding,
                                      header=False)
        else:
            # Standard CSV writing
            batch_data.to_csv(file_name, index=False, sep=self.output_config.csv_delimiter,
                              quotechar=self.output_config.csv_quotechar, na_rep="NaN",
                              compression=self.output_config.compression,
                              encoding=self.output_config.encoding,
                              header=self.output_config.include_header)


class JsonWriter(BaseWriter):
    def __init__(self, data, table_metadata, output_config, batch_size, logger=None):
        super().__init__(data, table_metadata, output_config, batch_size, logger)

    def write_batch(self, batch_data, file_name):
        # Apply fixed-width formatting if enabled
        if self.formatter.enable_fixed_width:
            formatted_data = self.formatter.format_dataframe(batch_data)
        else:
            formatted_data = batch_data

        # Check if we want JSONL (lines=True) or regular JSON
        use_jsonl = getattr(self.output_config, 'json_lines', True)
        json_indent = getattr(self.output_config, 'json_indent', 4 if not use_jsonl else None)

        if use_jsonl:
            # Write as JSONL (one JSON object per line)
            with open(file_name, 'w', encoding=getattr(self.output_config, 'encoding', 'utf-8')) as f:
                for _, row in formatted_data.iterrows():
                    # Convert row to dict and handle pandas/numpy types
                    row_dict = {}
                    for col, value in row.items():
                        if pd.isna(value):
                            row_dict[col] = None
                        elif isinstance(value, (pd.Timestamp, pd.Timedelta)):
                            row_dict[col] = str(value)
                        else:
                            row_dict[col] = value

                    json.dump(row_dict, f, default=str, ensure_ascii=False)
                    f.write('\n')
        else:
            # Write as regular JSON array
            formatted_data.to_json(file_name, index=False, orient='records',
                                   date_format='iso', lines=False, indent=json_indent)


class ExcelWriter(BaseWriter):
    def __init__(self, data, table_metadata, output_config, batch_size, logger=None):
        super().__init__(data, table_metadata, output_config, batch_size, logger)

        # Excel-specific configuration options
        self.sheet_name = getattr(output_config, 'sheet_name', self.table_metadata.get('table_name', 'Sheet1'))
        self.include_header = getattr(output_config, 'include_header', True)
        self.freeze_panes = getattr(output_config, 'freeze_panes', None)
        self.auto_adjust_width = getattr(output_config, 'auto_adjust_width', True)
        self.engine = getattr(output_config, 'excel_engine', 'openpyxl')
        self.date_format = getattr(output_config, 'date_format', 'YYYY-MM-DD')
        self.number_format = getattr(output_config, 'number_format', '#,##0.00')
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
                number_format = workbook.add_format({'num_format': self.number_format})

                # Apply header formatting
                if self.include_header:
                    headers = self.formatter.format_header_row(
                        self.columns) if self.formatter.enable_fixed_width else self.columns
                    for col_num, col_name in enumerate(headers):
                        worksheet.write(0, col_num, col_name, header_format)

                # Apply number formatting and column widths
                for col_num, col_name in enumerate(batch_data.columns):
                    if pd.api.types.is_numeric_dtype(batch_data[col_name]):
                        col_letter = chr(65 + col_num)
                        worksheet.set_column(f'{col_letter}:{col_letter}', None, number_format)

                    # Set column width based on fixed-width settings or auto-adjust
                    if self.formatter.enable_fixed_width:
                        width = self.formatter.get_column_width(col_name)
                        worksheet.set_column(col_num, col_num, width)
                    elif self.auto_adjust_width:
                        max_width = max(
                            len(str(col_name)) if self.include_header else 0,
                            batch_data[col_name].astype(str).str.len().max() if not batch_data.empty else 0
                        )
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
            # Apply fixed-width formatting if enabled
            if self.formatter.enable_fixed_width:
                formatted_data = self.formatter.format_dataframe(batch_data)
            else:
                formatted_data = batch_data

            file_extension = os.path.splitext(file_name)[1].lower()

            if file_extension == '.xlsx':
                engine = 'xlsxwriter' if hasattr(pd, 'ExcelWriter') else 'openpyxl'
            elif file_extension == '.xls':
                engine = 'xlwt'
            else:
                engine = 'xlsxwriter' if hasattr(pd, 'ExcelWriter') else 'openpyxl'
                if not file_name.endswith(('.xlsx', '.xls')):
                    file_name = file_name + '.xlsx'

            with pd.ExcelWriter(file_name, engine=engine,
                                date_format=self.date_format,
                                options={'remove_timezone': True} if engine == 'xlsxwriter' else {}) as writer:

                formatted_data.to_excel(writer,
                                        sheet_name=self.sheet_name,
                                        index=False,
                                        header=self.include_header,
                                        na_rep='')

                if engine == 'xlsxwriter':
                    self._apply_formatting(writer, batch_data, self.sheet_name)

        except ImportError as e:
            self.logger.error(f"Required Excel library not installed: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error writing Excel file: {e}")
            raise

    def write_data(self):
        """Override write_data for Excel to handle multiple sheets if needed."""
        self.logger.info(f'Writing data in Excel format')
        max_rows_per_sheet = getattr(self.output_config, 'max_rows_per_sheet', 1048576)

        if len(self.data) <= max_rows_per_sheet:
            path = self._prepare_file_name()
            for index, (batch_data, file_name) in enumerate(self.generate_batch()):
                self.write_batch(batch_data, file_name)
            self.logger.info(f"Data written to {path} with {len(self.data)} records")
        else:
            self.logger.info(f"Large dataset detected ({len(self.data)} rows). Splitting into multiple sheets.")
            sheet_num = 1
            for start in range(0, len(self.data), max_rows_per_sheet):
                end = min(start + max_rows_per_sheet, len(self.data))
                sheet_data = self.data.iloc[start:end]

                original_sheet_name = self.sheet_name
                self.sheet_name = f"{original_sheet_name}_Part{sheet_num}"

                file_name = self._prepare_file_name(sheet_num)
                self.write_batch(sheet_data, file_name)

                self.sheet_name = original_sheet_name
                sheet_num += 1

            self.logger.info(f"Data written to {sheet_num - 1} Excel files with {len(self.data)} total records")


class FixedWidthWriter(BaseWriter):
    def __init__(self, data, table_metadata, output_config, batch_size, logger=None):
        super().__init__(data, table_metadata, output_config, batch_size, logger)

        # Force enable fixed-width for this writer
        self.formatter.enable_fixed_width = True

        # Legacy compatibility - keep original properties
        self.column_widths = getattr(output_config, 'column_widths', {})
        self.default_width = getattr(output_config, 'default_column_width', 20)
        self.padding_char = getattr(output_config, 'padding_char', ' ')
        self.alignment = getattr(output_config, 'alignment', 'left')
        self.include_header = getattr(output_config, 'include_header', True)

    def _format_value(self, value, width, alignment='left'):
        """Legacy method - delegates to formatter"""
        return self.formatter.format_value(value, width, alignment)

    def _get_column_width(self, column_name):
        """Legacy method - delegates to formatter"""
        return self.formatter.get_column_width(column_name)

    def _format_header(self):
        """Format the header row."""
        if not self.include_header:
            return ''

        formatted_headers = self.formatter.format_header_row(self.columns)
        return ''.join(formatted_headers) + '\n'

    def _format_row(self, row):
        """Format a single data row using the formatter."""
        row_parts = []
        for i, (col, value) in enumerate(zip(self.columns, row)):
            width = self.formatter.get_column_width(col)
            is_numeric = pd.api.types.is_numeric_dtype(self.data[col])
            formatted_value = self.formatter.format_value(value, width, is_numeric=is_numeric)
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


# ===== FACTORY FUNCTION =====

def create_writer(data, table_metadata, output_config, batch_size=1000, logger=None):
    """Factory function to create appropriate writer based on format"""

    format_type = getattr(output_config, 'format', 'csv').lower()

    writers = {
        'csv': CSVWriter,
        'tsv': CSVWriter,  # TSV is just CSV with tab delimiter
        'json': JsonWriter,
        'jsonl': JsonWriter,
        'parquet': ParquetWriter,
        'excel': ExcelWriter,
        'xlsx': ExcelWriter,
        'xls': ExcelWriter,
        'fixed': FixedWidthWriter,
        'fwf': FixedWidthWriter,
        'fixed_width': FixedWidthWriter,
        'sql': SQLQueryWriter
    }

    writer_class = writers.get(format_type)
    if not writer_class:
        raise ValueError(f"Unsupported format: {format_type}")

    return writer_class(data, table_metadata, output_config, batch_size, logger)


# ===== EXAMPLE CONFIGURATION CLASS =====

class EnhancedOutputConfig:
    """Example configuration class with fixed-width support"""

    def __init__(self):
        # Basic settings
        self.format = 'csv'
        self.include_header = True
        self.encoding = 'utf-8'
        self.compression = None

        # CSV settings
        self.csv_delimiter = ','
        self.csv_quotechar = '"'

        # JSON settings
        self.json_lines = True
        self.json_indent = None

        # Fixed-width settings
        self.enable_fixed_width = False
        self.column_widths = {}
        self.default_column_width = 20
        self.padding_char = ' '
        self.alignment = 'left'
        self.numeric_alignment = 'right'
        self.truncate_char = '…'

        # Auto-sizing settings
        self.auto_size_columns = False
        self.max_auto_width = 50
        self.min_auto_width = 8

        # Excel settings
        self.sheet_name = 'Sheet1'
        self.excel_engine = 'openpyxl'
        self.auto_adjust_width = True
        self.freeze_panes = None

    def get_output_path(self, table_name):
        """Get output file path for table"""
        return f"{table_name}.{self.format}"


# ===== USAGE EXAMPLES =====

def example_csv_with_fixed_width():
    """Example: CSV with fixed-width formatting"""

    # Sample data
    data = [
        {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'balance': 1234.56},
        {'id': 2, 'name': 'Jane Smith', 'email': 'jane@verylongemailaddress.com', 'balance': 987.65},
        {'id': 3, 'name': 'Bob Wilson', 'email': 'bob@test.org', 'balance': 543.21}
    ]

    # Configure for fixed-width CSV
    config = EnhancedOutputConfig()
    config.format = 'csv'
    config.enable_fixed_width = True
    config.auto_size_columns = True
    config.column_widths = {'id': 5, 'name': 15, 'email': 25, 'balance': 10}

    # Create and use writer
    table_metadata = {'table_name': 'users'}
    writer = create_writer(data, table_metadata, config)
    writer.write_data()

    print("Fixed-width CSV created: users.csv")


def example_multiple_formats():
    """Example: Same data in multiple formats with fixed-width"""

    data = [
        {'product_id': 101, 'name': 'Widget A', 'price': 29.99, 'category': 'Tools'},
        {'product_id': 102, 'name': 'Super Long Product Name Here', 'price': 149.50, 'category': 'Electronics'},
        {'product_id': 103, 'name': 'Item C', 'price': 5.25, 'category': 'Office'}
    ]

    table_metadata = {'table_name': 'products'}

    # Configure fixed-width settings
    base_config = EnhancedOutputConfig()
    base_config.enable_fixed_width = True
    base_config.auto_size_columns = True
    base_config.max_auto_width = 30

    # Write to different formats
    formats = ['csv', 'json', 'fixed']

    for fmt in formats:
        config = EnhancedOutputConfig()
        config.__dict__.update(base_config.__dict__)  # Copy settings
        config.format = fmt

        writer = create_writer(data, table_metadata, config)
        writer.write_data()

        print(f"Created {fmt} file: products.{fmt}")


if __name__ == "__main__":
    print("Running fixed-width formatting examples...")
    example_csv_with_fixed_width()
    print()
    example_multiple_formats()
    print("Done!")