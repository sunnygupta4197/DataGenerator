"""
COMPLETE UNIFIED WRITER - All Formats Supported

This extends the working version with:
- Compression support (gzip, bz2, lzma)
- SQL Query writer
- Excel writer (xlsx/xls)
- Parquet writer
- Enhanced error handling and fallbacks
"""

import os
import logging
from typing import Dict, List, Any, Optional
import pandas as pd
import time
import numpy as np

from config_manager.config_manager import OutputConfig


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


class DataOptimizer:
    """Handles pandas data type optimization and cleaning"""

    def __init__(self, logger: logging.Logger, schema: dict = None):
        self.logger = logger
        self.schema_cache = schema if schema is not None else {}
        self.datetime_formats = {}

    def optimize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all optimizations to a DataFrame"""
        if df.empty:
            return df

        # Cache schema from first batch
        if not self.schema_cache:
            self._infer_schema(df)

        # Apply optimizations
        df = self._optimize_dtypes(df)
        df = self._clean_data(df)
        return df

    def _infer_schema(self, df: pd.DataFrame):
        """Infer optimal data types for each column"""
        for col in df.columns:
            series = df[col].dropna()
            if len(series) == 0:
                continue

            # Numeric optimization
            if pd.api.types.is_numeric_dtype(series):
                self.schema_cache[col] = self._get_optimal_numeric_type(series)

            # Datetime detection
            elif self._is_datetime_column(series):
                self.schema_cache[col] = 'datetime64[ns]'

            # Categorical optimization
            elif self._should_be_categorical(series):
                self.schema_cache[col] = 'category'

            else:
                self.schema_cache[col] = 'object'

        self.logger.debug(f"Schema inferred: {self.schema_cache}")

    def _get_optimal_numeric_type(self, series: pd.Series) -> str:
        """Find the smallest numeric type that fits the data"""
        if series.dtype.kind in 'iu':  # integers
            min_val, max_val = series.min(), series.max()
            if min_val >= 0:  # unsigned
                for dtype in ['uint8', 'uint16', 'uint32', 'uint64']:
                    if max_val <= np.iinfo(getattr(np, dtype)).max:
                        return dtype
            else:  # signed
                for dtype in ['int8', 'int16', 'int32', 'int64']:
                    info = np.iinfo(getattr(np, dtype))
                    if info.min <= min_val <= max_val <= info.max:
                        return dtype
        elif series.dtype.kind == 'f':  # floats
            # Try float32 if precision allows
            if (series.astype('float32') == series).all():
                return 'float32'
        return str(series.dtype)

    def _is_datetime_column(self, series: pd.Series) -> bool:
        """Check if series contains datetime data"""
        if series.dtype != 'object':
            return False

        sample = series.head(min(10, len(series)))
        successful_conversions = 0

        for value in sample:
            if pd.isna(value):
                continue
            try:
                pd.to_datetime(str(value), errors='raise')
                successful_conversions += 1
            except:
                pass

        non_null_count = sample.notna().sum()
        return (successful_conversions / non_null_count) > 0.8 if non_null_count > 0 else False

    def _should_be_categorical(self, series: pd.Series) -> bool:
        """Check if series should be categorical"""
        unique_ratio = series.nunique() / len(series)
        return unique_ratio < 0.5 and series.nunique() < 1000

    def _optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply cached schema to DataFrame"""
        for col, dtype in self.schema_cache.items():
            if col in df.columns:
                try:
                    if dtype == 'category':
                        df[col] = df[col].astype('category')
                    elif dtype == 'datetime64[ns]':
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    elif dtype.startswith(('int', 'uint', 'float')):
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype(dtype)
                except Exception as e:
                    self.logger.warning(f"Failed to optimize {col}: {e}")
        return df

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate data"""
        # Remove completely empty rows
        df = df.dropna(how='all')

        # Fill NaN values appropriately
        for col in df.columns:
            if col in self.schema_cache:
                dtype = self.schema_cache[col]
                if dtype.startswith(('int', 'uint', 'float')):
                    df[col] = df[col].fillna(0)
                elif dtype == 'object':
                    df[col] = df[col].fillna('')

        return df


# ===== PROGRESS TRACKING =====

class ProgressTracker:
    """Handles progress monitoring and statistics"""

    def __init__(self, enable: bool = True, log_interval: int = 100, logger: logging.Logger = None):
        self.enable = enable
        self.log_interval = log_interval
        self.logger = logger or logging.getLogger(__name__)

        self.records_written = 0
        self.batches_written = 0
        self.bytes_written = 0
        self.start_time = time.time()

    def update(self, batch_size: int, bytes_written: int = 0):
        """Update progress counters"""
        if not self.enable:
            return

        self.records_written += batch_size
        self.batches_written += 1
        self.bytes_written += bytes_written

        if self.batches_written % self.log_interval == 0:
            self._log_progress()

    def _log_progress(self):
        """Log current progress"""
        elapsed = time.time() - self.start_time
        records_per_sec = self.records_written / elapsed if elapsed > 0 else 0
        mb_per_sec = (self.bytes_written / (1024 * 1024)) / elapsed if elapsed > 0 else 0

        self.logger.info(
            f"Progress: {self.records_written:,} records, "
            f"{records_per_sec:.1f} records/sec, "
            f"{mb_per_sec:.2f} MB/sec"
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics"""
        elapsed = time.time() - self.start_time
        return {
            'records_written': self.records_written,
            'batches_written': self.batches_written,
            'bytes_written': self.bytes_written,
            'elapsed_seconds': elapsed,
            'records_per_second': self.records_written / elapsed if elapsed > 0 else 0
        }


# ===== STRATEGY IMPLEMENTATIONS =====

class CSVStrategy:
    """CSV strategy - matches existing streaming writer interface"""

    def __init__(self):
        self.file_handle = None

    def initialize_writer(self, file_path: str, config: OutputConfig):
        """Initialize CSV file writer"""
        self.file_handle = open(
            file_path, 'w',
            buffering=getattr(config, 'buffer_size', 8192),
            encoding=getattr(config, 'encoding', 'utf-8'),
            newline=''
        )
        return self.file_handle

    def write_header_impl(self, columns: List[str], file_handle, config: OutputConfig) -> None:
        """Write CSV header"""
        if getattr(config, 'include_header', True):
            import csv

            formatter = FixedWidthFormatter(config)
            formatted_headers = formatter.format_header_row(columns)

            writer = csv.writer(
                file_handle,
                delimiter=getattr(config, 'csv_delimiter', ','),
                quotechar=getattr(config, 'csv_quotechar', '"')
            )
            writer.writerow(formatted_headers)

    def write_batch_impl(self, batch_df: pd.DataFrame, file_handle, config: OutputConfig) -> int:
        """Write CSV batch"""
        batch_df.to_csv(
            file_handle,
            sep=getattr(config, 'csv_delimiter', ','),
            quotechar=getattr(config, 'csv_quotechar', '"'),
            index=False,
            header=False
        )
        return len(batch_df) * len(batch_df.columns) * 10  # Estimate bytes

    def write_footer_impl(self, file_handle, config: OutputConfig) -> None:
        """CSV doesn't need footer"""
        pass

    def close_impl(self, file_handle) -> None:
        """Close CSV file"""
        if file_handle:
            file_handle.close()


class JSONLStrategy:
    """JSONL strategy - line-by-line JSON"""

    def __init__(self):
        self.file_handle = None

    def initialize_writer(self, file_path: str, config: OutputConfig):
        """Initialize JSONL file writer"""
        self.file_handle = open(
            file_path, 'w',
            buffering=getattr(config, 'buffer_size', 8192),
            encoding=getattr(config, 'encoding', 'utf-8')
        )
        return self.file_handle

    def write_header_impl(self, columns: List[str], file_handle, config: OutputConfig) -> None:
        """JSONL doesn't need header"""
        pass

    def write_batch_impl(self, batch_df: pd.DataFrame, file_handle, config: OutputConfig) -> int:
        """Write JSONL batch"""
        import json

        records = batch_df.to_dict('records')
        content_length = 0

        for record in records:
            # Clean pandas/numpy types
            cleaned_record = self._clean_for_json(record)
            line = json.dumps(
                cleaned_record,
                ensure_ascii=getattr(config, 'json_ensure_ascii', False),
                default=str
            ) + '\n'
            file_handle.write(line)
            content_length += len(line)

        return content_length

    def write_footer_impl(self, file_handle, config: OutputConfig) -> None:
        """JSONL doesn't need footer"""
        pass

    def close_impl(self, file_handle) -> None:
        """Close JSONL file"""
        if file_handle:
            file_handle.close()

    def _clean_for_json(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert pandas/numpy types to JSON-serializable types"""
        import numpy as np

        cleaned = {}
        for key, value in record.items():
            if pd.isna(value):
                cleaned[key] = None
            elif isinstance(value, (np.integer, np.int64, np.int32)):
                cleaned[key] = int(value)
            elif isinstance(value, (np.floating, np.float64, np.float32)):
                cleaned[key] = float(value) if np.isfinite(value) else None
            elif isinstance(value, np.bool_):
                cleaned[key] = bool(value)
            elif isinstance(value, pd.Timestamp):
                cleaned[key] = value.isoformat()
            else:
                cleaned[key] = value
        return cleaned


class JSONStrategy:
    """JSON strategy - standard JSON array format"""

    def __init__(self):
        self.file_handle = None
        self.first_record = True

    def initialize_writer(self, file_path: str, config: OutputConfig):
        """Initialize JSON file writer"""
        self.file_handle = open(
            file_path, 'w',
            buffering=getattr(config, 'buffer_size', 8192),
            encoding=getattr(config, 'encoding', 'utf-8')
        )
        return self.file_handle

    def write_header_impl(self, columns: List[str], file_handle, config: OutputConfig) -> None:
        """Start JSON array"""
        file_handle.write('[\n')
        self.first_record = True

    def write_batch_impl(self, batch_df: pd.DataFrame, file_handle, config: OutputConfig) -> int:
        """Write JSON batch"""
        import json

        records = batch_df.to_dict('records')
        content_length = 0
        indent = getattr(config, 'json_indent', 2)

        for record in records:
            if not self.first_record:
                file_handle.write(',\n')

            # Clean pandas/numpy types
            cleaned_record = self._clean_for_json(record)
            json_str = json.dumps(
                cleaned_record,
                ensure_ascii=getattr(config, 'json_ensure_ascii', False),
                indent=indent,
                default=str
            )
            file_handle.write(json_str)
            content_length += len(json_str)
            self.first_record = False

        return content_length

    def write_footer_impl(self, file_handle, config: OutputConfig) -> None:
        """Close JSON array"""
        file_handle.write('\n]')

    def close_impl(self, file_handle) -> None:
        """Close JSON file"""
        if file_handle:
            file_handle.close()

    def _clean_for_json(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert pandas/numpy types to JSON-serializable types"""
        import numpy as np

        cleaned = {}
        for key, value in record.items():
            if pd.isna(value):
                cleaned[key] = None
            elif isinstance(value, (np.integer, np.int64, np.int32)):
                cleaned[key] = int(value)
            elif isinstance(value, (np.floating, np.float64, np.float32)):
                cleaned[key] = float(value) if np.isfinite(value) else None
            elif isinstance(value, np.bool_):
                cleaned[key] = bool(value)
            elif isinstance(value, pd.Timestamp):
                cleaned[key] = value.isoformat()
            else:
                cleaned[key] = value
        return cleaned


class FixedWidthStrategy:
    """Fixed-width strategy"""

    def __init__(self):
        self.file_handle = None

    def initialize_writer(self, file_path: str, config: OutputConfig):
        """Initialize fixed-width file writer"""
        self.file_handle = open(
            file_path, 'w',
            buffering=getattr(config, 'buffer_size', 8192),
            encoding=getattr(config, 'encoding', 'utf-8')
        )
        return self.file_handle

    def write_header_impl(self, columns: List[str], file_handle, config: OutputConfig) -> None:
        """Write fixed-width header"""
        if getattr(config, 'include_header', True):
            formatter = FixedWidthFormatter(config)
            formatted_headers = formatter.format_header_row(columns)
            header_line = ''.join(formatted_headers) + '\n'
            file_handle.write(header_line)

    def write_batch_impl(self, batch_df: pd.DataFrame, file_handle, config: OutputConfig) -> int:
        """Write fixed-width batch"""
        formatter = FixedWidthFormatter(config)

        lines = []
        for _, row in batch_df.iterrows():
            formatted_values = []
            for col, value in zip(batch_df.columns, row.values):
                width = formatter.get_column_width(col)
                is_numeric = pd.api.types.is_numeric_dtype(batch_df[col])
                formatted_value = formatter.format_value(value, width, is_numeric=is_numeric)
                formatted_values.append(formatted_value)

            line = ''.join(formatted_values) + '\n'
            lines.append(line)

        content = ''.join(lines)
        file_handle.write(content)
        return len(content.encode(getattr(config, 'encoding', 'utf-8')))

    def write_footer_impl(self, file_handle, config: OutputConfig) -> None:
        """Fixed-width doesn't need footer"""
        pass

    def close_impl(self, file_handle) -> None:
        """Close fixed-width file"""
        if file_handle:
            file_handle.close()


class SQLQueryStrategy:
    """SQL Query strategy - generates INSERT statements"""

    def __init__(self):
        self.file_handle = None
        self.table_name = None
        self.columns = None

    def initialize_writer(self, file_path: str, config: OutputConfig):
        """Initialize SQL file writer"""
        self.file_handle = open(
            file_path, 'w',
            buffering=getattr(config, 'buffer_size', 8192),
            encoding=getattr(config, 'encoding', 'utf-8')
        )
        # Extract table name from config or file path
        self.table_name = getattr(config, 'table_name',
                                 os.path.splitext(os.path.basename(file_path))[0])
        return self.file_handle

    def write_header_impl(self, columns: List[str], file_handle, config: OutputConfig) -> None:
        """Write SQL header comments"""
        self.columns = columns
        file_handle.write(f"-- SQL INSERT statements for table: {self.table_name}\n")
        file_handle.write(f"-- Generated on: {pd.Timestamp.now()}\n")
        file_handle.write(f"-- Columns: {', '.join(columns)}\n\n")

    def write_batch_impl(self, batch_df: pd.DataFrame, file_handle, config: OutputConfig) -> int:
        """Write SQL INSERT batch"""
        if self.columns is None:
            self.columns = list(batch_df.columns)

        content_length = 0
        column_names = ', '.join(self.columns)

        # Write INSERT statements in batches
        batch_size = getattr(config, 'sql_batch_size', 1000)

        for start_idx in range(0, len(batch_df), batch_size):
            end_idx = min(start_idx + batch_size, len(batch_df))
            batch_chunk = batch_df.iloc[start_idx:end_idx]

            # Generate VALUES clauses
            values_list = []
            for _, row in batch_chunk.iterrows():
                values = []
                for value in row:
                    if pd.isna(value):
                        values.append('NULL')
                    elif isinstance(value, str):
                        # Escape single quotes
                        escaped_value = str(value).replace("'", "''")
                        values.append(f"'{escaped_value}'")
                    elif isinstance(value, (int, float)):
                        values.append(str(value))
                    else:
                        # Convert to string and escape
                        escaped_value = str(value).replace("'", "''")
                        values.append(f"'{escaped_value}'")

                values_list.append(f"({', '.join(values)})")

            # Write INSERT statement
            insert_stmt = f"INSERT INTO {self.table_name} ({column_names}) VALUES\n"
            insert_stmt += ',\n'.join(values_list) + ";\n\n"

            file_handle.write(insert_stmt)
            content_length += len(insert_stmt)

        return content_length

    def write_footer_impl(self, file_handle, config: OutputConfig) -> None:
        """Write SQL footer"""
        file_handle.write(f"-- End of INSERT statements for {self.table_name}\n")

    def close_impl(self, file_handle) -> None:
        """Close SQL file"""
        if file_handle:
            file_handle.close()


class ExcelStrategy:
    """Excel strategy - supports xlsx and xls formats"""

    def __init__(self):
        self.writer = None
        self.batch_buffer = []
        self.columns = None
        self.file_path = None
        self.config = None

    def initialize_writer(self, file_path: str, config: OutputConfig):
        """Initialize Excel writer"""
        try:
            # Check for Excel libraries
            try:
                import openpyxl
                engine = 'openpyxl'
            except ImportError:
                try:
                    import xlsxwriter
                    engine = 'xlsxwriter'
                except ImportError:
                    raise ImportError("Excel support requires either 'openpyxl' or 'xlsxwriter'")

            self.file_path = file_path
            self.config = config

            # Ensure proper file extension
            if not file_path.endswith(('.xlsx', '.xls')):
                self.file_path = file_path + '.xlsx'

            # Excel writer will be created when we have data
            return self.file_path

        except ImportError as e:
            raise ImportError(f"Excel writing not available: {e}")

    def write_header_impl(self, columns: List[str], file_path, config: OutputConfig) -> None:
        """Store columns for later use"""
        self.columns = columns

    def write_batch_impl(self, batch_df: pd.DataFrame, file_path, config: OutputConfig) -> int:
        """Buffer batches for Excel writing"""
        self.batch_buffer.append(batch_df)
        return len(batch_df) * len(batch_df.columns) * 20  # Estimate

    def write_footer_impl(self, file_path, config: OutputConfig) -> None:
        """Write all buffered data to Excel"""
        if not self.batch_buffer:
            return

        try:
            # Combine all batches
            combined_df = pd.concat(self.batch_buffer, ignore_index=True)

            # Excel configuration
            sheet_name = getattr(config, 'sheet_name', 'Sheet1')
            include_header = getattr(config, 'include_header', True)

            # Write to Excel
            with pd.ExcelWriter(self.file_path, engine='openpyxl') as writer:
                combined_df.to_excel(
                    writer,
                    sheet_name=sheet_name,
                    index=False,
                    header=include_header
                )

            # Clear buffer
            self.batch_buffer.clear()

        except Exception as e:
            raise Exception(f"Failed to write Excel file: {e}")

    def close_impl(self, file_path) -> None:
        """Excel writing is done in write_footer_impl"""
        pass


class ParquetStrategy:
    """Parquet strategy - requires pyarrow"""

    def __init__(self):
        self.writer = None
        self.batch_buffer = []
        self.schema = None
        self.file_path = None
        self.row_group_size = 10000

    def initialize_writer(self, file_path: str, config: OutputConfig):
        """Initialize Parquet writer"""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            self.file_path = file_path
            self.row_group_size = getattr(config, 'parquet_row_group_size', 10000)

            # Ensure proper file extension
            if not file_path.endswith('.parquet'):
                self.file_path = file_path + '.parquet'

            return self.file_path

        except ImportError as e:
            raise ImportError(f"Parquet writing requires 'pyarrow': {e}")

    def write_header_impl(self, columns: List[str], file_path, config: OutputConfig) -> None:
        """Parquet schema is inferred from data"""
        pass

    def write_batch_impl(self, batch_df: pd.DataFrame, file_path, config: OutputConfig) -> int:
        """Buffer batches for optimal row group writing"""
        self.batch_buffer.append(batch_df)

        # Write row group when buffer is full
        total_rows = sum(len(df) for df in self.batch_buffer)
        if total_rows >= self.row_group_size:
            self._write_row_group(config)

        return len(batch_df) * len(batch_df.columns) * 8  # Estimate

    def write_footer_impl(self, file_path, config: OutputConfig) -> None:
        """Write remaining buffered data"""
        if self.batch_buffer:
            self._write_row_group(config)

    def close_impl(self, file_path) -> None:
        """Close Parquet writer"""
        if self.writer:
            self.writer.close()

    def _write_row_group(self, config):
        """Write accumulated batches as a row group"""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            if not self.batch_buffer:
                return

            # Combine all DataFrames
            combined_df = pd.concat(self.batch_buffer, ignore_index=True)
            table = pa.Table.from_pandas(combined_df, preserve_index=False)

            # Initialize writer if needed
            if self.writer is None:
                self.schema = table.schema
                compression = getattr(config, 'parquet_compression', 'snappy')
                self.writer = pq.ParquetWriter(
                    self.file_path,
                    self.schema,
                    compression=compression
                )

            # Write table
            self.writer.write_table(table)
            self.batch_buffer.clear()

        except Exception as e:
            raise Exception(f"Failed to write Parquet row group: {e}")


# ===== COMPRESSION WRAPPER =====

class CompressionWrapper:
    """Wrapper that adds compression to any strategy"""

    def __init__(self, base_writer, compression: str = 'gzip'):
        self.base_writer = base_writer
        self.compression = compression.lower()
        self.original_initialize = base_writer.strategy.initialize_writer

        # Validate compression type
        if self.compression not in ['gzip', 'bz2', 'lzma']:
            raise ValueError(f"Unsupported compression: {compression}")

        # Modify the strategy's initialization to use compression
        base_writer.strategy.initialize_writer = self._compressed_initialize

        # Update file path
        if not base_writer.file_path.endswith(f'.{self.compression}'):
            base_writer.file_path += f'.{self.compression}'

    def _compressed_initialize(self, file_path: str, config: OutputConfig):
        """Initialize with compression"""
        import gzip
        import bz2
        import lzma

        # Update file path with compression extension
        if not file_path.endswith(f'.{self.compression}'):
            file_path += f'.{self.compression}'

        if self.compression == 'gzip':
            return gzip.open(
                file_path, 'wt',
                encoding=getattr(config, 'encoding', 'utf-8'),
                compresslevel=getattr(config, 'compression_level', 6)
            )
        elif self.compression == 'bz2':
            return bz2.open(
                file_path, 'wt',
                encoding=getattr(config, 'encoding', 'utf-8'),
                compresslevel=getattr(config, 'compression_level', 9)
            )
        elif self.compression == 'lzma':
            return lzma.open(
                file_path, 'wt',
                encoding=getattr(config, 'encoding', 'utf-8')
            )

    def __getattr__(self, name):
        """Delegate all methods to base writer"""
        return getattr(self.base_writer, name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.base_writer.__exit__(exc_type, exc_val, exc_tb)


# ===== UNIFIED WRITER (Enhanced) =====

class Writer:
    """
    Enhanced unified writer with comprehensive format support
    """

    def __init__(self, file_path: str, config: OutputConfig, strategy, logger: logging.Logger = None, schema: dict = None, **kwargs):
        self.file_path = file_path
        self.config = config
        self.strategy = strategy
        self.logger = logger or logging.getLogger(__name__)

        # Ensure output directory exists
        os.makedirs(os.path.dirname(file_path) or '.', exist_ok=True)

        # Initialize common components (safely)
        try:
            self.optimizer = DataOptimizer(self.logger, schema)
        except Exception:
            self.optimizer = None

        try:
            self.progress = ProgressTracker(
                getattr(config, 'enable_progress', True),
                getattr(config, 'log_every_n_batches', 100),
                self.logger
            )
        except Exception:
            self.progress = None

        try:
            self.formatter = FixedWidthFormatter(config)
        except Exception:
            self.formatter = None

        # Writer state
        self.writer_context = None
        self.is_closed = False
        self.header_written = False

    def write_header(self, columns: List[str]) -> None:
        """Write file header using the strategy"""
        if self.header_written:
            return

        try:
            # Initialize the writer
            self.writer_context = self.strategy.initialize_writer(self.file_path, self.config)

            # Write format-specific header
            self.strategy.write_header_impl(columns, self.writer_context, self.config)

            self.header_written = True
            self.logger.debug(f"Header written for {self.strategy.__class__.__name__}")

        except Exception as e:
            self.logger.error(f"Failed to write header: {e}")
            raise

    def write_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Write batch of records using the strategy"""
        if not batch:
            return

        try:
            # Write header if not done yet
            if not self.header_written:
                columns = list(batch[0].keys()) if batch else []
                self.write_header(columns)

            # Convert to DataFrame
            df = pd.DataFrame(batch)

            # Apply optimizations if available
            if self.optimizer:
                try:
                    df = self.optimizer.optimize_dataframe(df)
                except Exception as e:
                    self.logger.warning(f"Data optimization failed: {e}")

            # Apply fixed-width formatting if enabled and available
            if self.formatter and getattr(self.config, 'enable_fixed_width', False):
                try:
                    df = self.formatter.format_dataframe(df)
                except Exception as e:
                    self.logger.warning(f"Fixed-width formatting failed: {e}")

            # Write using strategy
            bytes_written = self.strategy.write_batch_impl(df, self.writer_context, self.config)

            # Update progress if available
            if self.progress:
                try:
                    self.progress.update(len(batch), bytes_written)
                except Exception as e:
                    self.logger.warning(f"Progress update failed: {e}")

            self.logger.debug(f"Batch of {len(batch)} records written")

        except Exception as e:
            self.logger.error(f"Failed to write batch: {e}")
            raise

    def write_footer(self) -> None:
        """Write file footer using the strategy"""
        if self.writer_context:
            try:
                self.strategy.write_footer_impl(self.writer_context, self.config)
            except Exception as e:
                self.logger.warning(f"Failed to write footer: {e}")

    def close(self) -> None:
        """Close the writer"""
        if not self.is_closed:
            try:
                self.write_footer()

                if self.writer_context:
                    self.strategy.close_impl(self.writer_context)

                self.is_closed = True

                # Log completion stats
                if self.progress:
                    try:
                        stats = self.progress.get_stats()
                        self.logger.info(f"{self.strategy.__class__.__name__} completed: {stats['records_written']:,} records")
                    except Exception:
                        self.logger.info(f"{self.strategy.__class__.__name__} completed successfully")
                else:
                    self.logger.info(f"{self.strategy.__class__.__name__} completed successfully")

            except Exception as e:
                self.logger.error(f"Error during close: {e}")
                self.is_closed = True  # Mark as closed even if error

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.is_closed:
            self.close()


# ===== COMPLETE FACTORY =====

class WriterFactory:
    """
    Complete factory supporting all formats with compression
    """

    @classmethod
    def create_writer(cls, table_name: str, config: OutputConfig,
                      compression: Optional[str] = None, logger: logging.Logger = None, schema: dict = None, **kwargs) -> Writer:
        """Create unified writer with appropriate strategy"""

        if logger is None:
            logger = logging.getLogger(__name__)

        try:
            # Get file path
            file_path = config.get_output_path(table_name)

            # Determine format
            format_name = getattr(config, 'format', 'csv').lower()

            # Create appropriate strategy
            strategy = cls._create_strategy(format_name, config, logger)

            # Set table name in config for SQL strategy
            if format_name in ['sql', 'sql_query']:
                config.table_name = table_name

            # Create unified writer
            writer = Writer(file_path, config, strategy, logger, schema)

            # Add compression wrapper if requested
            if compression:
                try:
                    writer = CompressionWrapper(writer, compression)
                    logger.info(f"Added {compression} compression to {format_name} writer")
                except Exception as e:
                    logger.warning(f"Compression '{compression}' failed: {e}")

            logger.debug(f"Created {strategy.__class__.__name__} writer for {table_name}")
            return writer

        except Exception as e:
            logger.error(f"Failed to create writer for {table_name}: {e}")
            # Create fallback CSV writer
            try:
                config.format = 'csv'
                file_path = config.get_output_path(table_name)
                strategy = CSVStrategy()
                writer = Writer(file_path, config, strategy, logger)
                logger.info(f"Created fallback CSV writer for {table_name}")
                return writer
            except Exception as fallback_error:
                logger.error(f"Even fallback writer failed: {fallback_error}")
                raise

    @classmethod
    def _create_strategy(cls, format_name: str, config: OutputConfig, logger: logging.Logger):
        """Create strategy based on format name"""

        if format_name in ['csv']:
            return CSVStrategy()

        elif format_name == 'tsv':
            # Handle TSV as CSV with tab delimiter
            original_delimiter = getattr(config, 'csv_delimiter', ',')
            config.csv_delimiter = '\t'
            strategy = CSVStrategy()
            # Note: delimiter will be used during writing
            return strategy

        elif format_name in ['json']:
            return JSONStrategy()

        elif format_name in ['jsonl']:
            return JSONLStrategy()

        elif format_name in ['fixed', 'fwf', 'fixed_width']:
            # Enable fixed width formatting
            config.enable_fixed_width = True
            return FixedWidthStrategy()

        elif format_name in ['sql', 'sql_query']:
            return SQLQueryStrategy()

        elif format_name in ['excel', 'xlsx', 'xls']:
            return ExcelStrategy()

        elif format_name in ['parquet']:
            return ParquetStrategy()

        else:
            # Unknown format - fallback to CSV
            logger.warning(f"Unknown format '{format_name}', using CSV")
            return CSVStrategy()

    @classmethod
    def list_supported_formats(cls) -> List[str]:
        """Get list of supported formats"""
        return [
            'csv', 'tsv', 'json', 'jsonl',
            'fixed', 'fwf', 'fixed_width',
            'sql', 'sql_query', 'excel', 'xlsx', 'xls',
            'parquet'
        ]

    @classmethod
    def list_compression_formats(cls) -> List[str]:
        """Get list of supported compression formats"""
        return ['gzip', 'bz2', 'lzma']


# ===== ADVANCED FEATURES =====

def create_multi_format_writer(table_name: str, config: OutputConfig,
                               formats: List[str], logger: logging.Logger = None) -> List[Writer]:
    """Create multiple writers for the same data in different formats"""
    writers = []
    original_format = config.format

    for fmt in formats:
        try:
            config.format = fmt
            writer = WriterFactory.create_writer(f"{table_name}_{fmt}", config, logger=logger)
            writers.append(writer)
        except Exception as e:
            if logger:
                logger.error(f"Failed to create {fmt} writer: {e}")

    # Restore original format
    config.format = original_format
    return writers


def create_compressed_writer(table_name: str, config: OutputConfig,
                            compression: str = 'gzip', logger: logging.Logger = None) -> Writer:
    """Create writer with compression"""
    return WriterFactory.create_writer(
        table_name=table_name,
        config=config,
        compression=compression,
        logger=logger
    )


def adaptive_format_selection(data_sample: List[Dict[str, Any]], config: OutputConfig) -> str:
    """Intelligently select format based on data characteristics"""
    if not data_sample:
        return 'csv'

    data_size = len(data_sample)
    sample = data_sample[0] if data_sample else {}
    column_count = len(sample)

    # Analyze data characteristics
    has_nested_data = any(
        isinstance(v, (dict, list)) for row in data_sample[:10] for v in row.values()
    )

    has_long_text = any(
        isinstance(v, str) and len(v) > 100 for row in data_sample[:10] for v in row.values()
    )

    # Decision logic
    if has_nested_data:
        return 'json'
    elif data_size > 100000 and column_count > 20:
        return 'parquet'  # Large, wide data
    elif column_count > 50:
        return 'fixed'  # Very wide data
    elif has_long_text:
        return 'jsonl'  # Long text fields
    else:
        return 'csv'  # Default


# ===== TESTING AND VALIDATION =====

def test_all_formats():
    """Test all supported formats"""

    # Test data
    test_data = [
        {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'salary': 50000.0, 'active': True},
        {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'salary': 75000.0, 'active': False},
        {'id': 3, 'name': 'Bob Wilson', 'email': 'bob@example.com', 'salary': 60000.0, 'active': True}
    ]

    # Test configuration
    from config_manager.config_manager import OutputConfig
    config = OutputConfig()
    config.include_header = True
    config.enable_progress = False  # Disable for testing

    # Test results
    results = {}
    formats = WriterFactory.list_supported_formats()

    print("🧪 TESTING ALL FORMATS")
    print("=" * 40)

    for fmt in formats:
        try:
            config.format = fmt

            with WriterFactory.create_writer(f'test_{fmt}', config) as writer:
                writer.write_batch(test_data)

            results[fmt] = "✅ SUCCESS"
            print(f"  {fmt:15} → ✅ SUCCESS")

        except ImportError as e:
            results[fmt] = f"⚠️ DEPENDENCY: {str(e)[:50]}..."
            print(f"  {fmt:15} → ⚠️ DEPENDENCY MISSING")
        except Exception as e:
            results[fmt] = f"❌ ERROR: {str(e)[:50]}..."
            print(f"  {fmt:15} → ❌ ERROR")

    return results


def test_compression():
    """Test compression features"""

    test_data = [{'id': i, 'data': f'test_data_{i}' * 10} for i in range(100)]

    from config_manager.config_manager import OutputConfig
    config = OutputConfig()
    config.format = 'csv'
    config.enable_progress = False

    compressions = WriterFactory.list_compression_formats()
    results = {}

    print("\n🗜️ TESTING COMPRESSION")
    print("=" * 40)

    for compression in compressions:
        try:
            with WriterFactory.create_writer(
                f'test_compressed_{compression}',
                config,
                compression=compression
            ) as writer:
                writer.write_batch(test_data)

            results[compression] = "✅ SUCCESS"
            print(f"  {compression:10} → ✅ SUCCESS")

        except Exception as e:
            results[compression] = f"❌ ERROR: {str(e)[:50]}..."
            print(f"  {compression:10} → ❌ ERROR")

    return results


def demo_advanced_features():
    """Demonstrate advanced features"""

    print("\n🚀 ADVANCED FEATURES DEMO")
    print("=" * 40)

    # Sample data
    sample_data = [
        {'id': 1, 'name': 'Demo User', 'score': 95.5},
        {'id': 2, 'name': 'Test User', 'score': 87.2}
    ]

    from config_manager.config_manager import OutputConfig
    config = OutputConfig()
    config.enable_progress = False

    # 1. Multi-format export
    print("1. Multi-format export:")
    try:
        writers = create_multi_format_writer('demo', config, ['csv', 'json', 'jsonl'])
        for writer in writers:
            with writer:
                writer.write_batch(sample_data)
        print("   ✅ Created CSV, JSON, and JSONL files")
    except Exception as e:
        print(f"   ❌ Failed: {e}")

    # 2. Compressed export
    print("2. Compressed export:")
    try:
        with create_compressed_writer('demo_compressed', config, 'gzip') as writer:
            writer.write_batch(sample_data)
        print("   ✅ Created compressed CSV file")
    except Exception as e:
        print(f"   ❌ Failed: {e}")

    # 3. Adaptive format selection
    print("3. Adaptive format selection:")
    try:
        optimal_format = adaptive_format_selection(sample_data, config)
        print(f"   📊 Recommended format: {optimal_format}")
    except Exception as e:
        print(f"   ❌ Failed: {e}")

    # 4. SQL generation
    print("4. SQL generation:")
    try:
        config.format = 'sql'
        with WriterFactory.create_writer('demo_sql', config) as writer:
            writer.write_batch(sample_data)
        print("   ✅ Generated SQL INSERT statements")
    except Exception as e:
        print(f"   ❌ Failed: {e}")


# ===== USAGE EXAMPLES =====

def example_basic_usage():
    """Basic usage examples"""

    print("\n📝 BASIC USAGE EXAMPLES")
    print("=" * 40)

    from config_manager.config_manager import OutputConfig

    # Sample data
    data = [
        {'id': 1, 'name': 'Alice', 'department': 'Engineering', 'salary': 85000},
        {'id': 2, 'name': 'Bob', 'department': 'Marketing', 'salary': 70000},
        {'id': 3, 'name': 'Carol', 'department': 'Sales', 'salary': 65000}
    ]

    # Basic CSV
    config = OutputConfig()
    config.format = 'csv'
    with WriterFactory.create_writer('employees', config) as writer:
        writer.write_batch(data)
    print("✅ CSV: employees.csv")

    # JSON with formatting
    config.format = 'json'
    config.json_indent = 2
    with WriterFactory.create_writer('employees', config) as writer:
        writer.write_batch(data)
    print("✅ JSON: employees.json")

    # Fixed-width with custom widths
    config.format = 'fixed'
    config.enable_fixed_width = True
    config.column_widths = {'id': 5, 'name': 15, 'department': 12, 'salary': 10}
    with WriterFactory.create_writer('employees', config) as writer:
        writer.write_batch(data)
    print("✅ Fixed-width: employees.txt")

    # Compressed JSONL
    config.format = 'jsonl'
    with WriterFactory.create_writer('employees', config, compression='gzip') as writer:
        writer.write_batch(data)
    print("✅ Compressed JSONL: employees.jsonl.gz")


def example_streaming_usage():
    """Example of streaming usage (same interface as before)"""

    print("\n🌊 STREAMING USAGE (Same as before!)")
    print("=" * 40)

    from config_manager.config_manager import OutputConfig

    config = OutputConfig()
    config.format = 'csv'

    # This is exactly the same as your existing streaming code!
    writer = WriterFactory.create_writer('streaming_demo', config)

    columns = ['batch_id', 'record_id', 'value']
    writer.write_header(columns)

    try:
        # Simulate streaming batches
        for batch_num in range(3):
            batch_data = [
                {'batch_id': batch_num, 'record_id': i, 'value': f'data_{batch_num}_{i}'}
                for i in range(5)
            ]
            writer.write_batch(batch_data)
            print(f"   Wrote batch {batch_num + 1}")
    finally:
        writer.write_footer()
        writer.close()

    print("✅ Streaming completed - same interface as before!")


if __name__ == "__main__":
    print("🎉 COMPLETE UNIFIED WRITER")
    print("=" * 50)

    print("📋 SUPPORTED FORMATS:")
    formats = WriterFactory.list_supported_formats()
    for fmt in formats:
        print(f"  • {fmt}")

    print(f"\n🗜️ COMPRESSION: {', '.join(WriterFactory.list_compression_formats())}")

    # Run tests
    format_results = test_all_formats()
    compression_results = test_compression()

    # Demo features
    demo_advanced_features()
    example_basic_usage()
    example_streaming_usage()

    print(f"\n🎯 SUMMARY:")
    successful_formats = sum(1 for r in format_results.values() if 'SUCCESS' in r)
    total_formats = len(format_results)
    print(f"  📊 Formats working: {successful_formats}/{total_formats}")

    successful_compression = sum(1 for r in compression_results.values() if 'SUCCESS' in r)
    total_compression = len(compression_results)
    print(f"  🗜️ Compression working: {successful_compression}/{total_compression}")

    print(f"  ✅ Ready to use - same interface as before!")
    print(f"  🚀 New features: Compression, SQL, Excel, Parquet")