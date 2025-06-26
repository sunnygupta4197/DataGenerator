"""
Refactored Streaming Writers with Clean Architecture

This version separates concerns into focused, single-responsibility components:
- Data optimization (pandas handling)
- File I/O (format-specific writers)
- Configuration management
- Progress tracking
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Union, Protocol
from abc import ABC, abstractmethod
import time
from datetime import datetime
from dataclasses import dataclass
import pandas as pd
import numpy as np
from config_manager.config_manager import OutputConfig
from .writer import FixedWidthFormatter

# Optional imports
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

# ===== DATA OPTIMIZATION =====


class DataOptimizer:
    """Handles pandas data type optimization and cleaning"""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.schema_cache = {}
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


# ===== CORE WRITER INTERFACE =====

class StreamingWriter(ABC):
    """Clean base interface for streaming writers"""

    def __init__(self, file_path: str, config: OutputConfig, logger: logging.Logger = None):
        self.file_path = file_path
        self.config = config
        self.logger = logger or logging.getLogger(__name__)

        # Create output directory
        os.makedirs(os.path.dirname(file_path) or '.', exist_ok=True)

        # Initialize components
        self.optimizer = DataOptimizer(self.logger)
        self.progress = ProgressTracker(config.enable_progress, config.log_every_n_batches, self.logger)
        self.formatter = FixedWidthFormatter(config)

        self.is_closed = False
        self.columns = None
        self.header_written = False

    @abstractmethod
    def write_header(self, columns: List[str]) -> None:
        """Write file header"""
        pass

    @abstractmethod
    def write_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Write batch of records"""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close writer"""
        pass

    @abstractmethod
    def write_footer(self) -> None:
        """Write file footer"""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.is_closed:
            self.close()


# ===== FORMAT-SPECIFIC WRITERS =====

class CSVWriter(StreamingWriter):
    """Clean CSV writer implementation"""

    def __init__(self, file_path: str, config: OutputConfig, **kwargs):
        super().__init__(file_path, config)
        self.delimiter = config.csv_delimiter
        self.quote_char = config.csv_quotechar
        self.include_header = config.include_header

        self.file_handle = open(
            file_path, 'w',
            buffering=config.buffer_size,
            encoding=config.encoding,
            newline=''
        )

    def write_header(self, columns: List[str]) -> None:
        if self.header_written:
            return

        self.columns = columns

        if self.include_header:
            # Use pandas for consistent header formatting
            formatted_headers = self.formatter.format_header_row(self.columns)
            pd.DataFrame(columns=formatted_headers).to_csv(
                self.file_handle,
                sep=self.delimiter,
                quotechar=self.quote_char,
                index=False,
                header=True
            )

        self.header_written = True

    def write_batch(self, batch: List[Dict[str, Any]]) -> None:
        if not batch:
            return

        # Optimize with pandas
        df = pd.DataFrame(batch)
        df = self.optimizer.optimize_dataframe(df)

        # Apply fixed-width formatting if enabled
        df = self.formatter.format_dataframe(df)

        # Write using pandas
        df.to_csv(
            self.file_handle,
            sep=self.delimiter,
            quotechar=self.quote_char,
            index=False,
            header=False,
            mode='a'
        )

        # Estimate bytes written
        estimated_bytes = len(df) * len(df.columns) * 10
        self.progress.update(len(batch), estimated_bytes)

    def close(self) -> None:
        if not self.is_closed:
            self.file_handle.close()
            self.is_closed = True
            stats = self.progress.get_stats()
            self.logger.info(f"CSV completed: {stats['records_written']:,} records")

    def write_footer(self) -> None:
        """CSV files don't need a footer"""
        pass


class JSONLWriter(StreamingWriter):
    """Clean JSONL writer implementation"""

    def __init__(self, file_path: str, config: OutputConfig, **kwargs):
        super().__init__(file_path, config)
        self.indent = config.json_indent
        self.ensure_ascii = config.json_ensure_ascii

        self.file_handle = open(
            file_path, 'w',
            buffering=config.buffer_size,
            encoding=config.encoding
        )

    def write_header(self, columns: List[str]) -> None:
        self.columns = columns
        self.header_written = True

    def write_batch(self, batch: List[Dict[str, Any]]) -> None:
        if not batch:
            return

        # Optimize with pandas
        df = pd.DataFrame(batch)
        df = self.optimizer.optimize_dataframe(df)

        # Apply fixed-width formatting if enabled
        df = self.formatter.format_dataframe(df)

        # Convert back to records and serialize
        records = df.to_dict('records')
        lines = []

        for record in records:
            # Clean pandas/numpy types for JSON
            cleaned = self._clean_for_json(record)
            line = json.dumps(
                cleaned,
                ensure_ascii=self.ensure_ascii,
                indent=self.indent,
                default=self._json_serializer
            ) + '\n'
            lines.append(line)

        content = ''.join(lines)
        self.file_handle.write(content)

        self.progress.update(len(batch), len(content.encode(self.config.encoding)))

    def _clean_for_json(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert pandas/numpy types to JSON-serializable types"""
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

    def _json_serializer(self, obj):
        """Handle additional types for JSON serialization"""
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, 'item'):  # numpy scalar
            return obj.item()
        return str(obj)

    def close(self) -> None:
        if not self.is_closed:
            self.file_handle.close()
            self.is_closed = True
            stats = self.progress.get_stats()
            self.logger.info(f"JSONL completed: {stats['records_written']:,} records")

    def write_footer(self) -> None:
        """JSONL files don't need a footer"""
        pass


class ParquetWriter(StreamingWriter):
    """Clean Parquet writer implementation"""

    def __init__(self, file_path: str, config: OutputConfig, **kwargs):
        super().__init__(file_path, config)

        if not HAS_PYARROW:
            raise ImportError("pyarrow required for Parquet writing")

        self.compression = config.parquet_compression
        self.row_group_size = config.parquet_row_group_size

        self.writer = None
        self.batch_buffer = []
        self.schema = None

    def write_header(self, columns: List[str]) -> None:
        self.columns = columns

        # For Parquet, we can store fixed-width metadata
        if self.formatter.enable_fixed_width:
            self.formatter.analyze_data_for_widths(pd.DataFrame(columns=[col for col in columns]))

        self.header_written = True

    def write_batch(self, batch: List[Dict[str, Any]]) -> None:
        if not batch:
            return

        # Optimize with pandas
        df = pd.DataFrame(batch)
        df = self.optimizer.optimize_dataframe(df)

        self.batch_buffer.append(df)

        # Write row group when buffer is full
        total_rows = sum(len(df) for df in self.batch_buffer)
        if total_rows >= self.row_group_size:
            self._write_row_group()

        self.progress.update(len(batch))

    def _write_row_group(self):
        """Write accumulated batches as a row group"""
        if not self.batch_buffer:
            return

        # Combine all DataFrames
        combined_df = pd.concat(self.batch_buffer, ignore_index=True)

        # Convert to PyArrow table
        table = pa.Table.from_pandas(combined_df, preserve_index=False)

        # Initialize writer if needed
        if self.writer is None:
            self.schema = table.schema
            self.writer = pq.ParquetWriter(
                self.file_path,
                self.schema,
                compression=self.compression
            )

        # Write table
        self.writer.write_table(table)
        self.batch_buffer.clear()

    def close(self) -> None:
        if not self.is_closed:
            # Write remaining data
            if self.batch_buffer:
                self._write_row_group()

            if self.writer:
                self.writer.close()

            self.is_closed = True
            stats = self.progress.get_stats()
            self.logger.info(f"Parquet completed: {stats['records_written']:,} records")

    def write_footer(self) -> None:
        """Write remaining buffered data as final row group"""
        if self.batch_buffer:
            self._write_row_group()


# ===== COMPRESSION SUPPORT =====

class CompressionWriter(StreamingWriter):
    """Decorator that adds compression to any writer"""

    def __init__(self, base_writer: StreamingWriter, file_path: str, config: OutputConfig, compression: str = 'gzip'):
        super().__init__(file_path, config)
        self.base_writer = base_writer
        self.compression = compression.lower()

        # Validate compression type
        if self.compression not in ['gzip', 'bz2', 'lzma']:
            raise ValueError(f"Unsupported compression: {compression}")

        # Update file path with compression extension
        if not self.base_writer.file_path.endswith(f'.{self.compression}'):
            self.base_writer.file_path += f'.{self.compression}'

        # Replace file handle with compressed version
        self._setup_compression()

    def _setup_compression(self):
        """Replace the base writer's file handle with compressed version"""
        import gzip
        import bz2
        import lzma

        # Close original file handle if open
        if hasattr(self.base_writer, 'file_handle') and self.base_writer.file_handle:
            self.base_writer.file_handle.close()

        # Create compressed file handle
        if self.compression == 'gzip':
            self.base_writer.file_handle = gzip.open(
                self.base_writer.file_path, 'wt',
                encoding=self.base_writer.config.encoding,
                buffering=self.base_writer.config.buffer_size
            )
        elif self.compression == 'bz2':
            self.base_writer.file_handle = bz2.open(
                self.base_writer.file_path, 'wt',
                encoding=self.base_writer.config.encoding
            )
        elif self.compression == 'lzma':
            self.base_writer.file_handle = lzma.open(
                self.base_writer.file_path, 'wt',
                encoding=self.base_writer.config.encoding
            )

    def write_header(self, columns: List[str]) -> None:
        """Delegate to base writer"""
        self.base_writer.write_header(columns)

    def write_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Delegate to base writer"""
        self.base_writer.write_batch(batch)

    def write_footer(self) -> None:
        """Delegate to base writer"""
        self.base_writer.write_footer()

    def close(self) -> None:
        """Delegate to base writer"""
        self.base_writer.close()

    def __getattr__(self, name):
        """Delegate all other attributes to base writer"""
        return getattr(self.base_writer, name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class FixedWidthWriter(StreamingWriter):
    """Clean Fixed-Width writer implementation"""

    def __init__(self, file_path: str, config: OutputConfig, **kwargs):
        super().__init__(file_path, config)

        # Force enable fixed-width formatting for this writer
        self.formatter.enable_fixed_width = True

        self.include_header = getattr(config, 'include_header', True)

        self.file_handle = open(
            file_path, 'w',
            buffering=config.buffer_size,
            encoding=config.encoding
        )

    def write_header(self, columns: List[str]) -> None:
        if self.header_written:
            return

        self.columns = columns

        if self.include_header:
            formatted_headers = self.formatter.format_header(columns)
            header_line = ''.join(formatted_headers) + '\n'
            self.file_handle.write(header_line)

        self.header_written = True

    def write_batch(self, batch: List[Dict[str, Any]]) -> None:
        if not batch:
            return

        # Optimize with pandas
        df = pd.DataFrame(batch)
        df = self.optimizer.optimize_dataframe(df)

        # Format each row to fixed width
        lines = []
        for _, row in df.iterrows():
            line = ''.join(row.values) + '\n'
            lines.append(line)

        content = ''.join(lines)
        self.file_handle.write(content)

        # Estimate bytes written
        estimated_bytes = len(content.encode(self.config.encoding))
        self.progress.update(len(batch), estimated_bytes)

    def close(self) -> None:
        if not self.is_closed:
            self.file_handle.close()
            self.is_closed = True
            stats = self.progress.get_stats()
            self.logger.info(f"Fixed-width completed: {stats['records_written']:,} records")

    def write_footer(self) -> None:
        """Fixed-width files don't need a footer"""
        pass


class ExcelWriter(StreamingWriter):
    """Enhanced Excel writer with fixed-width support and streaming capabilities"""

    def __init__(self, file_path: str, config: OutputConfig, **kwargs):
        super().__init__(file_path, config)

        if not HAS_EXCEL:
            raise ImportError("Excel libraries required. Install with: pip install openpyxl xlsxwriter")

        # Excel-specific configuration
        self.sheet_name = getattr(config, 'sheet_name', 'Sheet1')
        self.include_header = getattr(config, 'include_header', True)
        self.freeze_panes = getattr(config, 'freeze_panes', None)  # e.g., (1, 0)
        self.auto_adjust_width = getattr(config, 'auto_adjust_width', True)
        self.engine = getattr(config, 'excel_engine', 'openpyxl')
        self.date_format = getattr(config, 'date_format', 'YYYY-MM-DD')
        self.number_format = getattr(config, 'number_format', '#,##0.00')
        self.max_rows_per_sheet = getattr(config, 'max_rows_per_sheet', 1048576)

        # Header formatting options
        self.header_format = getattr(config, 'header_format', {
            'bold': True,
            'bg_color': '#D7E4BC',
            'font_color': '#000000'
        })

        # Determine file extension and engine
        file_ext = os.path.splitext(file_path)[1].lower()
        if file_ext == '.xlsx':
            self.engine = 'xlsxwriter' if self.engine == 'xlsxwriter' else 'openpyxl'
        elif file_ext == '.xls':
            self.engine = 'xlwt'
        else:
            # Default to xlsx
            if not file_path.endswith('.xlsx'):
                self.file_path = file_path + '.xlsx'
            self.engine = 'xlsxwriter' if self.engine == 'xlsxwriter' else 'openpyxl'

        # Initialize Excel writer components
        self.excel_writer = None
        self.current_sheet = None
        self.current_row = 0
        self.sheet_count = 1
        self.batch_buffer = []
        self.columns = None

    def write_header(self, columns: List[str]) -> None:
        if self.header_written:
            return

        self.columns = columns

        # Analyze columns for fixed-width formatting if enabled
        if self.formatter.enable_fixed_width:
            # Create a sample DataFrame for analysis
            sample_df = pd.DataFrame(columns=columns)
            self.formatter.analyze_data_for_widths(sample_df)

        # Initialize Excel writer
        self._initialize_excel_writer()

        # Write header if enabled
        if self.include_header:
            self._write_excel_header(columns)

        self.header_written = True

    def _initialize_excel_writer(self):
        """Initialize the Excel writer and first worksheet"""
        try:
            self.excel_writer = pd.ExcelWriter(
                self.file_path,
                engine=self.engine,
                date_format=self.date_format,
                options={'remove_timezone': True} if self.engine == 'xlsxwriter' else {}
            )

            # Create first worksheet
            self._create_new_sheet()

        except Exception as e:
            self.logger.error(f"Failed to initialize Excel writer: {e}")
            raise

    def _create_new_sheet(self):
        """Create a new worksheet"""
        if self.sheet_count == 1:
            sheet_name = self.sheet_name
        else:
            sheet_name = f"{self.sheet_name}_Part{self.sheet_count}"

        # Create empty DataFrame for sheet initialization
        empty_df = pd.DataFrame()
        empty_df.to_excel(self.excel_writer, sheet_name=sheet_name, index=False, header=False)

        self.current_sheet = sheet_name
        self.current_row = 0

    def _write_excel_header(self, columns: List[str]):
        """Write formatted header to Excel"""
        if self.formatter.enable_fixed_width:
            formatted_headers = self.formatter.format_header(columns)
        else:
            formatted_headers = columns

        # Create header DataFrame
        header_df = pd.DataFrame([dict(zip(columns, formatted_headers))])

        # Write header to current sheet
        header_df.to_excel(
            self.excel_writer,
            sheet_name=self.current_sheet,
            startrow=self.current_row,
            index=False,
            header=False
        )

        self.current_row += 1

        # Apply formatting if using xlsxwriter
        if self.engine == 'xlsxwriter':
            self._apply_header_formatting(formatted_headers)

    def _apply_header_formatting(self, headers: List[str]):
        """Apply formatting to header row using xlsxwriter"""
        try:
            workbook = self.excel_writer.book
            worksheet = self.excel_writer.sheets[self.current_sheet]

            # Create header format
            header_format = workbook.add_format(self.header_format)

            # Apply formatting to header cells
            for col_num, header in enumerate(headers):
                worksheet.write(0, col_num, header, header_format)

            # Set column widths
            for col_num, col_name in enumerate(self.columns):
                if self.formatter.enable_fixed_width:
                    width = self.formatter.get_column_width(col_name)
                    worksheet.set_column(col_num, col_num, width)
                elif self.auto_adjust_width:
                    # Auto-adjust based on header length with some padding
                    header_width = len(headers[col_num]) + 2
                    worksheet.set_column(col_num, col_num, max(header_width, 10))

            # Freeze panes if specified
            if self.freeze_panes:
                worksheet.freeze_panes(*self.freeze_panes)

        except Exception as e:
            self.logger.warning(f"Could not apply header formatting: {e}")

    def write_batch(self, batch: List[Dict[str, Any]]) -> None:
        if not batch:
            return

        # Optimize with pandas
        df = pd.DataFrame(batch)
        df = self.optimizer.optimize_dataframe(df)

        # Apply fixed-width formatting if enabled
        if self.formatter.enable_fixed_width:
            df = self.formatter.format_dataframe(df)

        # Check if we need a new sheet (Excel row limit)
        if self.current_row + len(df) > self.max_rows_per_sheet:
            self._finalize_current_sheet()
            self.sheet_count += 1
            self._create_new_sheet()

            # Write header to new sheet if enabled
            if self.include_header:
                self._write_excel_header(self.columns)

        # Write data to current sheet
        df.to_excel(
            self.excel_writer,
            sheet_name=self.current_sheet,
            startrow=self.current_row,
            index=False,
            header=False
        )

        # Apply data formatting if using xlsxwriter
        if self.engine == 'xlsxwriter':
            self._apply_data_formatting(df, self.current_row)

        self.current_row += len(df)

        # Update progress
        estimated_bytes = len(df) * len(df.columns) * 10
        self.progress.update(len(batch), estimated_bytes)

    def _apply_data_formatting(self, df: pd.DataFrame, start_row: int):
        """Apply formatting to data rows using xlsxwriter"""
        try:
            workbook = self.excel_writer.book
            worksheet = self.excel_writer.sheets[self.current_sheet]

            # Create number format
            number_format = workbook.add_format({'num_format': self.number_format})

            # Apply number formatting to numeric columns
            for col_num, col_name in enumerate(df.columns):
                if pd.api.types.is_numeric_dtype(df[col_name]):
                    # Apply number format to the range of cells
                    end_row = start_row + len(df) - 1
                    col_letter = chr(65 + col_num)
                    cell_range = f'{col_letter}{start_row + 1}:{col_letter}{end_row + 1}'
                    worksheet.set_column(col_num, col_num, None, number_format)

        except Exception as e:
            self.logger.warning(f"Could not apply data formatting: {e}")

    def _finalize_current_sheet(self):
        """Finalize current sheet before creating a new one"""
        if self.engine == 'xlsxwriter' and self.excel_writer:
            try:
                # Any final sheet-specific formatting can go here
                pass
            except Exception as e:
                self.logger.warning(f"Error finalizing sheet: {e}")

    def close(self) -> None:
        if not self.is_closed:
            try:
                if self.excel_writer:
                    self.excel_writer.close()

                self.is_closed = True
                stats = self.progress.get_stats()

                sheet_info = f" across {self.sheet_count} sheet(s)" if self.sheet_count > 1 else ""
                self.logger.info(f"Excel completed: {stats['records_written']:,} records{sheet_info}")

            except Exception as e:
                self.logger.error(f"Error closing Excel writer: {e}")
                raise

    def write_footer(self) -> None:
        """Finalize any remaining Excel formatting"""
        if self.excel_writer and not self.is_closed:
            self._finalize_current_sheet()


# ===== FACTORY =====

class WriterFactory:
    """Clean factory for creating writers with optional compression"""

    @staticmethod
    def create_writer(table_name: str, config: OutputConfig, compression: Optional[str] = None, **kwargs) -> StreamingWriter:
        """Create appropriate writer based on config"""
        file_path = config.get_output_path(table_name)

        # Use compression from parameter or config
        compression = compression or config.compression

        writers = {
            'csv': CSVWriter,
            'tsv': lambda path, cfg, **kw: CSVWriter(path, cfg, delimiter='\t', **kw),
            'jsonl': JSONLWriter,
            'json': JSONLWriter,
            'parquet': ParquetWriter,
            'fixed': FixedWidthWriter,
            'fwf': FixedWidthWriter,
            'fixed_width': FixedWidthWriter,
            'excel': ExcelWriter,
            'xls': ExcelWriter,
            'xlsx': ExcelWriter
        }

        writer_class = writers.get(config.format)
        if not writer_class:
            raise ValueError(f"Unsupported format: {config.format}")

        # Create base writer
        base_writer = writer_class(file_path, config, **kwargs)

        # Wrap with compression if requested
        if compression:
            return CompressionWriter(base_writer, compression)

        return base_writer