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

        self.is_closed = False
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
        self.columns = None

    def write_header(self, columns: List[str]) -> None:
        if self.header_written:
            return

        self.columns = columns

        if self.include_header:
            # Use pandas for consistent header formatting
            pd.DataFrame(columns=columns).to_csv(
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
            'parquet': ParquetWriter
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