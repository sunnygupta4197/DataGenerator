import os
import logging
import threading
import time
from typing import Dict, List, Any, Iterator, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from dataclasses import dataclass
import pandas as pd
import gc
from contextlib import contextmanager
import psutil
import json

# Import streaming writers from separate module
from writers.streaming_writers import (
    StreamingWriter,
    WriterFactory
)
from config_manager.config_manager import OutputConfig


@dataclass
class GenerationTask:
    """Task for parallel data generation"""
    table_metadata: Dict[str, Any]
    batch_size: int
    start_index: int
    foreign_key_data: Dict[str, List]
    task_id: str


@dataclass
class GenerationResult:
    """Result from data generation task"""
    task_id: str
    data: List[Dict[str, Any]]
    generation_time: float
    error: Optional[str] = None


class MemoryMonitor:
    """Monitor memory usage and trigger cleanup when needed"""

    def __init__(self, max_memory_mb: int = 1000, check_interval: float = 1.0):
        self.max_memory_mb = max_memory_mb
        self.check_interval = check_interval
        self._monitoring = False
        self._monitor_thread = None
        self._callbacks = []
        self.current_memory_mb = 0

    def add_cleanup_callback(self, callback: Callable):
        """Add callback to call when memory threshold is reached"""
        self._callbacks.append(callback)

    def start_monitoring(self):
        """Start memory monitoring in background thread"""
        if not self._monitoring:
            self._monitoring = True
            self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self._monitor_thread.start()

    def stop_monitoring(self):
        """Stop memory monitoring"""
        self._monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=2.0)

    def _monitor_loop(self):
        """Main monitoring loop"""
        while self._monitoring:
            try:
                process = psutil.Process()
                self.current_memory_mb = process.memory_info().rss / 1024 / 1024

                if self.current_memory_mb > self.max_memory_mb:
                    logging.warning(f"Memory usage high: {self.current_memory_mb:.1f}MB")
                    for callback in self._callbacks:
                        try:
                            callback()
                        except Exception as e:
                            logging.error(f"Error in memory cleanup callback: {e}")

                time.sleep(self.check_interval)
            except Exception as e:
                logging.error(f"Error in memory monitoring: {e}")
                time.sleep(self.check_interval)

    def get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        return self.current_memory_mb


class ParallelDataGenerator:
    """
    Enhanced high-performance data generator that properly integrates with the existing
    sophisticated ValueGenerator, ConstraintManager, and validation systems
    """

    def __init__(self, data_generator_instance,
                 max_workers: int = None, max_memory_mb: int = 1000,
                 enable_streaming: bool = True, logger=None):
        """
        Initialize with the main DataGenerator instance to access all sophisticated components

        Args:
            data_generator_instance: Instance of the main DataGenerator class
            max_workers: Maximum number of parallel workers
            max_memory_mb: Maximum memory usage before cleanup
            enable_streaming: Enable streaming mode for large datasets
            logger: Logger instance
        """
        self.streaming_used = None
        self.data_generator = data_generator_instance
        self.output_config = data_generator_instance.config.output
        self.logger = logger or logging.getLogger(__name__)
        self.enable_streaming = enable_streaming

        # Parallel processing setup
        self.max_workers = max_workers or min(4, os.cpu_count())
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)
        self.process_pool = None  # Created when needed

        # Memory management
        self.memory_monitor = MemoryMonitor(max_memory_mb)
        self.memory_monitor.add_cleanup_callback(self._cleanup_memory)

        # Performance settings
        self.default_batch_size = 10000
        self.streaming_batch_size = 1000
        self.max_queue_size = 100

        # Statistics
        self.stats = {
            'total_records_generated': 0,
            'batches_processed': 0,
            'parallel_tasks_completed': 0,
            'memory_cleanups': 0,
            'generation_time': 0.0
        }

        # Start memory monitoring
        self.memory_monitor.start_monitoring()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        self.memory_monitor.stop_monitoring()
        if self.thread_pool:
            self.thread_pool.shutdown(wait=True)
        if self.process_pool:
            self.process_pool.shutdown(wait=True)
        gc.collect()

    def _cleanup_memory(self):
        """Memory cleanup callback"""
        self.stats['memory_cleanups'] += 1
        gc.collect()
        # Also trigger cleanup in the data generator
        if hasattr(self.data_generator, 'reset_constraint_tracking'):
            # Clear old constraint tracking data but keep essential FK pools
            self.data_generator.constraint_manager.cleanup_memory()
        self.logger.info(f"Memory cleanup performed. Current usage: {self.memory_monitor.get_memory_usage():.1f}MB")

    # ===================== STREAMING GENERATION =====================

    def generate_streaming(self, table_metadata: Dict[str, Any],
                           total_records: int,
                           foreign_key_data: Dict[str, List] = None,
                           enable_masking: bool = False,
                           security_manager=None,
                           sensitivity_map: Dict[str, str] = None) -> Iterator[List[Dict[str, Any]]]:
        """
        Generate data in streaming fashion using the sophisticated DataGenerator
        """
        if foreign_key_data is None:
            foreign_key_data = {}

        batch_size = self.streaming_batch_size
        total_batches = (total_records + batch_size - 1) // batch_size

        table_name = table_metadata.get('table_name', 'unknown')

        # Initialize streaming writer if output path provided
        writer = self._create_streaming_writer(table_name)
        columns = [col['name'] for col in table_metadata.get('columns', [])]
        writer.write_header(columns)

        start_time = time.time()

        try:
            for batch_idx in range(total_batches):
                start_index = batch_idx * batch_size
                current_batch_size = min(batch_size, total_records - start_index)

                self.logger.debug(f"Generating streaming batch {batch_idx + 1}/{total_batches}")

                # Use the sophisticated DataGenerator batch generation
                batch_data = self.data_generator.generate_batch_optimized(
                    table_metadata=table_metadata,
                    batch_size=current_batch_size,
                    foreign_key_data=foreign_key_data
                )

                # 🔧 APPLY MASKING HERE - This was missing!
                if enable_masking and security_manager and sensitivity_map:
                    try:
                        batch_data = security_manager.mask_sensitive_data(batch_data, sensitivity_map)
                        self.logger.debug(f"Applied masking to batch {batch_idx + 1}")
                    except Exception as e:
                        self.logger.error(f"Error applying masking to batch {batch_idx + 1}: {e}")

                # Store the batch in the DataGenerator for FK relationships
                self.data_generator.store_generated_batch(table_name, batch_data)

                # Update statistics
                self.stats['total_records_generated'] += len(batch_data)
                self.stats['batches_processed'] += 1

                # Write to file if writer provided
                if writer:
                    writer.write_batch(batch_data)

                # Yield batch for further processing
                yield batch_data

                # Memory management
                if self.memory_monitor.get_memory_usage() > self.memory_monitor.max_memory_mb * 0.8:
                    self._cleanup_memory()

        finally:
            # Finalize writer
            if writer:
                writer.write_footer()
                writer.close()

            # Update timing statistics
            self.stats['generation_time'] = time.time() - start_time

    def set_streaming_flag(self, flag: bool):
        """Set flag to indicate streaming was used (to avoid double writing)"""
        self.streaming_used = flag

    def _create_streaming_writer(self, table_name) -> StreamingWriter:
        """Create appropriate streaming writer using WriterFactory"""
        try:
            return WriterFactory.create_writer(table_name, self.output_config, logger=self.logger)
        except ValueError as e:
            self.logger.error(f"Failed to create writer: {e}")
            self.logger.warning("Falling back to CSV format")

            # Temporarily change config format for fallback
            original_format = self.output_config.format
            self.output_config.format = "csv"

            try:
                return WriterFactory.create_writer(table_name, self.output_config, logger=self.logger)
            finally:
                # Restore original format
                self.output_config.format = original_format

    # ===================== PARALLEL GENERATION =====================

    def generate_parallel(self, table_metadata: Dict[str, Any],
                          total_records: int,
                          foreign_key_data: Dict[str, List] = None,
                          use_processes: bool = False) -> List[Dict[str, Any]]:
        """
        Generate data using parallel processing with the sophisticated DataGenerator
        """
        if foreign_key_data is None:
            foreign_key_data = {}

        batch_size = self.default_batch_size // self.max_workers
        total_batches = (total_records + batch_size - 1) // batch_size

        # For parallel generation, we need to be careful about FK relationships
        # Generate FK pools upfront to share across workers
        self._prepare_fk_pools_for_parallel_generation(table_metadata, foreign_key_data)

        # Create generation tasks
        tasks = []
        for batch_idx in range(total_batches):
            start_index = batch_idx * batch_size
            current_batch_size = min(batch_size, total_records - start_index)

            task = GenerationTask(
                table_metadata=table_metadata,
                batch_size=current_batch_size,
                start_index=start_index,
                foreign_key_data=foreign_key_data,
                task_id=f"batch_{batch_idx}"
            )
            tasks.append(task)

        # Execute tasks in parallel
        if use_processes and len(tasks) > 2:
            # For process-based parallelism, we need to be more careful
            # as the DataGenerator instance can't be shared directly
            results = self._execute_process_parallel(tasks)
        else:
            # Thread-based parallelism can share the DataGenerator instance
            results = self._execute_thread_parallel(tasks)

        # Combine results
        all_data = []
        for result in sorted(results, key=lambda x: x.task_id):
            if result.error:
                self.logger.error(f"Task {result.task_id} failed: {result.error}")
            else:
                all_data.extend(result.data)
                self.stats['parallel_tasks_completed'] += 1

        # Store all generated data in the DataGenerator for FK relationships
        table_name = table_metadata.get('table_name', 'unknown')
        self.data_generator.store_generated_batch(table_name, all_data)

        self.stats['total_records_generated'] += len(all_data)
        return all_data

    def _prepare_fk_pools_for_parallel_generation(self, table_metadata: Dict[str, Any],
                                                  foreign_key_data: Dict[str, List]):
        """Prepare FK pools before parallel generation to ensure consistency"""
        foreign_keys = table_metadata.get("foreign_keys", [])

        for fk in foreign_keys:
            parent_table = fk["parent_table"]
            parent_column = fk["parent_column"]
            fk_key = f"{parent_table}.{parent_column}"

            # If we have explicit FK data, update the constraint manager
            if fk_key in foreign_key_data:
                self.data_generator.constraint_manager.update_fk_pool(
                    parent_table, parent_column, foreign_key_data[fk_key]
                )

    def _execute_thread_parallel(self, tasks: List[GenerationTask]) -> List[GenerationResult]:
        """Execute tasks using thread pool with shared DataGenerator"""
        futures = []

        for task in tasks:
            future = self.thread_pool.submit(self._execute_generation_task_with_datagenerator, task)
            futures.append(future)

        results = []
        for future in as_completed(futures):
            try:
                result = future.result(timeout=300)  # 5 minute timeout
                results.append(result)
            except Exception as e:
                self.logger.error(f"Thread task failed: {e}")
                results.append(GenerationResult(
                    task_id="unknown", data=[], generation_time=0, error=str(e)
                ))

        return results

    def _execute_process_parallel(self, tasks: List[GenerationTask]) -> List[GenerationResult]:
        """Execute tasks using process pool - more complex due to serialization"""
        # For process-based parallelism, we need to serialize the generation configuration
        # rather than the DataGenerator instance itself

        if not self.process_pool:
            self.process_pool = ProcessPoolExecutor(max_workers=self.max_workers)

        # Prepare serializable generation configuration
        generation_config = self._create_serializable_config()

        futures = []
        for task in tasks:
            future = self.process_pool.submit(
                execute_task_with_config,
                task,
                generation_config
            )
            futures.append(future)

        results = []
        for future in as_completed(futures):
            try:
                result = future.result(timeout=600)  # 10 minute timeout for processes
                results.append(result)
            except Exception as e:
                self.logger.error(f"Process task failed: {e}")
                results.append(GenerationResult(
                    task_id="unknown", data=[], generation_time=0, error=str(e)
                ))

        return results

    def _create_serializable_config(self) -> Dict[str, Any]:
        """Create a serializable configuration for process-based parallel generation"""
        return {
            'locale': self.data_generator.faker.locale if hasattr(self.data_generator, 'faker') else 'en_US',
            'config': self.data_generator.config if hasattr(self.data_generator, 'config') else {},
            'constraint_stats': self.data_generator.constraint_manager.get_constraint_statistics()
        }

    def _execute_generation_task_with_datagenerator(self, task: GenerationTask) -> GenerationResult:
        """Execute generation task using the sophisticated DataGenerator"""
        start_time = time.time()

        try:
            # Use the sophisticated batch generation from DataGenerator
            batch_data = self.data_generator.generate_batch_optimized(
                table_metadata=task.table_metadata,
                batch_size=task.batch_size,
                foreign_key_data=task.foreign_key_data
            )

            generation_time = time.time() - start_time

            return GenerationResult(
                task_id=task.task_id,
                data=batch_data,
                generation_time=generation_time
            )

        except Exception as e:
            generation_time = time.time() - start_time
            return GenerationResult(
                task_id=task.task_id,
                data=[],
                generation_time=generation_time,
                error=str(e)
            )

    # ===================== HYBRID STREAMING + PARALLEL =====================

    def generate_streaming_parallel(self, table_metadata: Dict[str, Any],
                                    total_records: int,
                                    foreign_key_data: Dict[str, List] = None,
                                    batch_size: int = None,
                                    enable_masking: bool = False,
                                    security_manager = None,
                                    sensitivity_map: Dict[str, str] = None) -> Iterator[List[Dict[str, Any]]]:
        """
        Combine streaming and parallel processing using the sophisticated DataGenerator
        """
        if foreign_key_data is None:
            foreign_key_data = {}

        if batch_size is None:
            batch_size = self.streaming_batch_size * self.max_workers
        table_name = table_metadata.get('table_name', 'unknown')

        # Initialize streaming writer
        writer = self._create_streaming_writer(table_name)
        columns = [col['name'] for col in table_metadata.get('columns', [])]
        writer.write_header(columns)

        start_time = time.time()
        total_batches = (total_records + batch_size - 1) // batch_size

        try:
            for batch_idx in range(total_batches):
                start_index = batch_idx * batch_size
                current_batch_size = min(batch_size, total_records - start_index)

                self.logger.debug(f"Generating hybrid batch {batch_idx + 1}/{total_batches}")

                # Generate batch using the sophisticated DataGenerator
                batch_data = self.data_generator.generate_batch_optimized(
                    table_metadata=table_metadata,
                    batch_size=current_batch_size,
                    foreign_key_data=foreign_key_data
                )

                # 🔧 APPLY MASKING HERE - This was missing!
                if enable_masking and security_manager and sensitivity_map:
                    try:
                        batch_data = security_manager.mask_sensitive_data(batch_data, sensitivity_map)
                        self.logger.debug(f"Applied masking to hybrid batch {batch_idx + 1}")
                    except Exception as e:
                        self.logger.error(f"Error applying masking to hybrid batch {batch_idx + 1}: {e}")

                # Store in DataGenerator for FK relationships
                self.data_generator.store_generated_batch(table_name, batch_data)

                # Write to file if writer provided
                if writer:
                    writer.write_batch(batch_data)

                # Update statistics
                self.stats['batches_processed'] += 1

                # Yield batch
                yield batch_data

                # Clean up batch data to free memory
                del batch_data

                # Memory management
                if self.memory_monitor.get_memory_usage() > self.memory_monitor.max_memory_mb * 0.8:
                    self._cleanup_memory()

        finally:
            if writer:
                writer.write_footer()
                writer.close()

            self.stats['generation_time'] = time.time() - start_time

    # ===================== ADAPTIVE GENERATION =====================

    def generate_adaptive(self, table_metadata: Dict[str, Any],
                          total_records: int,
                          foreign_key_data: Dict[str, List] = None,
                          enable_masking: bool = False,
                          security_manager = None,
                          sensitivity_map: Dict[str, str] = None) -> Iterator[List[Dict[str, Any]]]:
        """
        Adaptive generation that chooses optimal strategy using the sophisticated DataGenerator
        """
        # Analyze requirements
        estimated_memory_mb = self._estimate_memory_requirements(table_metadata, total_records)
        available_memory_mb = self.memory_monitor.max_memory_mb

        self.logger.info(f"Estimated memory: {estimated_memory_mb:.1f}MB, Available: {available_memory_mb}MB")

        # Choose strategy
        if estimated_memory_mb <= available_memory_mb * 0.5:
            # Small dataset - use parallel generation with DataGenerator
            self.logger.info("Using parallel generation strategy")
            data = self.generate_parallel(table_metadata, total_records, foreign_key_data)

            # Apply masking to complete dataset
            if enable_masking and security_manager and sensitivity_map:
                try:
                    data = security_manager.mask_sensitive_data(data, sensitivity_map)
                    self.logger.info("Applied masking to parallel-generated data")
                except Exception as e:
                    self.logger.error(f"Error applying masking to parallel data: {e}")

            yield data

        elif estimated_memory_mb <= available_memory_mb * 0.8:
            # Medium dataset - use streaming with parallel batches
            self.logger.info("Using streaming + parallel generation strategy")
            yield from self.generate_streaming_parallel(
                table_metadata, total_records, foreign_key_data,
                enable_masking=enable_masking,
                security_manager=security_manager,
                sensitivity_map=sensitivity_map
            )

        else:
            # Large dataset - use pure streaming
            self.logger.info("Using streaming generation strategy")
            yield from self.generate_streaming(
                table_metadata, total_records, foreign_key_data,
                enable_masking=enable_masking,
                security_manager=security_manager,
                sensitivity_map=sensitivity_map
            )

    def _estimate_memory_requirements(self, table_metadata: Dict[str, Any], total_records: int) -> float:
        """Estimate memory requirements for generation"""
        columns = table_metadata.get('columns', [])

        # Estimate average bytes per record
        bytes_per_record = 0
        for column in columns:
            column_type = column.get('type', 'str')

            if column_type in ['int', 'integer']:
                bytes_per_record += 8  # 64-bit integer
            elif column_type in ['float', 'double']:
                bytes_per_record += 8  # 64-bit float
            elif column_type in ['bool', 'boolean']:
                bytes_per_record += 1  # Boolean
            elif column_type == 'date':
                bytes_per_record += 10  # Date string
            else:
                # String type - estimate average length
                length_constraint = column.get('length', 50)
                if isinstance(length_constraint, int):
                    bytes_per_record += length_constraint
                elif isinstance(length_constraint, dict):
                    max_length = length_constraint.get('max', 50)
                    bytes_per_record += max_length
                else:
                    bytes_per_record += 50  # Default string length

        # Add overhead for Python objects and data structures
        bytes_per_record *= 3  # Rough estimate for Python overhead

        total_memory_bytes = total_records * bytes_per_record
        return total_memory_bytes / (1024 * 1024)  # Convert to MB

    # ===================== STATISTICS AND MONITORING =====================

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics including DataGenerator stats"""
        # Get stats from the underlying DataGenerator
        dg_stats = {}
        if hasattr(self.data_generator, 'get_constraint_statistics'):
            dg_stats = self.data_generator.get_constraint_statistics()

        return {
            **self.stats,
            'memory_usage_mb': self.memory_monitor.get_memory_usage(),
            'max_memory_mb': self.memory_monitor.max_memory_mb,
            'records_per_second': (
                self.stats['total_records_generated'] / self.stats['generation_time']
                if self.stats['generation_time'] > 0 else 0
            ),
            'average_batch_time': (
                self.stats['generation_time'] / self.stats['batches_processed']
                if self.stats['batches_processed'] > 0 else 0
            ),
            'data_generator_stats': dg_stats
        }

    def reset_stats(self):
        """Reset performance statistics"""
        self.stats = {
            'total_records_generated': 0,
            'batches_processed': 0,
            'parallel_tasks_completed': 0,
            'memory_cleanups': 0,
            'generation_time': 0.0
        }


# ===================== PROCESS-BASED PARALLEL FUNCTIONS =====================

def execute_task_with_config(task: GenerationTask, generation_config: Dict[str, Any]) -> GenerationResult:
    """
    Execute generation task in a separate process using configuration
    This recreates a minimal DataGenerator for process-based parallelism
    """
    start_time = time.time()

    try:
        # Import here to avoid circular imports in process
        from data_generator import DataGenerator

        # Create a new DataGenerator instance in this process
        config = generation_config.get('config', {})
        locale = generation_config.get('locale', 'en_US')

        data_generator = DataGenerator(config, locale)

        # Generate the batch
        batch_data = data_generator.generate_batch_optimized(
            table_metadata=task.table_metadata,
            batch_size=task.batch_size,
            foreign_key_data=task.foreign_key_data
        )

        generation_time = time.time() - start_time

        return GenerationResult(
            task_id=task.task_id,
            data=batch_data,
            generation_time=generation_time
        )

    except Exception as e:
        generation_time = time.time() - start_time
        return GenerationResult(
            task_id=task.task_id,
            data=[],
            generation_time=generation_time,
            error=str(e)
        )


# ===================== DATA QUALITY ANALYZER =====================

class DataQualityAnalyzer:
    """
    Analyze data quality and provide insights
    """

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.analysis_cache = {}

    def analyze_distribution(self, data: List[Dict[str, Any]],
                             table_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze statistical distribution of generated data"""
        if not data:
            return {}

        df = pd.DataFrame(data)
        analysis = {
            'record_count': len(df),
            'column_analysis': {},
            'data_quality_score': 0.0,
            'issues': []
        }

        columns = table_metadata.get('columns', [])
        quality_scores = []

        for column in columns:
            column_name = column['name']

            if column_name not in df.columns:
                analysis['issues'].append(f"Missing column: {column_name}")
                continue

            col_analysis = self._analyze_column(df[column_name], column)
            analysis['column_analysis'][column_name] = col_analysis
            quality_scores.append(col_analysis['quality_score'])

        # Calculate overall quality score
        if quality_scores:
            analysis['data_quality_score'] = sum(quality_scores) / len(quality_scores)

        return analysis

    def _analyze_column(self, series: pd.Series, column_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze individual column quality"""
        analysis = {
            'null_count': series.isnull().sum(),
            'null_percentage': (series.isnull().sum() / len(series)) * 100,
            'unique_count': series.nunique(),
            'uniqueness_ratio': series.nunique() / len(series),
            'quality_score': 1.0,
            'issues': []
        }

        # Data type specific analysis
        column_type = column_metadata.get('type', 'str')

        if column_type in ['int', 'integer', 'float', 'double']:
            analysis.update(self._analyze_numeric_column(series))
        elif column_type in ['str', 'string', 'text']:
            analysis.update(self._analyze_string_column(series))
        elif column_type in ['date', 'datetime']:
            analysis.update(self._analyze_date_column(series))

        # Check constraints
        self._check_column_constraints(series, column_metadata, analysis)

        return analysis

    def _analyze_numeric_column(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze numeric column"""
        numeric_series = pd.to_numeric(series, errors='coerce')

        return {
            'mean': numeric_series.mean(),
            'median': numeric_series.median(),
            'std': numeric_series.std(),
            'min': numeric_series.min(),
            'max': numeric_series.max(),
            'outlier_count': self._count_outliers(numeric_series)
        }

    def _analyze_string_column(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze string column"""
        str_series = series.astype(str)
        lengths = str_series.str.len()

        return {
            'avg_length': lengths.mean(),
            'min_length': lengths.min(),
            'max_length': lengths.max(),
            'empty_count': (str_series == '').sum(),
            'common_patterns': self._find_common_patterns(str_series)
        }

    def _analyze_date_column(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze date column"""
        try:
            date_series = pd.to_datetime(series, errors='coerce')
            return {
                'earliest_date': date_series.min(),
                'latest_date': date_series.max(),
                'invalid_dates': date_series.isnull().sum()
            }
        except:
            return {'invalid_dates': len(series)}

    def _count_outliers(self, series: pd.Series) -> int:
        """Count outliers using IQR method"""
        if series.isnull().all():
            return 0

        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        outliers = series[(series < lower_bound) | (series > upper_bound)]
        return len(outliers)

    def _find_common_patterns(self, series: pd.Series) -> List[str]:
        """Find common patterns in string data"""
        # Simplified pattern detection
        patterns = []

        # Check for common formats
        if series.str.contains(r'^\d+').any():
            patterns.append('numeric_strings')

        if series.str.contains(r'^[A-Z]+-\d+').any():
            patterns.append('prefix_numeric')

        if series.str.contains(r'@').any():
            patterns.append('email_like')

        return patterns

    def _check_column_constraints(self, series: pd.Series,
                                  column_metadata: Dict[str, Any],
                                  analysis: Dict[str, Any]):
        """Check if column data meets defined constraints"""
        # Check nullable constraint
        nullable = column_metadata.get('nullable', True)
        if not nullable and analysis['null_count'] > 0:
            analysis['issues'].append('Null values in non-nullable column')
            analysis['quality_score'] -= 0.2

        # Check unique constraint
        constraints = column_metadata.get('constraints', [])
        if 'unique' in constraints and analysis['uniqueness_ratio'] < 1.0:
            analysis['issues'].append('Duplicate values in unique column')
            analysis['quality_score'] -= 0.3

        # Check length constraints
        length_constraint = column_metadata.get('length')
        if length_constraint:
            self._check_length_constraint(series, length_constraint, analysis)

    def _check_length_constraint(self, series: pd.Series,
                                 length_constraint: Any,
                                 analysis: Dict[str, Any]):
        """Check length constraint compliance"""
        str_series = series.astype(str)
        lengths = str_series.str.len()

        violations = 0

        if isinstance(length_constraint, int):
            violations = (lengths != length_constraint).sum()
        elif isinstance(length_constraint, dict):
            min_len = length_constraint.get('min', 0)
            max_len = length_constraint.get('max', float('inf'))
            violations = ((lengths < min_len) | (lengths > max_len)).sum()

        if violations > 0:
            violation_pct = (violations / len(series)) * 100
            analysis['issues'].append(f'{violations} length constraint violations ({violation_pct:.1f}%)')
            analysis['quality_score'] -= min(0.5, violation_pct / 100)

    def detect_anomalies(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Detect anomalies in generated data"""
        if not data:
            return {'anomalies': []}

        df = pd.DataFrame(data)
        anomalies = []

        # Detect duplicate records
        duplicates = df.duplicated()
        if duplicates.any():
            anomalies.append({
                'type': 'duplicate_records',
                'count': duplicates.sum(),
                'severity': 'medium'
            })

        # Detect suspicious patterns
        for column in df.columns:
            col_anomalies = self._detect_column_anomalies(df[column], column)
            anomalies.extend(col_anomalies)

        return {'anomalies': anomalies}

    def _detect_column_anomalies(self, series: pd.Series, column_name: str) -> List[Dict[str, Any]]:
        """Detect anomalies in individual column"""
        anomalies = []

        # Check for excessive nulls
        null_pct = (series.isnull().sum() / len(series)) * 100
        if null_pct > 50:
            anomalies.append({
                'type': 'excessive_nulls',
                'column': column_name,
                'percentage': null_pct,
                'severity': 'high'
            })

        # Check for low cardinality in large datasets
        if len(series) > 1000 and series.nunique() < 10:
            anomalies.append({
                'type': 'low_cardinality',
                'column': column_name,
                'unique_count': series.nunique(),
                'total_count': len(series),
                'severity': 'medium'
            })

        return anomalies

    def validate_business_rules(self, data: List[Dict[str, Any]],
                                rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate complex business rules"""
        if not data or not rules:
            return {'violations': []}

        df = pd.DataFrame(data)
        violations = []

        for rule in rules:
            rule_violations = self._validate_single_rule(df, rule)
            violations.extend(rule_violations)

        return {
            'violations': violations,
            'total_violations': len(violations),
            'compliance_rate': 1 - (len(violations) / len(data)) if data else 1.0
        }

    def _validate_single_rule(self, df: pd.DataFrame, rule: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate single business rule"""
        violations = []
        rule_type = rule.get('type')

        try:
            if rule_type == 'conditional':
                # Example: If age > 65, then plan_type must be 'senior'
                condition_column = rule.get('condition_column')
                condition_operator = rule.get('condition_operator', '>')
                condition_value = rule.get('condition_value')
                requirement_column = rule.get('requirement_column')
                requirement_value = rule.get('requirement_value')

                if all([condition_column,
                        requirement_column]) and condition_column in df.columns and requirement_column in df.columns:
                    if condition_operator == '>':
                        condition_mask = df[condition_column] > condition_value
                    elif condition_operator == '<':
                        condition_mask = df[condition_column] < condition_value
                    elif condition_operator == '==':
                        condition_mask = df[condition_column] == condition_value
                    else:
                        condition_mask = pd.Series([False] * len(df))

                    violating_rows = df[condition_mask & (df[requirement_column] != requirement_value)]

                    for idx, row in violating_rows.iterrows():
                        violations.append({
                            'rule_type': 'conditional',
                            'rule_description': f"If {condition_column} {condition_operator} {condition_value}, then {requirement_column} must be {requirement_value}",
                            'violation_row_index': idx,
                            'actual_values': {condition_column: row[condition_column],
                                              requirement_column: row[requirement_column]}
                        })

            elif rule_type == 'range_dependency':
                # Example: If income > 100000, then credit_score must be > 700
                income_column = rule.get('income_column', 'income')
                score_column = rule.get('score_column', 'credit_score')
                income_threshold = rule.get('income_threshold', 100000)
                score_threshold = rule.get('score_threshold', 700)

                if income_column in df.columns and score_column in df.columns:
                    violating_rows = df[(df[income_column] > income_threshold) & (df[score_column] <= score_threshold)]

                    for idx, row in violating_rows.iterrows():
                        violations.append({
                            'rule_type': 'range_dependency',
                            'rule_description': f"If {income_column} > {income_threshold}, then {score_column} must be > {score_threshold}",
                            'violation_row_index': idx,
                            'actual_values': {income_column: row[income_column], score_column: row[score_column]}
                        })

            elif rule_type == 'mutual_exclusivity':
                # Example: status cannot be both 'ACTIVE' and 'SUSPENDED'
                column1 = rule.get('column1')
                column2 = rule.get('column2')
                value1 = rule.get('value1')
                value2 = rule.get('value2')

                if column1 in df.columns and column2 in df.columns:
                    violating_rows = df[(df[column1] == value1) & (df[column2] == value2)]

                    for idx, row in violating_rows.iterrows():
                        violations.append({
                            'rule_type': 'mutual_exclusivity',
                            'rule_description': f"{column1} cannot be {value1} when {column2} is {value2}",
                            'violation_row_index': idx,
                            'actual_values': {column1: row[column1], column2: row[column2]}
                        })

        except Exception as e:
            self.logger.error(f"Error validating business rule {rule_type}: {e}")

        return violations


# ===================== SECURITY MANAGER =====================

class SecurityManager:
    """
    Comprehensive security management for sensitive data
    """

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.encryption_key = None
        self.masking_rules = {}
        self.enable_masking = False
        self.audit_trail = []

    def set_encryption_key(self, key: bytes):
        """Set encryption key for sensitive data"""
        self.encryption_key = key

    def add_masking_rule(self, column_pattern: str, masking_type: str):
        """Add data masking rule"""
        self.masking_rules[column_pattern] = masking_type

    def mask_sensitive_data(self, data: List[Dict[str, Any]],
                            sensitivity_map: Dict[str, str]) -> List[Dict[str, Any]]:
        """Apply data masking rules to sensitive data"""
        if not self.enable_masking:
            return data

        masked_data = []
        masked_fields_count = {}

        for row in data:
            masked_row = {}
            for column, value in row.items():
                sensitivity_level = sensitivity_map.get(column, 'PUBLIC')

                if sensitivity_level in ['PII', 'SENSITIVE']:
                    masked_row[column] = self._mask_value(value, column)
                    masked_fields_count[column] = masked_fields_count.get(column, 0) + 1
                else:
                    masked_row[column] = value

            masked_data.append(masked_row)

        # Log masking activity
        if masked_fields_count:
            self.logger.info(f"Data masking applied: {masked_fields_count}")

        return masked_data

    def _mask_value(self, value: Any, column: str) -> Any:
        """Apply masking to individual value"""
        if value is None:
            return value

        str_value = str(value)

        # Apply custom masking rules first
        for pattern, masking_type in self.masking_rules.items():
            if pattern.lower() in column.lower():
                return self._apply_custom_masking(str_value, masking_type)

        # Default masking rules
        if '@' in str_value:
            return self._mask_email(str_value)
        elif str_value.replace('-', '').replace(' ', '').replace('(', '').replace(')', '').isdigit() and len(
                str_value) >= 10:
            return self._mask_phone(str_value)
        elif 'name' in column.lower():
            return self._mask_name(str_value)
        elif 'ssn' in column.lower() or 'social' in column.lower():
            return self._mask_ssn(str_value)
        elif 'card' in column.lower() or 'credit' in column.lower():
            return self._mask_credit_card(str_value)
        else:
            return self._mask_generic(str_value)

    def _apply_custom_masking(self, value: str, masking_type: str) -> str:
        """Apply custom masking based on type"""
        if masking_type == 'full':
            return '*' * len(value)
        elif masking_type == 'partial':
            return self._mask_generic(value)
        elif masking_type == 'hash':
            import hashlib
            return hashlib.sha256(value.encode()).hexdigest()[:8]
        else:
            return self._mask_generic(value)

    def _mask_email(self, email: str) -> str:
        """Mask email address"""
        if '@' not in email:
            return email

        local, domain = email.split('@', 1)
        if len(local) > 2:
            masked_local = local[0] + '*' * (len(local) - 2) + local[-1]
        else:
            masked_local = '*' * len(local)

        return f"{masked_local}@{domain}"

    def _mask_phone(self, phone: str) -> str:
        """Mask phone number"""
        digits = ''.join(c for c in phone if c.isdigit())
        if len(digits) >= 10:
            masked = digits[:3] + '*' * (len(digits) - 6) + digits[-3:]
            return phone.replace(digits, masked)
        return phone

    def _mask_name(self, name: str) -> str:
        """Mask name"""
        if len(name) <= 1:
            return name
        return name[0] + '*' * (len(name) - 1)

    def _mask_ssn(self, ssn: str) -> str:
        """Mask SSN"""
        digits = ''.join(c for c in ssn if c.isdigit())
        if len(digits) == 9:
            return f"***-**-{digits[-4:]}"
        return '*' * len(ssn)

    def _mask_credit_card(self, card: str) -> str:
        """Mask credit card number"""
        digits = ''.join(c for c in card if c.isdigit())
        if len(digits) >= 13:
            return f"****-****-****-{digits[-4:]}"
        return '*' * len(card)

    def _mask_generic(self, value: str) -> str:
        """Generic masking"""
        if len(value) <= 2:
            return '*' * len(value)
        return value[0] + '*' * (len(value) - 2) + value[-1]

    def encrypt_sensitive_fields(self, data: List[Dict[str, Any]],
                                 sensitive_fields: List[str]) -> List[Dict[str, Any]]:
        """Encrypt sensitive data fields"""
        if not self.encryption_key:
            self.logger.warning("No encryption key set, skipping encryption")
            return data

        encrypted_data = []
        encryption_count = 0

        for row in data:
            encrypted_row = {}
            for column, value in row.items():
                if column in sensitive_fields:
                    encrypted_row[column] = self._encrypt_value(value)
                    encryption_count += 1
                else:
                    encrypted_row[column] = value

            encrypted_data.append(encrypted_row)

        self.logger.info(f"Encrypted {encryption_count} field values")
        return encrypted_data

    def _encrypt_value(self, value: Any) -> Optional[str]:
        """Encrypt individual value"""
        if value is None:
            return value

        try:
            # In production, use proper encryption libraries like cryptography
            # This is a simplified example using base64 (NOT secure!)
            import base64
            import hashlib

            str_value = str(value)

            # Create a simple "encryption" (this is NOT secure!)
            # In production, use proper AES encryption
            key_hash = hashlib.sha256(self.encryption_key).digest()[:16]

            # Simple XOR encryption (demonstration only)
            encrypted_bytes = bytes([ord(c) ^ key_hash[i % len(key_hash)] for i, c in enumerate(str_value)])
            encoded = base64.b64encode(encrypted_bytes).decode()

            return f"ENC:{encoded}"

        except Exception as e:
            self.logger.error(f"Encryption failed: {e}")
            return f"ENC_ERROR:{str(value)[:8]}***"

    def audit_data_generation(self, generation_params: Dict[str, Any],
                              records_count: int,
                              sensitive_columns: List[str]) -> Dict[str, Any]:
        """Create audit trail for data generation"""
        from datetime import datetime
        import uuid

        audit_record = {
            'audit_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'event_type': 'data_generation',
            'generation_params': generation_params,
            'records_generated': records_count,
            'sensitive_columns': sensitive_columns,
            'user': os.getenv('USER', 'unknown'),
            'hostname': os.getenv('HOSTNAME', 'unknown'),
            'pid': os.getpid(),
            'compliance_level': self._assess_compliance_level(generation_params),
            'security_measures_applied': {
                'masking_enabled': self.enable_masking,
                'encryption_enabled': bool(self.encryption_key),
                'custom_rules_count': len(self.masking_rules)
            }
        }

        # Store in audit trail
        self.audit_trail.append(audit_record)

        # Log audit record
        self.logger.info(f"Audit record created: {audit_record['audit_id']}")

        return audit_record

    def _assess_compliance_level(self, params: Dict[str, Any]) -> str:
        """Assess compliance level of generation parameters"""
        risk_indicators = []

        # Check for PII-related terms
        pii_terms = ['pii', 'personal', 'ssn', 'social', 'credit', 'email', 'phone', 'address']
        sensitive_terms = ['sensitive', 'confidential', 'private', 'restricted']

        for key, value in params.items():
            str_value = str(value).lower()
            if any(term in str_value for term in pii_terms):
                risk_indicators.append('PII_DETECTED')
            elif any(term in str_value for term in sensitive_terms):
                risk_indicators.append('SENSITIVE_DETECTED')

        # Assess overall risk
        if 'PII_DETECTED' in risk_indicators:
            return 'HIGH_RISK'
        elif 'SENSITIVE_DETECTED' in risk_indicators:
            return 'MEDIUM_RISK'
        else:
            return 'LOW_RISK'

    def export_audit_trail(self, output_path: str = None) -> str:
        """Export audit trail to file"""
        if not output_path:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"./audit_trail_{timestamp}.json"

        try:
            with open(output_path, 'w') as f:
                json.dump(self.audit_trail, f, indent=2, default=str)

            self.logger.info(f"Audit trail exported to: {output_path}")
            return output_path

        except Exception as e:
            self.logger.error(f"Failed to export audit trail: {e}")
            raise

    def validate_compliance(self, data: List[Dict[str, Any]],
                            compliance_rules: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data against compliance rules"""
        compliance_report = {
            'timestamp': time.time(),
            'total_records': len(data),
            'violations': [],
            'compliance_score': 1.0,
            'rules_checked': len(compliance_rules)
        }

        if not data:
            return compliance_report

        df = pd.DataFrame(data)

        for rule_name, rule_config in compliance_rules.items():
            try:
                violations = self._check_compliance_rule(df, rule_name, rule_config)
                compliance_report['violations'].extend(violations)
            except Exception as e:
                self.logger.error(f"Error checking compliance rule {rule_name}: {e}")

        # Calculate compliance score
        if compliance_report['violations']:
            violation_rate = len(compliance_report['violations']) / len(data)
            compliance_report['compliance_score'] = max(0.0, 1.0 - violation_rate)

        return compliance_report

    def _check_compliance_rule(self, df: pd.DataFrame, rule_name: str,
                               rule_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check individual compliance rule"""
        violations = []
        rule_type = rule_config.get('type')

        if rule_type == 'no_real_pii':
            # Check that no real PII patterns exist
            pii_patterns = rule_config.get('patterns', [])
            for pattern in pii_patterns:
                for column in df.columns:
                    if df[column].dtype == 'object':  # String columns
                        matches = df[column].str.contains(pattern, regex=True, na=False)
                        if matches.any():
                            for idx in df[matches].index:
                                violations.append({
                                    'rule': rule_name,
                                    'type': 'real_pii_detected',
                                    'column': column,
                                    'row_index': idx,
                                    'pattern': pattern
                                })

        elif rule_type == 'data_minimization':
            # Check that only necessary columns are present
            allowed_columns = set(rule_config.get('allowed_columns', []))
            actual_columns = set(df.columns)
            unauthorized_columns = actual_columns - allowed_columns

            if unauthorized_columns:
                violations.append({
                    'rule': rule_name,
                    'type': 'unauthorized_columns',
                    'columns': list(unauthorized_columns)
                })

        elif rule_type == 'anonymization_check':
            # Check that sensitive data is properly anonymized
            sensitive_columns = rule_config.get('sensitive_columns', [])
            for column in sensitive_columns:
                if column in df.columns:
                    # Check for patterns that suggest real data
                    real_patterns = rule_config.get('real_data_patterns', [])
                    for pattern in real_patterns:
                        matches = df[column].astype(str).str.contains(pattern, regex=True, na=False)
                        if matches.any():
                            violations.append({
                                'rule': rule_name,
                                'type': 'insufficient_anonymization',
                                'column': column,
                                'pattern': pattern,
                                'match_count': matches.sum()
                            })

        return violations


# ===================== PERFORMANCE PROFILER =====================

class PerformanceProfiler:
    """
    Advanced performance profiling for data generation
    """

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.profiles = {}
        self.active_profiles = {}

    @contextmanager
    def profile(self, operation_name: str):
        """Context manager for profiling operations"""
        start_time = time.time()
        start_memory = self._get_memory_usage()

        try:
            yield
        finally:
            end_time = time.time()
            end_memory = self._get_memory_usage()

            duration = end_time - start_time
            memory_delta = end_memory - start_memory

            self._record_profile(operation_name, duration, memory_delta)

    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except:
            return 0.0

    def _record_profile(self, operation_name: str, duration: float, memory_delta: float):
        """Record profiling data"""
        if operation_name not in self.profiles:
            self.profiles[operation_name] = {
                'call_count': 0,
                'total_duration': 0.0,
                'min_duration': float('inf'),
                'max_duration': 0.0,
                'total_memory_delta': 0.0,
                'min_memory_delta': float('inf'),
                'max_memory_delta': float('-inf')
            }

        profile = self.profiles[operation_name]
        profile['call_count'] += 1
        profile['total_duration'] += duration
        profile['min_duration'] = min(profile['min_duration'], duration)
        profile['max_duration'] = max(profile['max_duration'], duration)
        profile['total_memory_delta'] += memory_delta
        profile['min_memory_delta'] = min(profile['min_memory_delta'], memory_delta)
        profile['max_memory_delta'] = max(profile['max_memory_delta'], memory_delta)

    def get_profile_report(self) -> Dict[str, Any]:
        """Generate comprehensive profile report"""
        report = {
            'timestamp': time.time(),
            'operations': {}
        }

        for operation_name, profile in self.profiles.items():
            avg_duration = profile['total_duration'] / profile['call_count']
            avg_memory_delta = profile['total_memory_delta'] / profile['call_count']

            report['operations'][operation_name] = {
                'call_count': profile['call_count'],
                'total_duration_seconds': profile['total_duration'],
                'average_duration_seconds': avg_duration,
                'min_duration_seconds': profile['min_duration'],
                'max_duration_seconds': profile['max_duration'],
                'average_memory_delta_mb': avg_memory_delta,
                'min_memory_delta_mb': profile['min_memory_delta'],
                'max_memory_delta_mb': profile['max_memory_delta'],
                'calls_per_second': profile['call_count'] / profile['total_duration'] if profile[
                                                                                             'total_duration'] > 0 else 0
            }

        return report

    def reset_profiles(self):
        """Reset all profiling data"""
        self.profiles.clear()
        self.active_profiles.clear()
