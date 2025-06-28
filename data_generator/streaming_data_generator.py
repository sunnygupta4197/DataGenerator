import os
import logging
import threading
import time
from typing import Dict, List, Any, Iterator, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from dataclasses import dataclass
import gc
from contextlib import contextmanager
import psutil


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
                 enable_streaming: bool = True, performance_profiler=None, logger=None):
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
        self.performance_profiler = performance_profiler if performance_profiler else PerformanceProfiler(logger=logger)

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
                           foreign_key_data: Dict[str, List] = None) -> Iterator[List[Dict[str, Any]]]:
        """
        Generate data in streaming fashion using the sophisticated DataGenerator
        """
        with self.performance_profiler.profile("streaming_generation_total"):

            # PROFILE SETUP
            with self.performance_profiler.profile("streaming_setup"):
                if foreign_key_data is None:
                    foreign_key_data = {}

                batch_size = self.streaming_batch_size
                total_batches = (total_records + batch_size - 1) // batch_size

                table_name = table_metadata.get('table_name', 'unknown')

            start_time = time.time()
            for batch_idx in range(total_batches):
                with self.performance_profiler.profile("streaming_batch_generation"):

                    start_index = batch_idx * batch_size
                    current_batch_size = min(batch_size, total_records - start_index)

                    self.logger.debug(f"Generating streaming batch {batch_idx + 1}/{total_batches}")

                    # Use the sophisticated DataGenerator batch generation
                    with self.performance_profiler.profile("batch_data_generation"):
                        batch_data = self.data_generator.generate_batch_optimized(
                            table_metadata=table_metadata,
                            batch_size=current_batch_size,
                            foreign_key_data=foreign_key_data
                        )
                    with self.performance_profiler.profile("fk_storage"):
                        # Store the batch in the DataGenerator for FK relationships
                        self.data_generator.store_generated_batch(table_name, batch_data)

                # Update statistics
                self.stats['total_records_generated'] += len(batch_data)
                self.stats['batches_processed'] += 1

                # Yield batch for further processing
                yield batch_data
                with self.performance_profiler.profile("memory_management"):
                    # Memory management
                    if self.memory_monitor.get_memory_usage() > self.memory_monitor.max_memory_mb * 0.8:
                        self._cleanup_memory()

                # Update timing statistics
                self.stats['generation_time'] = time.time() - start_time

    def set_streaming_flag(self, flag: bool):
        """Set flag to indicate streaming was used (to avoid double writing)"""
        self.streaming_used = flag

    # ===================== PARALLEL GENERATION =====================

    def generate_parallel(self, table_metadata: Dict[str, Any],
                          total_records: int,
                          foreign_key_data: Dict[str, List] = None,
                          use_processes: bool = False) -> List[Dict[str, Any]]:
        """
        Generate data using parallel processing with the sophisticated DataGenerator
        """
        with self.performance_profiler.profile("parallel_generation_total"):

            # PROFILE SETUP
            with self.performance_profiler.profile("parallel_setup"):
                if foreign_key_data is None:
                    foreign_key_data = {}

                batch_size = self.default_batch_size // self.max_workers
                total_batches = (total_records + batch_size - 1) // batch_size

            with self.performance_profiler.profile("fk_preparation"):

                # For parallel generation, we need to be careful about FK relationships
                # Generate FK pools upfront to share across workers
                self._prepare_fk_pools_for_parallel_generation(table_metadata, foreign_key_data)

            with self.performance_profiler.profile("task_creation"):
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

            with self.performance_profiler.profile("parallel_execution"):
                # Execute tasks in parallel
                if use_processes and len(tasks) > 2:
                    # For process-based parallelism, we need to be more careful
                    # as the DataGenerator instance can't be shared directly
                    results = self._execute_process_parallel(tasks)
                else:
                    # Thread-based parallelism can share the DataGenerator instance
                    results = self._execute_thread_parallel(tasks)

            with self.performance_profiler.profile("result_combination"):
                # Combine results
                all_data = []
                for result in sorted(results, key=lambda x: x.task_id):
                    if result.error:
                        self.logger.error(f"Task {result.task_id} failed: {result.error}")
                    else:
                        all_data.extend(result.data)
                        self.stats['parallel_tasks_completed'] += 1

            with self.performance_profiler.profile("fk_storage"):
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
                                    batch_size: int = None) -> Iterator[List[Dict[str, Any]]]:
        """
        Combine streaming and parallel processing using the sophisticated DataGenerator
        """
        if foreign_key_data is None:
            foreign_key_data = {}

        if batch_size is None:
            batch_size = self.streaming_batch_size * self.max_workers
        table_name = table_metadata.get('table_name', 'unknown')

        start_time = time.time()
        total_batches = (total_records + batch_size - 1) // batch_size
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

            # Store in DataGenerator for FK relationships
            self.data_generator.store_generated_batch(table_name, batch_data)

            # Update statistics
            self.stats['batches_processed'] += 1

            # Yield batch
            yield batch_data

            # Clean up batch data to free memory
            del batch_data

            # Memory management
            if self.memory_monitor.get_memory_usage() > self.memory_monitor.max_memory_mb * 0.8:
                self._cleanup_memory()

            self.stats['generation_time'] = time.time() - start_time

    # ===================== ADAPTIVE GENERATION =====================

    def generate_adaptive(self, table_metadata: Dict[str, Any],
                          total_records: int,
                          foreign_key_data: Dict[str, List] = None) -> Iterator[List[Dict[str, Any]]]:
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

            yield data

        elif estimated_memory_mb <= available_memory_mb * 0.8:
            # Medium dataset - use streaming with parallel batches
            self.logger.info("Using streaming + parallel generation strategy")
            yield from self.generate_streaming_parallel(
                table_metadata, total_records, foreign_key_data
            )

        else:
            # Large dataset - use pure streaming
            self.logger.info("Using streaming generation strategy")
            yield from self.generate_streaming(
                table_metadata, total_records, foreign_key_data
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
