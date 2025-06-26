import os
import logging
import argparse
from datetime import datetime
import json
from typing import Dict, List, Any, Tuple, Optional
from traceback import print_exc
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from dataclasses import dataclass
import gc

import pandas as pd

from constraint_manager.optimized_constraint_manager import OptimizedConstraintManager
from validators.unified_validation_system import UnifiedValidator
from data_generator.streaming_data_generator import ParallelDataGenerator, DataQualityAnalyzer, SecurityManager, \
    PerformanceProfiler
from config_manager.config_manager import ConfigurationManager, GenerationConfig
from data_generator.data_generator import DataGenerator

from writers.unified_writer import UnifiedWriterFactory, UnifiedWriter


@dataclass
class CachedComponents:
    """Cache for expensive-to-create components"""
    data_generator: Optional[DataGenerator] = None
    security_manager: Optional[SecurityManager] = None
    quality_analyzer: Optional[DataQualityAnalyzer] = None
    last_used: Optional[datetime] = None


class ComponentCache:
    """Thread-safe component cache with memory management"""

    def __init__(self, max_size: int = 10):
        self.max_size = max_size
        self._cache = {}
        self._lock = threading.Lock()

    def get_or_create(self, key: str, factory_func, *args, **kwargs):
        with self._lock:
            if key in self._cache:
                self._cache[key].last_used = datetime.now()
                return self._cache[key]

            if len(self._cache) >= self.max_size:
                self._evict_oldest()

            component = factory_func(*args, **kwargs)
            self._cache[key] = CachedComponents(
                data_generator=component,
                last_used=datetime.now()
            )
            return self._cache[key]

    def _evict_oldest(self):
        if not self._cache:
            return
        oldest_key = min(self._cache.keys(),
                         key=lambda k: self._cache[k].last_used)
        del self._cache[oldest_key]
        gc.collect()


class OptimizedDataGenerationEngine:
    """
    Highly optimized data generation engine with performance improvements
    """

    # Class-level cache for shared components
    _component_cache = ComponentCache()
    _instance_count = 0

    def __init__(self, config: GenerationConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger

        # Instance tracking for optimization
        OptimizedDataGenerationEngine._instance_count += 1
        self.instance_id = OptimizedDataGenerationEngine._instance_count

        # Initialize caches first
        self._fk_constraints_cache = {}
        self._sensitivity_cache = {}
        self._memory_estimate_cache = {}

        # Initialize core components with optimized versions and caching
        self.constraint_manager = self._get_cached_constraint_manager()
        self.validator = self._get_cached_validator()

        # Create optimized DataGenerator with lazy initialization
        self.data_generator = self._create_optimized_data_generator()

        # Initialize the enhanced parallel data generator with optimizations
        self.parallel_generator = self._create_optimized_parallel_generator()

        self.streaming_used = False

        # Use cached analyzers
        self.data_quality_analyzer = self._get_cached_quality_analyzer()
        self.security_manager = self._get_cached_security_manager()
        self.performance_profiler = PerformanceProfiler(logger=logger)

        # Configure security with batch operations
        if config.security.enable_data_masking:
            self._configure_security_batch()

        # Pre-allocate statistics structure
        self.generation_stats = self._create_stats_structure()

        # Performance optimizations
        self._setup_performance_optimizations()

    def _get_cached_constraint_manager(self):
        """Get cached constraint manager"""
        cache_key = f"constraint_manager_{self.config.locale}_{self.config.performance.max_workers}"

        if not hasattr(self.__class__, '_constraint_manager_cache'):
            self.__class__._constraint_manager_cache = {}

        if cache_key not in self.__class__._constraint_manager_cache:
            self.__class__._constraint_manager_cache[cache_key] = OptimizedConstraintManager(
                logger=self.logger,
                max_memory_mb=self.config.performance.max_memory_mb,
                enable_parallel=self.config.performance.enable_parallel
            )

        return self.__class__._constraint_manager_cache[cache_key]

    def _get_cached_validator(self):
        """Get cached validator"""
        cache_key = "unified_validator"

        if not hasattr(self.__class__, '_validator_cache'):
            self.__class__._validator_cache = {}

        if cache_key not in self.__class__._validator_cache:
            self.__class__._validator_cache[cache_key] = UnifiedValidator(logger=self.logger)

        return self.__class__._validator_cache[cache_key]

    def _get_cached_quality_analyzer(self):
        """Get cached quality analyzer"""
        cache_key = "data_quality_analyzer"

        if not hasattr(self.__class__, '_quality_analyzer_cache'):
            self.__class__._quality_analyzer_cache = {}

        if cache_key not in self.__class__._quality_analyzer_cache:
            self.__class__._quality_analyzer_cache[cache_key] = DataQualityAnalyzer(logger=self.logger)

        return self.__class__._quality_analyzer_cache[cache_key]

    def _get_cached_security_manager(self):
        """Get cached security manager"""
        cache_key = "security_manager"

        if not hasattr(self.__class__, '_security_manager_cache'):
            self.__class__._security_manager_cache = {}

        if cache_key not in self.__class__._security_manager_cache:
            self.__class__._security_manager_cache[cache_key] = SecurityManager(logger=self.logger)

        return self.__class__._security_manager_cache[cache_key]

    def _create_optimized_data_generator(self):
        """Create data generator with optimizations"""
        # Use weak references to prevent circular references
        generator = DataGenerator(
            self.config,
            self.config.locale,
            ai_config=self.config.ai,
            logger=self.logger
        )

        # Pre-warm generator caches if small dataset
        if self.config.rows < 10000:
            self._prewarm_generator_caches(generator)

        return generator

    def _create_optimized_parallel_generator(self):
        """Create optimized parallel generator"""
        return ParallelDataGenerator(
            data_generator_instance=self.data_generator,
            max_workers=min(self.config.performance.max_workers, os.cpu_count() * 2),  # Optimal worker count
            max_memory_mb=self.config.performance.max_memory_mb,
            enable_streaming=self.config.performance.enable_streaming,
            logger=self.logger
        )

    def _prewarm_generator_caches(self, generator):
        """Pre-warm generator caches for better performance"""
        try:
            # Generate small sample to warm up caches
            sample_metadata = {
                "table_name": "warmup",
                "columns": [
                    {"name": "id", "type": "int", "constraints": []},
                    {"name": "name", "type": "varchar", "constraints": []}
                ]
            }
            # Small warmup generation
            list(self.parallel_generator.generate_adaptive(
                sample_metadata, 10, {}, False, None, None
            ))
        except Exception:
            # Ignore warmup errors
            pass

    def _configure_security_batch(self):
        """Configure security with batch operations for better performance"""
        self.security_manager.enable_masking = True

        # Batch add masking rules for better performance
        masking_rules = [
            ('email', 'partial'),
            ('phone', 'partial'),
            ('ssn', 'partial'),
            ('credit', 'partial'),
            ('income', 'hash'),
            ('salary', 'hash')
        ]

        for field, rule_type in masking_rules:
            self.security_manager.add_masking_rule(field, rule_type)

        # Set encryption key if provided
        if hasattr(self.config.security, 'encryption_key') and self.config.security.encryption_key:
            self.security_manager.set_encryption_key(self.config.security.encryption_key.encode())

    def _create_stats_structure(self):
        """Pre-allocate statistics structure for better performance"""
        return {
            'tables_processed': 0,
            'total_records_generated': 0,
            'total_duration': 0.0,
            'memory_peak_mb': 0.0,
            'quality_scores': {},
            'security_operations': 0,
            'performance_profiles': {},
            'errors': []
        }

    def _setup_performance_optimizations(self):
        """Setup various performance optimizations"""
        # Disable pandas warnings for performance
        pd.options.mode.chained_assignment = None

        # Configure garbage collection for better memory management
        gc.set_threshold(700, 10, 10)  # More aggressive GC

        # Pre-allocate common data structures
        self._batch_cache = {}
        self._fk_cache = {}

    def generate_data_for_table(self, table_metadata: Dict[str, Any],
                                total_records: int,
                                foreign_key_data: Dict[str, List] = None,
                                output_dir: str = "./output") -> Tuple[List[Dict], Dict]:
        """
        Optimized data generation for a single table
        """
        table_name = table_metadata["table_name"]
        self.logger.info(f"🚀 Starting optimized generation for table '{table_name}' ({total_records:,} records)")

        self.streaming_used = False
        start_time = datetime.now()

        try:
            # Fast path for small datasets
            if total_records < 1000:
                return self._generate_small_dataset_fast_path(
                    table_metadata, total_records, foreign_key_data, output_dir
                )

            # Optimized generation strategy selection
            with self.performance_profiler.profile(f'table_{table_name}_generation'):
                generated_data = self._select_and_execute_optimized_strategy(
                    table_metadata, total_records, foreign_key_data, output_dir
                )

            # Optimized post-processing pipeline
            generated_data = self._optimized_post_processing_pipeline(
                generated_data, table_metadata, table_name
            )

            # Update statistics efficiently
            self._update_statistics_efficient(generated_data, start_time, table_name)

            # Extract foreign key data with caching
            generated_fk_data = self._extract_foreign_key_data_cached(generated_data, table_metadata)

            strategy_name = "streaming" if self.streaming_used else "parallel/adaptive"
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.info(
                f"✅ Completed {table_name}: {len(generated_data):,} records in {duration:.2f}s using {strategy_name} strategy")

            return generated_data, generated_fk_data

        except Exception as e:
            print_exc()
            self.logger.error(f"❌ Failed to generate data for {table_name}: {e}")
            self.generation_stats['errors'].append(f"{table_name}: {str(e)}")
            raise

    def _generate_small_dataset_fast_path(self, table_metadata: Dict[str, Any],
                                          total_records: int,
                                          foreign_key_data: Dict[str, List],
                                          output_dir: str) -> Tuple[List[Dict], Dict]:
        """Fast path for small datasets - bypass expensive operations"""
        table_name = table_metadata["table_name"]

        # Use simple generation for small datasets
        generated_data = list(self.parallel_generator.generate_adaptive(
            table_metadata=table_metadata,
            total_records=total_records,
            foreign_key_data=foreign_key_data or {},
            enable_masking=False,  # Skip masking for small datasets
            security_manager=None,
            sensitivity_map=None
        ))

        # Flatten if nested
        if generated_data and isinstance(generated_data[0], list):
            flat_data = []
            for batch in generated_data:
                flat_data.extend(batch)
            generated_data = flat_data

        # Skip expensive post-processing for small datasets
        return generated_data, self._extract_foreign_key_data_cached(generated_data, table_metadata)

    def _select_and_execute_optimized_strategy(self, table_metadata: Dict[str, Any],
                                               total_records: int,
                                               foreign_key_data: Dict[str, List],
                                               output_dir: str) -> List[Dict]:
        """Optimized strategy selection with performance improvements"""

        # Pre-calculate memory requirements only once
        estimated_memory_mb = self._estimate_memory_cached(table_metadata, total_records)

        # Optimized strategy selection with better thresholds
        if self.config.performance.enable_streaming and total_records > 50000:  # Lower threshold
            self.streaming_used = True
            return self._generate_with_optimized_streaming(table_metadata, total_records, foreign_key_data)
        elif self.config.performance.enable_parallel and total_records > 5000:  # Lower threshold
            return self._generate_with_optimized_parallel(table_metadata, total_records, foreign_key_data)
        else:
            return self._generate_with_optimized_adaptive(table_metadata, total_records, foreign_key_data)

    def _estimate_memory_cached(self, table_metadata: Dict[str, Any], total_records: int) -> float:
        """Cached memory estimation with proper key generation"""
        table_name = table_metadata.get("table_name", "unknown")
        column_count = len(table_metadata.get("columns", []))

        # Create a simple cache key based on table characteristics
        cache_key = f"{table_name}_{column_count}_{total_records}"

        # Check cache
        if hasattr(self, '_memory_estimate_cache') and cache_key in self._memory_estimate_cache:
            return self._memory_estimate_cache[cache_key]

        # Calculate memory estimate
        estimated_memory = self.parallel_generator._estimate_memory_requirements(table_metadata, total_records)

        # Cache the result
        if not hasattr(self, '_memory_estimate_cache'):
            self._memory_estimate_cache = {}
        self._memory_estimate_cache[cache_key] = estimated_memory

        return estimated_memory

    def _generate_with_optimized_streaming(self, table_metadata: Dict[str, Any],
                                           total_records: int,
                                           foreign_key_data: Dict[str, List]) -> List[Dict]:
        """Optimized streaming generation"""
        self.logger.info(f"📊 Using OPTIMIZED STREAMING generation strategy")

        table_name = table_metadata["table_name"]
        output_path = self.config.output.get_output_path(table_name)

        all_data = []
        batch_count = 0
        quality_scores = []

        # Optimized masking configuration
        enable_masking = self.config.security.enable_data_masking
        sensitivity_map = self._build_sensitivity_map_cached(table_metadata) if enable_masking else None

        # Use larger batches for better performance
        batch_size = min(10000, total_records // 10)  # Dynamic batch sizing

        # Optimized streaming with batch processing
        for batch in self.parallel_generator.generate_streaming(
                table_metadata=table_metadata,
                total_records=total_records,
                foreign_key_data=foreign_key_data,
                enable_masking=enable_masking,
                security_manager=self.security_manager if enable_masking else None,
                sensitivity_map=sensitivity_map
        ):
            batch_count += 1

            # Optimized validation - only for critical batches
            if self.config.validation.strict_mode and batch_count % 5 == 0:  # Less frequent validation
                self._validate_batch_optimized(batch, table_metadata, batch_count)

            # Efficient quality monitoring - less frequent checks
            if batch_count % 20 == 0 and self.config.validation.enable_data_quality_analysis:
                batch_quality = self._analyze_quality_sample(batch, table_metadata)
                if batch_quality:
                    quality_scores.append(batch_quality.get('data_quality_score', 0))

            # Keep smaller sample for final analysis
            if len(all_data) < 5000:  # Reduced sample size
                all_data.extend(batch[:min(len(batch), 5000 - len(all_data))])

            # Less frequent progress logging
            if batch_count % 50 == 0:  # Reduced logging frequency
                self._log_progress_efficient(batch_count, len(batch), total_records)

        self._log_streaming_completion_efficient(table_name, total_records, output_path)
        return all_data

    def _build_sensitivity_map_cached(self, table_metadata: Dict[str, Any]) -> Dict[str, str]:
        """Cached sensitivity map building with proper key generation"""
        table_name = table_metadata.get("table_name", "unknown")
        columns = table_metadata.get('columns', [])

        # Create a hashable cache key
        column_signature = tuple(
            (col.get("name", ""), col.get("sensitivity", "PUBLIC"))
            for col in columns
        )
        cache_key = f"{table_name}_sensitivity_{hash(column_signature)}"

        # Check cache
        if hasattr(self, '_sensitivity_cache') and cache_key in self._sensitivity_cache:
            return self._sensitivity_cache[cache_key]

        # Build sensitivity map
        sensitivity_map = {}
        for column in columns:
            sensitivity_level = column.get('sensitivity', 'PUBLIC')
            sensitivity_map[column.get('name', '')] = sensitivity_level

        # Cache the result
        if not hasattr(self, '_sensitivity_cache'):
            self._sensitivity_cache = {}
        self._sensitivity_cache[cache_key] = sensitivity_map

        return sensitivity_map

    def _validate_batch_optimized(self, batch: List[Dict], table_metadata: Dict[str, Any], batch_num: int):
        """Optimized batch validation with sampling"""
        if not batch or len(batch) < 100:
            return

        # Sample-based validation for large batches
        sample_size = min(100, len(batch))
        sample = batch[:sample_size] if len(batch) > sample_size else batch

        validation_results = self.validator.validate_batch(sample, table_metadata)

        if validation_results['invalid_records'] > 0:
            error_rate = (validation_results['invalid_records'] / validation_results['total_records']) * 100
            if error_rate > 10.0:  # Higher threshold for warnings
                self.logger.warning(f"⚠️ Batch {batch_num} sample has high error rate: {error_rate:.1f}%")

    def _analyze_quality_sample(self, batch: List[Dict], table_metadata: Dict[str, Any]) -> Optional[Dict]:
        """Efficient quality analysis on samples"""
        if not batch:
            return None

        # Use smaller sample for quality analysis
        sample = batch[:min(50, len(batch))]
        return self.data_quality_analyzer.analyze_distribution(sample, table_metadata)

    def _log_progress_efficient(self, batch_count: int, batch_size: int, total_records: int):
        """Efficient progress logging"""
        total_so_far = batch_count * batch_size
        progress_pct = (total_so_far / total_records) * 100
        memory_usage = self.parallel_generator.memory_monitor.get_memory_usage()
        self.logger.info(
            f"📈 Progress: {total_so_far:,}/{total_records:,} ({progress_pct:.1f}%) - Mem: {memory_usage:.1f}MB"
        )

    def _generate_with_optimized_parallel(self, table_metadata: Dict[str, Any],
                                          total_records: int,
                                          foreign_key_data: Dict[str, List]) -> List[Dict]:
        """Optimized parallel generation"""
        self.logger.info(f"⚡ Using OPTIMIZED PARALLEL generation strategy")

        # Smarter process vs thread selection
        use_processes = (total_records > 100000 and
                         self.config.performance.max_workers > 2 and
                         os.cpu_count() > 2)

        return self.parallel_generator.generate_parallel(
            table_metadata=table_metadata,
            total_records=total_records,
            foreign_key_data=foreign_key_data,
            use_processes=use_processes
        )

    def _generate_with_optimized_adaptive(self, table_metadata: Dict[str, Any],
                                          total_records: int,
                                          foreign_key_data: Dict[str, List]) -> List[Dict]:
        """Optimized adaptive generation"""
        self.logger.info(f"🤖 Using OPTIMIZED ADAPTIVE generation strategy")

        all_data = []

        # Optimized masking configuration
        enable_masking = self.config.security.enable_data_masking and total_records > 1000
        sensitivity_map = None

        if enable_masking:
            sensitivity_map = self._build_sensitivity_map_cached(table_metadata)

        # Optimized adaptive generator
        for batch in self.parallel_generator.generate_adaptive(
                table_metadata=table_metadata,
                total_records=total_records,
                foreign_key_data=foreign_key_data,
                enable_masking=enable_masking,
                security_manager=self.security_manager if enable_masking else None,
                sensitivity_map=sensitivity_map
        ):
            if isinstance(batch, list) and len(batch) > 0:
                all_data.extend(batch)

        return all_data

    def _optimized_post_processing_pipeline(self, data: List[Dict],
                                            table_metadata: Dict[str, Any],
                                            table_name: str) -> List[Dict]:
        """Optimized post-processing pipeline"""
        if not data:
            return data

        # Skip expensive operations for small datasets
        if len(data) < 1000:
            return data

        # Parallel post-processing for large datasets
        with ThreadPoolExecutor(max_workers=min(4, os.cpu_count())) as executor:
            futures = []

            # Quality analysis (if enabled)
            if self.config.validation.enable_data_quality_analysis:
                futures.append(executor.submit(self._analyze_quality_async, data, table_metadata, table_name))

            # Security processing (if enabled)
            if self.config.security.enable_data_masking:
                futures.append(executor.submit(self._apply_security_async, data, table_metadata))

            # Business rules (if enabled)
            if getattr(self.config.validation, 'enable_business_rules', False):
                futures.append(executor.submit(self._validate_business_rules_async, data, table_metadata))

            # Wait for completion
            for future in as_completed(futures):
                try:
                    result = future.result(timeout=30)  # 30 second timeout
                    if isinstance(result, list):  # Security processing returns modified data
                        data = result
                except Exception as e:
                    self.logger.warning(f"⚠️ Post-processing task failed: {e}")

        return data

    def _analyze_quality_async(self, data: List[Dict], table_metadata: Dict[str, Any], table_name: str):
        """Async quality analysis"""
        sample_size = min(2000, len(data))  # Smaller sample
        quality_analysis = self.data_quality_analyzer.analyze_distribution(
            data[:sample_size], table_metadata
        )

        if quality_analysis:
            self.generation_stats['quality_scores'][table_name] = quality_analysis.get('data_quality_score', 0.0)
            self._log_quality_insights_brief(table_name, quality_analysis)

    def _apply_security_async(self, data: List[Dict], table_metadata: Dict[str, Any]) -> List[Dict]:
        """Async security processing"""
        if not self.config.security.enable_data_masking:
            return data

        # Build sensitivity map
        sensitivity_map = {}
        sensitive_columns = []

        for column in table_metadata.get('columns', []):
            sensitivity_level = column.get('sensitivity', 'PUBLIC')
            sensitivity_map[column['name']] = sensitivity_level
            if sensitivity_level in ['PII', 'SENSITIVE']:
                sensitive_columns.append(column['name'])

        if not sensitive_columns:
            return data

        # Apply masking efficiently
        masked_data = self.security_manager.mask_sensitive_data(data, sensitivity_map)

        # Update statistics
        masking_operations = len(data) * len(sensitive_columns)
        self.generation_stats['security_operations'] += masking_operations

        return masked_data

    def _validate_business_rules_async(self, data: List[Dict], table_metadata: Dict[str, Any]):
        """Async business rules validation"""
        business_rules = table_metadata.get('business_rules', [])
        if not business_rules or not data:
            return

        # Sample-based validation for performance
        sample_size = min(500, len(data))
        rule_violations = self.data_quality_analyzer.validate_business_rules(
            data[:sample_size], business_rules
        )

        violation_count = rule_violations.get('total_violations', 0)
        if violation_count > 0:
            compliance_rate = rule_violations.get('compliance_rate', 1.0)
            self.logger.warning(f"⚖️ Business rule violations: {violation_count} (Compliance: {compliance_rate:.1%})")

    def _update_statistics_efficient(self, generated_data: List[Dict], start_time: datetime, table_name: str):
        """Efficient statistics update"""
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Batch update statistics
        self.generation_stats.update({
            'tables_processed': self.generation_stats['tables_processed'] + 1,
            'total_records_generated': self.generation_stats['total_records_generated'] + len(generated_data),
            'total_duration': self.generation_stats['total_duration'] + duration
        })

        # Update peak memory efficiently
        current_memory = self.parallel_generator.memory_monitor.get_memory_usage()
        if current_memory > self.generation_stats['memory_peak_mb']:
            self.generation_stats['memory_peak_mb'] = current_memory

    def _extract_foreign_key_data_cached(self, data: List[Dict], table_metadata: Dict[str, Any]) -> Dict[str, List]:
        """Cached foreign key data extraction - PRESERVES REFERENTIAL INTEGRITY"""
        fk_data = {}

        if not data:
            return fk_data

        # Cache key for this table's FK structure
        table_name = table_metadata.get('table_name', 'unknown')
        cache_key = f"{table_name}_fk"

        if cache_key in self._fk_cache:
            pk_columns = self._fk_cache[cache_key]
        else:
            pk_columns = self._get_primary_key_columns(table_metadata)
            self._fk_cache[cache_key] = pk_columns

        # Extract FK data efficiently - CRITICAL FOR REFERENTIAL INTEGRITY
        for pk_col in pk_columns:
            values = [row.get(pk_col) for row in data if row.get(pk_col) is not None]
            if values:
                fk_key = f"{table_name}.{pk_col}"
                # Keep enough values for referential integrity but limit for memory
                # Ensure we have sufficient values for dependent tables
                max_values = max(25000, len(values))  # Keep at least what we generated
                fk_data[fk_key] = values[:max_values]

                # Log FK data extraction for referential integrity tracking
                self.logger.debug(f"🔑 Extracted {len(fk_data[fk_key])} FK values from {table_name}.{pk_col}")

        return fk_data

    def _log_quality_insights_brief(self, table_name: str, quality_analysis: Dict[str, Any]):
        """Brief quality insights logging"""
        score = quality_analysis.get('data_quality_score', 0)
        issues_count = len(quality_analysis.get('issues', []))
        self.logger.info(f"📊 {table_name} quality: {score:.3f} ({issues_count} issues)")

    def _log_streaming_completion_efficient(self, table_name: str, total_records: int, output_path: str):
        """Efficient streaming completion logging"""
        try:
            if os.path.exists(output_path):
                file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
                self.logger.info(f"💾 {table_name}: {file_size_mb:.1f}MB, {total_records:,} records")
        except Exception:
            pass  # Skip file info if error

    def get_comprehensive_statistics(self) -> Dict[str, Any]:
        """Optimized statistics gathering"""
        # Get stats in parallel to reduce blocking time
        with ThreadPoolExecutor(max_workers=3) as executor:
            parallel_stats_future = executor.submit(self.parallel_generator.get_performance_stats)
            constraint_stats_future = executor.submit(self.data_generator.constraint_manager.get_constraint_statistics)
            profile_report_future = executor.submit(self.performance_profiler.get_profile_report)

            # Gather results
            try:
                parallel_stats = parallel_stats_future.result(timeout=5)
            except:
                parallel_stats = {}

            try:
                constraint_stats = constraint_stats_future.result(timeout=5)
            except:
                constraint_stats = {}

            try:
                profile_report = profile_report_future.result(timeout=5)
            except:
                profile_report = {}

        return {
            'generation_summary': self.generation_stats,
            'parallel_generator_performance': parallel_stats,
            'constraint_manager_performance': constraint_stats,
            'performance_profiling': profile_report,
            'memory_usage_mb': parallel_stats.get('memory_usage_mb', 0),
            'peak_memory_mb': self.generation_stats['memory_peak_mb'],
            'average_quality_score': (
                sum(self.generation_stats['quality_scores'].values()) /
                len(self.generation_stats['quality_scores'])
                if self.generation_stats['quality_scores'] else 0
            ),
            'security_operations_count': self.generation_stats['security_operations'],
            'total_errors': len(self.generation_stats['errors'])
        }

    def cleanup(self):
        """Optimized cleanup with garbage collection"""
        try:
            if hasattr(self.parallel_generator, 'cleanup'):
                self.parallel_generator.cleanup()
            if hasattr(self.data_generator, 'cleanup'):
                self.data_generator.cleanup()
        except Exception as e:
            self.logger.warning(f"Cleanup warning: {e}")

        # Clear instance caches
        if hasattr(self, '_batch_cache'):
            self._batch_cache.clear()
        if hasattr(self, '_fk_cache'):
            self._fk_cache.clear()

        # Clear new caches
        if hasattr(self, '_fk_constraints_cache'):
            self._fk_constraints_cache.clear()
        if hasattr(self, '_sensitivity_cache'):
            self._sensitivity_cache.clear()
        if hasattr(self, '_memory_estimate_cache'):
            self._memory_estimate_cache.clear()

        # Clear class-level caches periodically (every 10th instance)
        if hasattr(self, 'instance_id') and self.instance_id % 10 == 0:
            self._clear_class_caches()

        # Force garbage collection
        gc.collect()

    @classmethod
    def _clear_class_caches(cls):
        """Clear class-level caches periodically"""
        try:
            if hasattr(cls, '_constraint_manager_cache'):
                cls._constraint_manager_cache.clear()
            if hasattr(cls, '_validator_cache'):
                cls._validator_cache.clear()
            if hasattr(cls, '_quality_analyzer_cache'):
                cls._quality_analyzer_cache.clear()
            if hasattr(cls, '_security_manager_cache'):
                cls._security_manager_cache.clear()
        except Exception:
            pass

    def _get_primary_key_columns(self, table_metadata: Dict[str, Any]) -> List[str]:
        """Get primary key columns from table metadata"""
        # Check composite primary key first
        composite_pk = table_metadata.get("composite_primary_key", [])
        if composite_pk:
            return composite_pk

        # Look for individual primary key columns
        pk_columns = []
        for column in table_metadata.get("columns", []):
            constraints = column.get("constraints", []) + column.get("constraint", [])
            if "PK" in constraints:
                pk_columns.append(column["name"])

        return pk_columns


class OptimizedDataGenerationOrchestrator:
    """
    Highly optimized orchestrator with performance improvements
    """

    def __init__(self, config: GenerationConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.generation_engine = OptimizedDataGenerationEngine(config, logger)

        # Pre-compute table order to avoid repeated calculations
        self.ordered_tables = self._process_tables_in_dependency_order()

        # Pre-allocate data structures
        self.all_foreign_key_data = {}
        self.all_table_data = {}

        # Initialize orchestrator-level caches
        self._fk_constraints_cache = {}

    def run_data_generation(self, total_records: int, output_dir: str) -> Dict[str, List]:
        """Optimized main orchestration method"""

        start_time = datetime.now()

        self.logger.info(f"🚀 Starting OPTIMIZED data generation process")
        self.logger.info(f"📊 Configuration: {total_records:,} records per table")
        self.logger.info(f"⚡ Max workers: {self.config.performance.max_workers}")
        self.logger.info(f"💾 Max memory: {self.config.performance.max_memory_mb}MB")
        self.logger.info(f"📋 Processing {len(self.ordered_tables)} tables in dependency order")

        try:
            # Process tables with optimized parallel processing where possible
            if len(self.ordered_tables) > 1 and total_records < 50000:
                # For smaller datasets, we can process independent tables in parallel
                self._process_tables_optimized_parallel(total_records, output_dir)
            else:
                # For large datasets or single table, process sequentially but optimized
                self._process_tables_optimized_sequential(total_records, output_dir)

        except KeyboardInterrupt:
            self.logger.warning("⚠️ Data generation interrupted by user")
            return {}
        except Exception as e:
            self.logger.error(f"❌ Unexpected error during data generation: {e}")
            print_exc()
            raise
        finally:
            # Always clean up resources
            self.generation_engine.cleanup()

        # Generate optimized final report
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()
        self._generate_optimized_final_report(total_duration)

        self.logger.info(f"🎉 Optimized data generation completed in {total_duration:.2f} seconds!")
        return self.all_table_data

    def _process_tables_optimized_sequential(self, total_records: int, output_dir: str):
        """Optimized sequential table processing - PRESERVES REFERENTIAL INTEGRITY"""
        for i, table_metadata in enumerate(self.ordered_tables, 1):
            table_name = table_metadata["table_name"]
            self.logger.info(f"📊 Processing table {i}/{len(self.ordered_tables)}: {table_name}")

            # Get foreign key constraints efficiently - CRITICAL FOR REFERENTIAL INTEGRITY
            foreign_key_data = self._collect_foreign_key_constraints_cached(table_metadata)

            if foreign_key_data:
                fk_count = sum(len(values) for values in foreign_key_data.values())
                self.logger.info(f"🔗 FK constraints: {len(foreign_key_data)} types, {fk_count:,} total values")

                # Validate referential integrity constraints
                self._validate_referential_integrity_constraints(foreign_key_data, table_metadata)

            # Generate data with optimizations
            generated_records, generated_fk_data = self.generation_engine.generate_data_for_table(
                table_metadata, total_records, foreign_key_data, output_dir
            )

            # Store results efficiently
            self.all_table_data[table_name] = generated_records
            self._update_foreign_key_data_efficient(generated_fk_data)

            # Export data if not already streamed
            if not self.generation_engine.streaming_used:
                self._export_table_data_optimized(generated_records, table_metadata)

            # Efficient memory management
            if i % 3 == 0:  # Every 3 tables, cleanup memory
                gc.collect()
                current_memory = self.generation_engine.parallel_generator.memory_monitor.get_memory_usage()
                self.logger.info(f"💾 Memory after cleanup: {current_memory:.1f}MB")

    def _validate_referential_integrity_constraints(self, foreign_key_data: Dict[str, List],
                                                    table_metadata: Dict[str, Any]):
        """Validate that referential integrity constraints can be satisfied"""
        table_name = table_metadata.get("table_name", "unknown")
        foreign_keys = table_metadata.get("foreign_keys", [])

        integrity_issues = []

        for fk in foreign_keys:
            parent_table = fk.get("parent_table", "")
            parent_column = fk.get("parent_column", "")
            child_column = fk.get("column", fk.get("child_column", ""))
            fk_key = f"{parent_table}.{parent_column}"

            available_values = foreign_key_data.get(fk_key, [])

            if not available_values and parent_table != table_name:  # Skip self-references
                integrity_issues.append({
                    'parent_table': parent_table,
                    'parent_column': parent_column,
                    'child_column': child_column,
                    'issue': 'No available parent values'
                })

        if integrity_issues:
            self.logger.warning(f"⚠️ Referential integrity issues found for {table_name}:")
            for issue in integrity_issues:
                self.logger.warning(
                    f"   - {issue['child_column']} -> {issue['parent_table']}.{issue['parent_column']}: {issue['issue']}")

            # Optionally raise exception in strict mode
            if hasattr(self.config.validation, 'strict_referential_integrity') and \
                    self.config.validation.strict_referential_integrity:
                raise ValueError(f"Referential integrity constraints cannot be satisfied for table {table_name}")
        else:
            self.logger.info(f"✅ Referential integrity constraints validated for {table_name}")

    def _process_tables_optimized_parallel(self, total_records: int, output_dir: str):
        """Process independent tables in parallel for small datasets"""
        # Identify tables that can be processed in parallel (no dependencies)
        independent_tables, dependent_tables = self._separate_independent_tables()

        if independent_tables:
            self.logger.info(f"⚡ Processing {len(independent_tables)} independent tables in parallel")

            # Process independent tables in parallel
            with ThreadPoolExecutor(max_workers=min(len(independent_tables), 4)) as executor:
                futures = {}

                for table_metadata in independent_tables:
                    future = executor.submit(
                        self._process_single_table_optimized,
                        table_metadata, total_records, {}, output_dir
                    )
                    futures[future] = table_metadata["table_name"]

                # Collect results
                for future in as_completed(futures):
                    table_name = futures[future]
                    try:
                        generated_records, generated_fk_data = future.result()
                        self.all_table_data[table_name] = generated_records
                        self._update_foreign_key_data_efficient(generated_fk_data)
                    except Exception as e:
                        self.logger.error(f"❌ Failed to process {table_name} in parallel: {e}")

        # Process dependent tables sequentially
        if dependent_tables:
            self.logger.info(f"📋 Processing {len(dependent_tables)} dependent tables sequentially")
            for table_metadata in dependent_tables:
                self._process_single_table_with_dependencies(table_metadata, total_records, output_dir)

    def _separate_independent_tables(self) -> Tuple[List[Dict], List[Dict]]:
        """Separate tables into independent and dependent groups"""
        independent = []
        dependent = []

        for table in self.ordered_tables:
            foreign_keys = table.get("foreign_keys", [])
            has_dependencies = any(
                fk.get("parent_table", "") != table.get("table_name", "")
                for fk in foreign_keys
            )

            if has_dependencies:
                dependent.append(table)
            else:
                independent.append(table)

        return independent, dependent

    def _process_single_table_optimized(self, table_metadata: Dict[str, Any],
                                        total_records: int,
                                        foreign_key_data: Dict[str, List],
                                        output_dir: str) -> Tuple[List[Dict], Dict]:
        """Process a single table with optimizations"""
        return self.generation_engine.generate_data_for_table(
            table_metadata, total_records, foreign_key_data, output_dir
        )

    def _process_single_table_with_dependencies(self, table_metadata: Dict[str, Any],
                                                total_records: int,
                                                output_dir: str):
        """Process a single table that has dependencies"""
        table_name = table_metadata["table_name"]

        # Get foreign key constraints
        foreign_key_data = self._collect_foreign_key_constraints_cached(table_metadata)

        # Generate data
        generated_records, generated_fk_data = self.generation_engine.generate_data_for_table(
            table_metadata, total_records, foreign_key_data, output_dir
        )

        # Store results
        self.all_table_data[table_name] = generated_records
        self._update_foreign_key_data_efficient(generated_fk_data)

        # Export if needed
        if not self.generation_engine.streaming_used:
            self._export_table_data_optimized(generated_records, table_metadata)

    def _update_foreign_key_data_efficient(self, generated_fk_data: Dict[str, List]):
        """Efficiently update foreign key data - PRESERVES REFERENTIAL INTEGRITY"""
        for key, values in generated_fk_data.items():
            if key not in self.all_foreign_key_data:
                self.all_foreign_key_data[key] = []

            # Extend and limit in one operation
            self.all_foreign_key_data[key].extend(values)

            # More aggressive limit for memory efficiency but preserve integrity
            # Keep a reasonable pool size to maintain referential integrity
            max_fk_pool_size = max(25000, len(values) * 2)  # Ensure we keep enough values
            if len(self.all_foreign_key_data[key]) > max_fk_pool_size:
                # Keep the most recent values to maintain integrity for dependent tables
                self.all_foreign_key_data[key] = self.all_foreign_key_data[key][-max_fk_pool_size // 2:]

            # Log FK pool updates for referential integrity tracking
            if len(values) > 0:
                self.logger.debug(
                    f"🔗 Updated FK pool {key}: +{len(values)} values, total: {len(self.all_foreign_key_data[key])}")

    def _collect_foreign_key_constraints_cached(self, table_metadata: Dict[str, Any]) -> Dict[str, List]:
        """Cached foreign key constraint collection with proper serialization"""
        # Create a hashable cache key from table metadata
        table_name = table_metadata.get("table_name", "unknown")
        foreign_keys = table_metadata.get("foreign_keys", [])

        # Create a simple cache key from the foreign key structure
        fk_signature = tuple(
            (fk.get("parent_table", ""), fk.get("parent_column", ""))
            for fk in foreign_keys
        )
        cache_key = f"{table_name}_{hash(fk_signature)}"

        # Check if we have this cached
        if hasattr(self, '_fk_constraints_cache') and cache_key in self._fk_constraints_cache:
            # Update with current data and return
            cached_constraints = self._fk_constraints_cache[cache_key].copy()
            for fk_key in cached_constraints:
                cached_constraints[fk_key] = self.all_foreign_key_data.get(fk_key, [])
            return cached_constraints

        # Build constraints - PRESERVE REFERENTIAL INTEGRITY LOGIC
        constraints = {}
        for fk in foreign_keys:
            parent_table = fk.get("parent_table", "")
            parent_column = fk.get("parent_column", "")
            fk_key = f"{parent_table}.{parent_column}"

            available_values = self.all_foreign_key_data.get(fk_key, [])
            constraints[fk_key] = available_values

            # Log FK availability for referential integrity tracking
            if available_values:
                self.logger.debug(f"🔗 FK constraint {fk_key}: {len(available_values):,} values available")
            else:
                self.logger.warning(
                    f"⚠️ FK constraint {fk_key}: No values available - may impact referential integrity")

        # Cache the structure (not the actual data)
        if not hasattr(self, '_fk_constraints_cache'):
            self._fk_constraints_cache = {}
        self._fk_constraints_cache[cache_key] = {fk_key: [] for fk_key in constraints.keys()}

        return constraints

    def _export_table_data_optimized(self, data: List[Dict], table_metadata: Dict):
        """Optimized data export with minimal overhead"""
        if not data:
            return

        table_name = table_metadata["table_name"]

        try:
            # Use context manager for automatic resource management
            with UnifiedWriterFactory.create_writer(
                    table_name=table_name,
                    config=self.config.output,
                    logger=self.logger
            ) as writer:
                # For small datasets, write in single batch
                if len(data) <= 10000:
                    writer.write_batch(data)
                else:
                    # For larger datasets, use optimized batching
                    batch_size = 25000  # Larger batches for better performance
                    for i in range(0, len(data), batch_size):
                        batch = data[i:i + batch_size]
                        writer.write_batch(batch)

            self.logger.info(f"💾 Exported {len(data):,} records")

        except Exception as e:
            self.logger.error(f"❌ Export failed for {table_name}: {e}")
            raise

    def _process_tables_in_dependency_order(self) -> List[Dict]:
        """Optimized table ordering with cycle detection - PRESERVES REFERENTIAL INTEGRITY"""
        tables = self.config.tables
        processed = []
        remaining = tables.copy()
        processed_names = set()

        # Build dependency graph for faster lookups - CRITICAL FOR REFERENTIAL INTEGRITY
        dependency_graph = {}
        for table in tables:
            table_name = table.get("table_name", "")
            dependencies = set()

            for fk in table.get("foreign_keys", []):
                parent_table = fk.get("parent_table", "")
                if parent_table != table_name:  # Avoid self-references
                    dependencies.add(parent_table)

            dependency_graph[table_name] = dependencies

        # Log dependency information for referential integrity validation
        self.logger.info("🔗 Table dependency analysis for referential integrity:")
        for table_name, deps in dependency_graph.items():
            if deps:
                self.logger.info(f"   {table_name} depends on: {', '.join(deps)}")

        # Topological sort with optimization - ENSURES REFERENTIAL INTEGRITY ORDER
        iteration = 0
        max_iterations = len(tables) * 2

        while remaining and iteration < max_iterations:
            iteration += 1
            progress_made = False

            # Find tables with no unmet dependencies
            ready_tables = []
            for table in remaining:
                table_name = table.get("table_name", "")
                dependencies = dependency_graph.get(table_name, set())

                # Check if all dependencies are satisfied
                if dependencies.issubset(processed_names):
                    ready_tables.append(table)

            if ready_tables:
                # Process all ready tables
                for table in ready_tables:
                    processed.append(table)
                    processed_names.add(table.get("table_name", ""))
                    remaining.remove(table)
                    progress_made = True

                    # Log processing order for referential integrity verification
                    deps = dependency_graph.get(table.get("table_name", ""), set())
                    if deps:
                        self.logger.debug(f"✅ {table.get('table_name', '')} ready - dependencies {deps} satisfied")

            if not progress_made and remaining:
                # Handle circular dependencies - CRITICAL FOR REFERENTIAL INTEGRITY
                self.logger.warning("⚠️ Circular dependency detected - analyzing for referential integrity issues")

                # Identify circular dependencies
                remaining_names = {table.get("table_name", "") for table in remaining}
                circular_deps = []

                for table in remaining:
                    table_name = table.get("table_name", "")
                    deps = dependency_graph.get(table_name, set())
                    circular_in_remaining = deps.intersection(remaining_names)
                    if circular_in_remaining:
                        circular_deps.append((table_name, circular_in_remaining))

                if circular_deps:
                    self.logger.warning("🔄 Circular dependencies found:")
                    for table_name, circular_deps_set in circular_deps:
                        self.logger.warning(f"   {table_name} <-> {circular_deps_set}")

                # Process remaining tables anyway but warn about potential integrity issues
                self.logger.warning("⚠️ Processing remaining tables - referential integrity may be compromised")
                processed.extend(remaining)
                break

        self.logger.info(
            f"📋 Final processing order (preserves referential integrity): {[t.get('table_name', '') for t in processed]}")
        return processed

    def _generate_optimized_final_report(self, total_duration: float):
        """Generate optimized final report with minimal overhead"""
        stats = self.generation_engine.get_comprehensive_statistics()
        output_dir = self.config.output.directory

        # Streamlined report structure
        report = {
            "timestamp": datetime.now().isoformat(),
            "generator": "OptimizedParallelDataGenerator",
            "performance": {
                "duration_seconds": total_duration,
                "total_records": stats['generation_summary']['total_records_generated'],
                "records_per_second": stats['generation_summary'][
                                          'total_records_generated'] / total_duration if total_duration > 0 else 0,
                "peak_memory_mb": stats.get('peak_memory_mb', 0),
                "tables_processed": stats['generation_summary']['tables_processed'],
                "errors": len(stats['generation_summary']['errors'])
            },
            "quality": {
                "average_score": stats.get('average_quality_score', 0),
                "tables_analyzed": len(stats['generation_summary'].get('quality_scores', {}))
            },
            "security": {
                "operations": stats.get('security_operations_count', 0),
                "masking_enabled": self.config.security.enable_data_masking
            }
        }

        # Save report efficiently
        report_path = os.path.join(output_dir, "optimized_generation_report.json")
        try:
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            self.logger.info(f"📊 Report saved: {report_path}")
        except Exception as e:
            self.logger.error(f"❌ Report save failed: {e}")

        # Log optimized summary
        self._log_optimized_summary(report)

    def _log_optimized_summary(self, report: Dict):
        """Log optimized summary to console"""
        perf = report["performance"]
        quality = report["quality"]
        security = report["security"]

        self.logger.info("=" * 60)
        self.logger.info("🎉 OPTIMIZED GENERATION SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Tables: {perf['tables_processed']}")
        self.logger.info(f"Records: {perf['total_records']:,}")
        self.logger.info(f"Duration: {perf['duration_seconds']:.2f}s")
        self.logger.info(f"Speed: {perf['records_per_second']:,.0f} records/sec")
        self.logger.info(f"Peak Memory: {perf['peak_memory_mb']:.1f}MB")

        if quality['tables_analyzed'] > 0:
            self.logger.info(f"Quality Score: {quality['average_score']:.3f}")

        if security['masking_enabled']:
            self.logger.info(f"Security Ops: {security['operations']:,}")

        if perf['errors'] > 0:
            self.logger.warning(f"⚠️ Errors: {perf['errors']}")
        else:
            self.logger.info("✅ No errors")

        self.logger.info("=" * 60)


def main(config: GenerationConfig, total_records: int = None, output_dir: str = "./output") -> Dict[str, List]:
    """
    Optimized main function with performance improvements
    """
    logger = logging.getLogger(__name__)

    actual_total_records = total_records or config.rows

    # Create optimized orchestrator
    orchestrator = OptimizedDataGenerationOrchestrator(config, logger)

    # Log startup with minimal overhead
    logger.info(f"🚀 OPTIMIZED Data Generation Engine")
    logger.info(f"📊 Target: {actual_total_records:,} records/table")
    logger.info(f"⚡ Workers: {config.performance.max_workers}")
    logger.info(f"💾 Memory: {config.performance.max_memory_mb}MB")

    # Run optimized generation
    return orchestrator.run_data_generation(actual_total_records, output_dir)


def setup_logging(config: GenerationConfig) -> logging.Logger:
    """Optimized logging setup"""
    log_level = getattr(logging, config.logging.level.upper(), logging.INFO)

    # Simplified format for better performance
    log_format = "%(asctime)s | %(levelname)-8s | %(message)s"

    # Configure with minimal handlers
    handlers = [logging.StreamHandler(sys.stdout)]

    if config.logging.file_path:
        log_dir = os.path.dirname(config.logging.file_path)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        file_handler = logging.FileHandler(config.logging.file_path)
        file_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(file_handler)

    # Configure with performance optimizations
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=handlers,
        force=True
    )

    # Suppress verbose loggers
    for logger_name in ['faker', 'pandas', 'urllib3']:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    return logging.getLogger(__name__)


def parse_arguments():
    """Optimized argument parsing"""
    parser = argparse.ArgumentParser(
        description='Optimized Data Generator with Enhanced Performance',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Essential arguments only
    parser.add_argument('--config', '-c', required=True, help='Configuration file (JSON)')
    parser.add_argument('--output_dir', '-o', default='./output', help='Output directory')
    parser.add_argument('--rows', '-r', type=int, help='Records per table')
    parser.add_argument('--max_workers', '-w', type=int, help='Max worker threads/processes')
    parser.add_argument('--max_memory', '-m', type=int, help='Max memory (MB)')

    # Performance flags
    parser.add_argument('--enable_streaming', action='store_true', help='Enable streaming')
    parser.add_argument('--enable_parallel', action='store_true', help='Enable parallel processing')
    parser.add_argument('--enable_quality_analysis', action='store_true', help='Enable quality analysis')
    parser.add_argument('--enable_masking', action='store_true', help='Enable data masking')
    parser.add_argument('--enable_all_features', action='store_true', help='Enable all features')

    # Output options
    parser.add_argument('--format', '-f',
                        choices=['csv', 'json', 'jsonl', 'parquet', 'sql_query'],
                        help='Output format')
    parser.add_argument('--log_level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='Logging level')

    return parser.parse_args()


def apply_command_line_overrides(config: GenerationConfig, args):
    """Optimized configuration override"""
    # Direct assignments for better performance
    if args.rows:
        config.rows = args.rows
    if args.max_workers:
        config.performance.max_workers = args.max_workers
    if args.max_memory:
        config.performance.max_memory_mb = args.max_memory
    if args.format:
        config.output.format = args.format
    if args.output_dir:
        config.output.change_directory(args.output_dir)
    if args.log_level:
        config.logging.level = args.log_level

    # Feature flags with batch updates
    feature_updates = {}
    if args.enable_streaming:
        feature_updates['enable_streaming'] = True
    if args.enable_parallel:
        feature_updates['enable_parallel'] = True

    # Apply performance updates in batch
    for key, value in feature_updates.items():
        setattr(config.performance, key, value)

    # Validation flags
    if args.enable_quality_analysis:
        config.validation.enable_data_quality_analysis = True
    if args.enable_masking:
        config.security.enable_data_masking = True

    # Enable all features efficiently
    if args.enable_all_features:
        config.performance.enable_streaming = True
        config.performance.enable_parallel = True
        config.validation.enable_data_quality_analysis = True
        config.security.enable_data_masking = True


if __name__ == "__main__":
    print("🚀 OPTIMIZED DATA GENERATOR WITH ENHANCED PERFORMANCE")
    print("=" * 70)

    start_time = datetime.now()

    try:
        # Parse arguments
        args = parse_arguments()

        # Load configuration
        config_manager = ConfigurationManager()
        config = config_manager.load_configuration(args.config, getattr(args, 'environment', None))

        # Apply overrides
        apply_command_line_overrides(config, args)

        # Setup logging
        logger = setup_logging(config)

        # Log configuration summary
        logger.info("📋 Optimized Configuration:")
        logger.info(f"   Records/table: {config.rows:,}")
        logger.info(f"   Workers: {config.performance.max_workers}")
        logger.info(f"   Memory: {config.performance.max_memory_mb}MB")
        logger.info(f"   Tables: {len(config.tables)}")

        # Feature summary
        features = []
        if config.performance.enable_streaming:
            features.append("Streaming")
        if config.performance.enable_parallel:
            features.append("Parallel")
        if config.validation.enable_data_quality_analysis:
            features.append("Quality")
        if config.security.enable_data_masking:
            features.append("Security")

        logger.info(f"   Features: {', '.join(features) if features else 'Basic'}")

        # Generate output directory
        timestamp = start_time.strftime("%Y%m%d_%H%M%S")
        output_directory = os.path.join(args.output_dir, timestamp)

        # Run optimized generation
        logger.info("🚀 Starting optimized generation...")

        generated_data = main(
            config=config,
            total_records=config.rows,
            output_dir=output_directory
        )

        # Calculate execution time
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()

        # Success message
        print("\n" + "=" * 70)
        print("🎉 OPTIMIZED GENERATION COMPLETED!")
        print("=" * 70)
        print(f"✅ Generated: {len(generated_data)} tables")
        print(f"⚡ Performance: Enhanced with optimizations")
        print(f"💾 Memory: Optimized management")
        print(f"📁 Output: {output_directory}")
        print(f"⏱️ Time: {total_duration:.2f} seconds")
        print("=" * 70)

        if features:
            print(f"🔧 Features: {', '.join(features)}")
            print("   • Streaming: Large dataset support")
            print("   • Parallel: Multi-threaded processing")
            print("   • Quality: Data validation")
            print("   • Security: Data protection")
            print("=" * 70)

    except FileNotFoundError as e:
        print(f"❌ Config file not found: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print(f"\n⚠️ Generation interrupted")
        sys.exit(130)
    except Exception as e:
        print(f"❌ Error: {e}")
        print_exc()
        sys.exit(1)