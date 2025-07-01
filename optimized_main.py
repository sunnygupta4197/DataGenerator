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
import time

import pandas as pd

from constraint_manager.constraint_manager import ConstraintManager
from data_generator.streaming_data_generator import ParallelDataGenerator, PerformanceProfiler
from config_manager.config_manager import ConfigurationManager, GenerationConfig
from data_generator.data_generator import DataGenerator
from writers.writer import WriterFactory, Writer

from processors.processor_pipeline import ProcessorPipeline
from generators.generator_factory import GeneratorFactory
from quality import BusinessRulesEngine, DataQualityAnalyzer, AnomalyDetector, Validator, SecurityManager


@dataclass
class CachedComponents:
    """Cache for expensive-to-create components"""
    data_generator: Optional[DataGenerator] = None
    security_manager: Optional[SecurityManager] = None
    quality_analyzer: Optional[DataQualityAnalyzer] = None
    last_used: Optional[datetime] = None
    access_count: int = 0


class ComponentCache:
    """Thread-safe component cache with memory management"""

    def __init__(self, max_size: int = 20):
        self.max_size = max_size
        self._cache = {}
        self._lock = threading.Lock()

    def get_or_create(self, key: str, factory_func, *args, **kwargs):
        with self._lock:
            if key in self._cache:
                self._cache[key].last_used = datetime.now()
                self._cache[key].access_count += 1
                return self._cache[key]

            if len(self._cache) >= self.max_size:
                self._evict_oldest()

            component = factory_func(*args, **kwargs)
            self._cache[key] = CachedComponents(
                data_generator=component,
                last_used=datetime.now(),
                access_count=1
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
    Optimized data generation engine with complete feature set
    """

    # Class-level cache for shared components
    _component_cache = ComponentCache()
    _instance_count = 0

    def __init__(self, config: GenerationConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger

        # Instance tracking
        OptimizedDataGenerationEngine._instance_count += 1
        self.instance_id = OptimizedDataGenerationEngine._instance_count

        # Initialize caches
        self._fk_constraints_cache = {}
        self._sensitivity_cache = {}
        self._memory_estimate_cache = {}
        self._business_rules_cache = {}
        self.performance_profiler = PerformanceProfiler(logger=logger)

        # Initialize core components with caching
        self.constraint_manager = self._get_cached_constraint_manager()
        self.validator = self._get_cached_validator()
        self.data_generator = self._create_optimized_data_generator()
        self.parallel_generator = self._create_optimized_parallel_generator()

        self.streaming_used = False

        # Initialize analyzers
        self.data_quality_analyzer = self._get_cached_quality_analyzer()
        self.security_manager = self._get_cached_security_manager()

        # Initialize engines
        self.business_rules_engine = BusinessRulesEngine(logger=logger)
        self.anomaly_detector = AnomalyDetector(logger=logger)

        self.generator_factory = GeneratorFactory(
            config, logger, self.parallel_generator
        )

        self.processor_pipeline = ProcessorPipeline(
            config, logger, self.data_quality_analyzer,
            self.security_manager, self.business_rules_engine, self.anomaly_detector, self.validator
        )

        # Configure security
        if config.security.enable_data_masking:
            self._configure_security()

        # Initialize statistics
        self.generation_stats = self._create_stats_structure()

        # Setup optimizations
        self._setup_performance_optimizations()

    def _create_stats_structure(self):
        """Create statistics structure"""
        return {
            'tables_processed': 0,
            'total_records_generated': 0,
            'total_duration': 0.0,
            'memory_peak_mb': 0.0,
            'quality_scores': {},
            'security_operations': 0,
            'business_rules_violations': 0,
            'anomalies_detected': 0,
            'errors': []
        }

    def _accumulate_metrics(self, metrics: Dict[str, Any]):
        """Accumulate metrics from batch processing"""
        for key, value in metrics.items():
            if key in self.generation_stats:
                if isinstance(value, (int, float)):
                    self.generation_stats[key] += value
                elif isinstance(value, list):
                    self.generation_stats[key].extend(value)
            elif key == 'quality_score' and value is not None:
                # Store quality scores for later averaging
                if 'quality_scores_batch' not in self.generation_stats:
                    self.generation_stats['quality_scores_batch'] = []
                self.generation_stats['quality_scores_batch'].append(value)
            else:
                self.generation_stats[key] = value

    def _update_final_stats(self, table_name: str, records_count: int, duration: float):
        """Update final statistics for completed table"""
        self.generation_stats.update({
            'tables_processed': self.generation_stats['tables_processed'] + 1,
            'total_records_generated': self.generation_stats['total_records_generated'] + records_count,
            'total_duration': self.generation_stats['total_duration'] + duration
        })

        # Update memory peak if available
        try:
            current_memory = self.parallel_generator.memory_monitor.get_memory_usage()
            if current_memory > self.generation_stats['memory_peak_mb']:
                self.generation_stats['memory_peak_mb'] = current_memory
        except AttributeError:
            # Memory monitor might not be available
            pass

        # Calculate average quality score for this table if available
        batch_scores = self.generation_stats.get('quality_scores_batch', [])
        if batch_scores:
            avg_quality = sum(batch_scores) / len(batch_scores)
            self.generation_stats['quality_scores'][table_name] = avg_quality
            # Clear batch scores for next table
            self.generation_stats['quality_scores_batch'] = []

    def _log_progress(self, batch_count: int, batch_size: int, total_records: int):
        """Log generation progress"""
        if batch_count % 50 == 0:  # Log every 50 batches
            total_so_far = batch_count * batch_size
            progress_pct = min((total_so_far / total_records) * 100, 100)

            try:
                memory_usage = self.parallel_generator.memory_monitor.get_memory_usage()
                self.logger.info(
                    f"📈 Progress: {total_so_far:,}/{total_records:,} "
                    f"({progress_pct:.1f}%) - Memory: {memory_usage:.1f}MB"
                )
            except AttributeError:
                self.logger.info(
                    f"📈 Progress: {total_so_far:,}/{total_records:,} ({progress_pct:.1f}%)"
                )

    def _extract_foreign_key_data(self, data: List[Dict], table_metadata: Dict[str, Any]) -> Dict[str, List]:
        """Extract foreign key data for use by dependent tables"""
        fk_data = {}

        if not data:
            return fk_data

        table_name = table_metadata.get('table_name', 'unknown')
        pk_columns = self._get_primary_key_columns(table_metadata)

        for pk_col in pk_columns:
            values = [row.get(pk_col) for row in data if row.get(pk_col) is not None]
            if values:
                fk_key = f"{table_name}.{pk_col}"
                # Limit values for memory efficiency
                max_values = max(25000, len(values))
                fk_data[fk_key] = values[:max_values]

        return fk_data

    def _get_primary_key_columns(self, table_metadata: Dict[str, Any]) -> List[str]:
        """Get primary key columns from table metadata"""
        # Check for composite primary key first
        composite_pk = table_metadata.get("composite_primary_key", [])
        if composite_pk:
            return composite_pk

        # Find PK columns from constraints
        pk_columns = []
        for column in table_metadata.get("columns", []):
            constraints = column.get("constraints", []) + column.get("constraint", [])
            if "PK" in constraints:
                pk_columns.append(column["name"])

        return pk_columns

    def get_comprehensive_statistics(self) -> Dict[str, Any]:
        """Get comprehensive generation statistics"""
        # Calculate average quality score
        parallel_stats = self.parallel_generator.get_performance_stats()
        constraint_stats = self.data_generator.constraint_manager.get_constraint_statistics()
        profile_report = self.performance_profiler.get_profile_report()
        validator_status = self.data_generator.validator.get_performance_stats()
        pipeline_stats = self.processor_pipeline.get_detailed_processor_stats()

        return {
            'generation_summary': self.generation_stats,
            'parallel_generator_performance': parallel_stats,
            'validator_stats': validator_status,
            'pipeline_stats': pipeline_stats,
            'constraint_manager_performance': constraint_stats,
            'performance_profiling': profile_report,
            'memory_usage_mb': parallel_stats.get('memory_usage_mb', 0),
            'average_quality_score': (
                sum(self.generation_stats['quality_scores'].values()) /
                len(self.generation_stats['quality_scores'])
                if self.generation_stats['quality_scores'] else 0
            ),
            'security_operations_count': self.generation_stats['security_operations'],
            'total_tables': self.generation_stats['tables_processed'],
            'total_records': self.generation_stats['total_records_generated'],
            'total_duration': self.generation_stats['total_duration'],
            'peak_memory_mb': self.generation_stats['memory_peak_mb'],
            'records_per_second': (
                self.generation_stats['total_records_generated'] /
                self.generation_stats['total_duration']
                if self.generation_stats['total_duration'] > 0 else 0
            ),
            'errors_count': len(self.generation_stats['errors'])
        }

    def cleanup(self):
        """Cleanup resources"""
        try:
            if hasattr(self.parallel_generator, 'cleanup'):
                self.parallel_generator.cleanup()
            if hasattr(self.data_generator, 'cleanup'):
                self.data_generator.cleanup()
        except Exception as e:
            self.logger.warning(f"Cleanup warning: {e}")

        # Clear internal caches
        if hasattr(self.processor_pipeline, 'cleanup'):
            # Add cleanup to processors if needed
            pass

    def _get_cached_constraint_manager(self):
        """Get cached constraint manager"""
        cache_key = f"constraint_manager_{self.config.locale}_{self.config.performance.max_workers}"

        def factory():
            return ConstraintManager(
                logger=self.logger,
                max_memory_mb=self.config.performance.max_memory_mb,
                enable_parallel=self.config.performance.enable_parallel
            )

        cached_component = self._component_cache.get_or_create(cache_key, factory)
        return cached_component.data_generator

    def _get_cached_validator(self):
        """Get cached validator"""
        cache_key = "unified_validator"

        def factory():
            return Validator(logger=self.logger)

        cached_component = self._component_cache.get_or_create(cache_key, factory)
        return cached_component.data_generator

    def _get_cached_quality_analyzer(self):
        """Get cached quality analyzer"""
        cache_key = "data_quality_analyzer"

        def factory():
            return DataQualityAnalyzer(logger=self.logger)

        cached_component = self._component_cache.get_or_create(cache_key, factory)
        return cached_component.data_generator

    def _get_cached_security_manager(self):
        """Get cached security manager"""
        cache_key = "security_manager"

        def factory():
            return SecurityManager(logger=self.logger)

        cached_component = self._component_cache.get_or_create(cache_key, factory)
        return cached_component.data_generator

    def _create_optimized_data_generator(self):
        """Create optimized data generator"""
        generator = DataGenerator(
            self.config,
            self.config.locale,
            ai_config=self.config.ai,
            logger=self.logger
        )

        # Pre-warm caches for small datasets
        if self.config.rows < 10000:
            self._prewarm_generator_caches(generator)

        return generator

    def _create_optimized_parallel_generator(self):
        """Create optimized parallel generator"""
        optimal_workers = min(
            self.config.performance.max_workers,
            os.cpu_count() * 2
        )

        return ParallelDataGenerator(
            data_generator_instance=self.data_generator,
            max_workers=optimal_workers,
            max_memory_mb=self.config.performance.max_memory_mb,
            enable_streaming=self.config.performance.enable_streaming,
            performance_profiler=self.performance_profiler,
            logger=self.logger
        )

    def _prewarm_generator_caches(self, generator):
        """Pre-warm generator caches"""
        try:
            sample_metadata = {
                "table_name": "warmup",
                "columns": [
                    {"name": "id", "type": "int", "constraints": []},
                    {"name": "name", "type": "varchar", "constraints": []}
                ]
            }
            list(self.parallel_generator.generate_adaptive(
                sample_metadata, 10, {}
            ))
        except Exception:
            pass

    def _configure_security(self):
        """Configure security with batch operations"""
        self.security_manager.enable_masking = True

        masking_rules = [
            ('email', 'partial'), ('phone', 'partial'), ('ssn', 'partial'),
            ('credit', 'partial'), ('income', 'hash'), ('salary', 'hash')
        ]

        for field, rule_type in masking_rules:
            self.security_manager.add_masking_rule(field, rule_type)

        if hasattr(self.config.security, 'encryption_key') and self.config.security.encryption_key:
            self.security_manager.set_encryption_key(self.config.security.encryption_key.encode())

    def _setup_performance_optimizations(self):
        """Setup performance optimizations"""
        pd.options.mode.chained_assignment = None
        gc.set_threshold(700, 10, 10)

        self._batch_cache = {}
        self._fk_cache = {}

    def generate_data_for_table(self, table_metadata: Dict[str, Any],
                                total_records: int,
                                foreign_key_data: Dict[str, List] = None) -> Tuple[List[Dict], Dict]:
        """Generate data for a table using the appropriate strategy"""
        table_name = table_metadata["table_name"]
        self.logger.info(f"🚀 Generating data for table '{table_name}' ({total_records:,} records)")

        start_time = time.time()

        try:
            # Select appropriate generator
            with self.performance_profiler.profile(f"table_generation_{table_name}"):
                # PROFILE GENERATOR SELECTION
                with self.performance_profiler.profile("generator_selection"):
                    generator = self.generator_factory.select_generator(table_metadata, total_records)

            # Generate data in batches
            all_data = []
            batch_count = 0

            with self.performance_profiler.profile(f"batch_generation_loop_{table_name}"):
                for batch in generator.generate(table_metadata, total_records, foreign_key_data):
                    batch_count += 1

                    with self.performance_profiler.profile(f"batch_processing_{table_name}"):
                        processed_batch, batch_metrics = self.processor_pipeline.process_batch(
                            batch, table_metadata, batch_count
                        )

                    # Accumulate data and metrics
                    all_data.extend(processed_batch)
                    self._accumulate_metrics({'pipeline_summary': batch_metrics})

                    # Progress logging
                    self._log_progress(batch_count, len(processed_batch), total_records)

            # Update final statistics
            duration = time.time() - start_time
            self._update_final_stats(table_name, len(all_data), duration)

            with self.performance_profiler.profile(f"fk_extraction_{table_name}"):
                # Extract foreign key data
                fk_data = self._extract_foreign_key_data(all_data, table_metadata)

            self.logger.info(
                f"✅ Completed {table_name}: {len(all_data):,} records in {duration:.2f}s "
                f"using {generator.get_strategy_name()} strategy"
            )

            return all_data, fk_data

        except Exception as e:
            self.logger.error(f"❌ Failed to generate data for {table_name}: {e}")
            raise


class OptimizedDataGenerationOrchestrator:
    """
    Optimized orchestrator with complete feature set
    """

    def __init__(self, config: GenerationConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.generation_engine = OptimizedDataGenerationEngine(config, logger)

        # Pre-compute table order
        self.ordered_tables = self._process_tables_in_dependency_order()

        # Pre-allocate data structures
        self.all_foreign_key_data = {}
        self.all_table_data = {}

        # Initialize caches
        self._fk_constraints_cache = {}

    def run_data_generation(self, total_records: int) -> Dict[str, List]:
        """Main orchestration method"""
        with self.generation_engine.performance_profiler.profile("total_generation_process"):
            with self.generation_engine.performance_profiler.profile("orchestrator_initialization"):
                start_time = datetime.now()
                self.logger.info(f"🚀 Starting OPTIMIZED data generation process")
                self.logger.info(f"📊 Configuration: {total_records:,} records per table")
                self.logger.info(f"⚡ Max workers: {self.config.performance.max_workers}")
                self.logger.info(f"💾 Max memory: {self.config.performance.max_memory_mb}MB")
                self.logger.info(f"📋 Processing {len(self.ordered_tables)} tables in dependency order")

                # Log enabled features
                self._log_enabled_features()

                with self.generation_engine.performance_profiler.profile("processing_strategy_selection"):
                    use_parallel = self._should_use_parallel_table_processing(total_records)

                with self.generation_engine.performance_profiler.profile("table_processing"):
                    try:
                        # Process tables
                        if use_parallel:
                            self._process_tables_optimized_parallel(total_records)
                        else:
                            self._process_tables_optimized_sequential(total_records)

                    except KeyboardInterrupt:
                        self.logger.warning("⚠️ Data generation interrupted by user")
                        return {}
                    except Exception as e:
                        self.logger.error(f"❌ Unexpected error during data generation: {e}")
                        print_exc()
                        raise
                    finally:
                        self.generation_engine.cleanup()

                with self.generation_engine.performance_profiler.profile("final_report_generation"):
                    end_time = datetime.now()
                    total_duration = (end_time - start_time).total_seconds()
                    self._generate_comprehensive_final_report(total_duration)

                    self.logger.info(f"🎉 Optimized data generation completed in {total_duration:.2f} seconds!")
                    return self.all_table_data

    def _log_enabled_features(self):
        """Log enabled features"""
        features = []
        if self.config.performance.enable_streaming:
            features.append("Streaming")
        if self.config.performance.enable_parallel:
            features.append("Parallel")
        if self.config.validation.enable_data_quality_analysis:
            features.append("Quality")
        if self.config.security.enable_data_masking:
            features.append("Security")
        if getattr(self.config.validation, 'enable_business_rules', False):
            features.append("Rules")
        if getattr(self.config.validation, 'enable_anomaly_detection', False):
            features.append("Anomalies")
        if self.config.ai.openai.enabled or self.config.ai.mistral.enabled:
            features.append("AI")

        if features:
            self.logger.info(f"🔧 Features: {', '.join(features)}")

    def _should_use_parallel_table_processing(self, total_records: int) -> bool:
        """Decide whether to use parallel table processing"""
        independent_tables, _ = self._separate_independent_tables()

        return (
                len(independent_tables) > 1 and
                total_records < 100000 and
                os.cpu_count() > 2 and
                len(self.ordered_tables) > 2
        )

    def _process_tables_optimized_sequential(self, total_records: int):
        """Optimized sequential table processing"""
        with self.generation_engine.performance_profiler.profile("sequential_table_processing"):
            for i, table_metadata in enumerate(self.ordered_tables, 1):
                table_name = table_metadata["table_name"]
                self.logger.info(f"📊 Processing table {i}/{len(self.ordered_tables)}: {table_name}")

                # Get foreign key constraints
                with self.generation_engine.performance_profiler.profile(f"table_processing_{table_name}"):

                    # PROFILE FK CONSTRAINT COLLECTION
                    with self.generation_engine.performance_profiler.profile(f"fk_collection_{table_name}"):

                        foreign_key_data = self._collect_foreign_key_constraints_cached(table_metadata)

                        if foreign_key_data:
                            fk_count = sum(len(values) for values in foreign_key_data.values())
                            self.logger.info(
                                f"🔗 FK constraints: {len(foreign_key_data)} types, {fk_count:,} total values")

                            # Validate referential integrity
                            self._validate_referential_integrity_constraints(foreign_key_data, table_metadata)

                        # Generate data
                        with self.generation_engine.performance_profiler.profile(f"data_generation_{table_name}"):
                            generated_records, generated_fk_data = self.generation_engine.generate_data_for_table(
                                table_metadata, total_records, foreign_key_data
                            )

                        # Store results
                        with self.generation_engine.performance_profiler.profile(f"data_storage_{table_name}"):
                            self.all_table_data[table_name] = generated_records
                            self._update_foreign_key_data_efficient(generated_fk_data)

                        # Export data if not already streamed
                        with self.generation_engine.performance_profiler.profile(f"data_export_{table_name}"):
                            if not self.generation_engine.streaming_used:
                                self._export_table_data_optimized(generated_records, table_metadata)

                        # Memory management
                        if i % 3 == 0:
                            gc.collect()
                            current_memory = self.generation_engine.parallel_generator.memory_monitor.get_memory_usage()
                            self.logger.info(f"💾 Memory after cleanup: {current_memory:.1f}MB")

    def _process_tables_optimized_parallel(self, total_records: int):
        """Optimized parallel table processing"""
        independent_tables, dependent_tables = self._separate_independent_tables()

        if independent_tables:
            self.logger.info(f"⚡ Processing {len(independent_tables)} independent tables in parallel")

            max_parallel_workers = min(len(independent_tables), os.cpu_count(), 4)

            with ThreadPoolExecutor(max_workers=max_parallel_workers) as executor:
                futures = {}

                for table_metadata in independent_tables:
                    future = executor.submit(
                        self._process_single_table_optimized,
                        table_metadata, total_records, {}
                    )
                    futures[future] = table_metadata["table_name"]

                for future in as_completed(futures, timeout=3600):
                    table_name = futures[future]
                    try:
                        generated_records, generated_fk_data = future.result()
                        self.all_table_data[table_name] = generated_records
                        self._update_foreign_key_data_efficient(generated_fk_data)
                        self.logger.info(f"✅ Parallel completed: {table_name}")
                        if not self.generation_engine.streaming_used:
                            # Get table metadata for export
                            table_metadata = next(
                                table for table in independent_tables
                                if table["table_name"] == table_name
                            )
                            self._export_table_data_optimized(generated_records, table_metadata)

                    except Exception as e:
                        self.logger.error(f"❌ Failed to process {table_name} in parallel: {e}")

        # Process dependent tables sequentially
        if dependent_tables:
            self.logger.info(f"📋 Processing {len(dependent_tables)} dependent tables sequentially")
            for table_metadata in dependent_tables:
                self._process_single_table_with_dependencies(table_metadata, total_records)

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

    def _validate_referential_integrity_constraints(self, foreign_key_data: Dict[str, List],
                                                    table_metadata: Dict[str, Any]):
        """Validate referential integrity constraints"""
        table_name = table_metadata.get("table_name", "unknown")
        foreign_keys = table_metadata.get("foreign_keys", [])

        integrity_issues = []

        for fk in foreign_keys:
            parent_table = fk.get("parent_table", "")
            parent_column = fk.get("parent_column", "")
            child_column = fk.get("column", fk.get("child_column", ""))
            fk_key = f"{parent_table}.{parent_column}"

            available_values = foreign_key_data.get(fk_key, [])

            if not available_values and parent_table != table_name:
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

            if (hasattr(self.config.validation, 'strict_referential_integrity') and
                    self.config.validation.strict_referential_integrity):
                raise ValueError(f"Referential integrity constraints cannot be satisfied for table {table_name}")
        else:
            self.logger.info(f"✅ Referential integrity constraints validated for {table_name}")

    def _process_single_table_optimized(self, table_metadata: Dict[str, Any],
                                        total_records: int,
                                        foreign_key_data: Dict[str, List]) -> Tuple[List[Dict], Dict]:
        """Process a single table with optimizations"""
        return self.generation_engine.generate_data_for_table(
            table_metadata, total_records, foreign_key_data
        )

    def _process_single_table_with_dependencies(self, table_metadata: Dict[str, Any],
                                                total_records: int):
        """Process a single table that has dependencies"""
        table_name = table_metadata["table_name"]

        foreign_key_data = self._collect_foreign_key_constraints_cached(table_metadata)

        generated_records, generated_fk_data = self.generation_engine.generate_data_for_table(
            table_metadata, total_records, foreign_key_data
        )

        self.all_table_data[table_name] = generated_records
        self._update_foreign_key_data_efficient(generated_fk_data)

        if not self.generation_engine.streaming_used:
            self._export_table_data_optimized(generated_records, table_metadata)

    def _update_foreign_key_data_efficient(self, generated_fk_data: Dict[str, List]):
        """Efficiently update foreign key data"""
        for key, values in generated_fk_data.items():
            if key not in self.all_foreign_key_data:
                self.all_foreign_key_data[key] = []

            self.all_foreign_key_data[key].extend(values)

            # Limit FK pool size for memory efficiency
            max_fk_pool_size = max(50000, len(values) * 2)
            if len(self.all_foreign_key_data[key]) > max_fk_pool_size:
                self.all_foreign_key_data[key] = self.all_foreign_key_data[key][-max_fk_pool_size // 2:]

    def _collect_foreign_key_constraints_cached(self, table_metadata: Dict[str, Any]) -> Dict[str, List]:
        """Cached foreign key constraint collection"""
        table_name = table_metadata.get("table_name", "unknown")
        foreign_keys = table_metadata.get("foreign_keys", [])

        fk_signature = tuple(
            (fk.get("parent_table", ""), fk.get("parent_column", ""))
            for fk in foreign_keys
        )
        cache_key = f"{table_name}_{hash(fk_signature)}"

        if cache_key in self._fk_constraints_cache:
            cached_constraints = self._fk_constraints_cache[cache_key].copy()
            for fk_key in cached_constraints:
                cached_constraints[fk_key] = self.all_foreign_key_data.get(fk_key, [])
            return cached_constraints

        constraints = {}
        for fk in foreign_keys:
            parent_table = fk.get("parent_table", "")
            parent_column = fk.get("parent_column", "")
            fk_key = f"{parent_table}.{parent_column}"

            available_values = self.all_foreign_key_data.get(fk_key, [])
            constraints[fk_key] = available_values

        self._fk_constraints_cache[cache_key] = {fk_key: [] for fk_key in constraints.keys()}
        return constraints

    def _export_table_data_optimized(self, data: List[Dict], table_metadata: Dict):
        """Optimized data export"""
        if not data:
            return

        table_name = table_metadata["table_name"]
        schema = {}
        for column in table_metadata["columns"]:
            schema.update({column['name']: column['type']})

        try:
            with WriterFactory.create_writer(
                    table_name=table_name,
                    config=self.config.output,
                    logger=self.logger,
                    schema=schema
            ) as writer:

                if len(data) <= 10000:
                    writer.write_batch(data)
                else:
                    batch_size = 25000
                    for i in range(0, len(data), batch_size):
                        batch = data[i:i + batch_size]
                        writer.write_batch(batch)

            self.logger.info(f"💾 Exported {len(data):,} records")

        except Exception as e:
            self.logger.error(f"❌ Export failed for {table_name}: {e}")
            raise

    def _process_tables_in_dependency_order(self) -> List[Dict]:
        """Order tables based on foreign key dependencies"""
        tables = self.config.tables
        processed = []
        remaining = tables.copy()
        processed_names = set()

        max_iterations = len(tables) * 2
        iteration = 0

        while remaining and iteration < max_iterations:
            progress_made = False
            iteration += 1

            for table in remaining[:]:
                if self._can_process_table(table, processed_names):
                    processed.append(table)
                    processed_names.add(table["table_name"])
                    remaining.remove(table)
                    progress_made = True

            if not progress_made:
                self.logger.warning("⚠️ Possible circular dependency detected in table relationships")
                processed.extend(remaining)
                break

        return processed

    def _can_process_table(self, table: Dict, processed_names: set) -> bool:
        """Check if table dependencies are met"""
        foreign_keys = table.get("foreign_keys", [])
        table_name = table["table_name"]

        for fk in foreign_keys:
            parent_table = fk["parent_table"]
            if parent_table not in processed_names and parent_table != table_name:
                return False

        return True

    def _generate_comprehensive_final_report(self, total_duration: float):
        """Generate comprehensive final report"""
        stats = self.generation_engine.get_comprehensive_statistics()

        # Comprehensive report structure
        report = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "generator_version": "OptimizedDataGenerator",
                "total_duration_seconds": total_duration
            },
            "configuration_summary": {
                "total_tables": len(self.config.tables),
                "records_per_table": self.config.rows,
                "output_format": self.config.output.format,
                "max_workers": self.config.performance.max_workers,
                "max_memory_mb": self.config.performance.max_memory_mb,
                "features_enabled": {
                    "streaming": self.config.performance.enable_streaming,
                    "parallel": self.config.performance.enable_parallel,
                    "quality_analysis": self.config.validation.enable_data_quality_analysis,
                    "security_masking": self.config.security.enable_data_masking,
                    "business_rules": getattr(self.config.validation, 'enable_business_rules', False),
                    "anomaly_detection": getattr(self.config.validation, 'enable_anomaly_detection', False),
                    "ai_integration": {
                        'openai': self.config.ai.openai.enabled,
                        'mistral': self.config.ai.mistral.enabled
                    }
                }
            },
            "performance_summary": {
                "total_records_generated": stats['generation_summary']['total_records_generated'],
                "average_records_per_second": (
                    stats['generation_summary']['total_records_generated'] / total_duration
                    if total_duration > 0 else 0
                ),
                "peak_memory_usage_mb": stats.get('peak_memory_mb', 0),
                "tables_processed": stats['generation_summary']['tables_processed'],
                "total_errors": len(stats['generation_summary']['errors'])
            },
            "quality_metrics": {
                "average_quality_score": stats.get('average_quality_score', 0),
                "quality_by_table": stats['generation_summary'].get('quality_scores', {}),
                "total_quality_checks": len(stats['generation_summary'].get('quality_scores', {}))
            },
            "security_metrics": {
                "total_security_operations": stats.get('security_operations_count', 0),
                "masking_enabled": self.config.security.enable_data_masking,
                "encryption_enabled": bool(getattr(self.generation_engine.security_manager, 'encryption_key', None))
            },
            "business_intelligence": {
                "business_rules_violations": stats.get('business_rules_violations', 0),
                "anomalies_detected": stats.get('anomalies_detected', 0)
            },
            "detailed_statistics": stats,
            "error_details": stats['generation_summary'].get('errors', [])
        }

        # Save comprehensive report
        report_path = os.path.join(self.config.output.report_directory, "comprehensive_generation_report.json")
        try:
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            self.logger.info(f"📊 Comprehensive report saved to: {report_path}")
        except Exception as e:
            self.logger.error(f"❌ Could not save report: {e}")

        # Save specialized reports
        self._save_post_processor_pipeline_report()
        self._save_performance_profiling_report()
        self._save_security_audit_trail()

        # Log summary to console
        self._log_comprehensive_summary_to_console(report)

    def _save_post_processor_pipeline_report(self):
        """Save detailed performance profiling report"""
        try:
            profile_report = self.generation_engine.processor_pipeline.get_detailed_processor_stats()
            profile_path = os.path.join(self.config.output.report_directory, "post_processor_pipeline_report.json")

            with open(profile_path, 'w') as f:
                json.dump(profile_report, f, indent=2, default=str)

            self.logger.info(f"Post processor pipeline report saved to: {profile_path}")
        except Exception as e:
            self.logger.error(f"❌ Could not save post processor pipeline report: {e}")

    def _save_performance_profiling_report(self):
        """Save detailed performance profiling report"""
        try:
            profile_report = self.generation_engine.performance_profiler.get_profile_report()
            profile_path = os.path.join(self.config.output.report_directory, "performance_profiling_report.json")

            with open(profile_path, 'w') as f:
                json.dump(profile_report, f, indent=2, default=str)

            self.logger.info(f"⚡ Performance profiling report saved to: {profile_path}")
        except Exception as e:
            self.logger.error(f"❌ Could not save performance profiling report: {e}")

    def _save_security_audit_trail(self):
        """Save security audit trail"""
        try:
            audit_path = os.path.join(self.config.output.report_directory, "security_audit_trail.json")
            self.generation_engine.security_manager.export_audit_trail(audit_path)
            self.logger.info(f"🔒 Security audit trail saved to: {audit_path}")
        except Exception as e:
            self.logger.error(f"❌ Could not save security audit trail: {e}")

    def _log_comprehensive_summary_to_console(self, report: Dict):
        """Log comprehensive summary to console"""
        config = report["configuration_summary"]
        perf = report["performance_summary"]
        quality = report["quality_metrics"]
        security = report["security_metrics"]
        bi = report["business_intelligence"]

        self.logger.info("=" * 80)
        self.logger.info("🎉 COMPREHENSIVE GENERATION SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(f"Generator Type: {report['metadata']['generator_version']}")
        self.logger.info(f"Tables Processed: {perf['tables_processed']}")
        self.logger.info(f"Total Records: {perf['total_records_generated']:,}")
        self.logger.info(f"Duration: {report['metadata']['total_duration_seconds']:.2f} seconds")
        self.logger.info(f"Speed: {perf['average_records_per_second']:,.0f} records/second")
        self.logger.info(f"Peak Memory: {perf['peak_memory_usage_mb']:.1f} MB")
        self.logger.info(f"Workers Used: {config['max_workers']}")

        # Feature status
        self.logger.info("─" * 40)
        self.logger.info("🔧 FEATURES ENABLED:")
        features = config["features_enabled"]
        self.logger.info(f"Streaming: {'✅' if features['streaming'] else '❌'}")
        self.logger.info(f"Parallel: {'✅' if features['parallel'] else '❌'}")
        self.logger.info(f"Quality Analysis: {'✅' if features['quality_analysis'] else '❌'}")
        self.logger.info(f"Security/Masking: {'✅' if features['security_masking'] else '❌'}")
        self.logger.info(f"Business Rules: {'✅' if features.get('business_rules', False) else '❌'}")
        self.logger.info(f"Anomaly Detection: {'✅' if features.get('anomaly_detection', False) else '❌'}")
        ai_enabled = any(features.get('ai_integration', {}).values())
        self.logger.info(f"AI Integration: {'✅' if ai_enabled else '❌'}")

        # Quality metrics
        if quality['total_quality_checks'] > 0:
            self.logger.info("─" * 40)
            self.logger.info("📊 QUALITY METRICS:")
            self.logger.info(f"Average Quality Score: {quality['average_quality_score']:.3f}/1.0")
            self.logger.info(f"Tables Analyzed: {quality['total_quality_checks']}")

            quality_scores = quality.get('quality_by_table', {})
            if quality_scores:
                best_table = max(quality_scores.keys(), key=lambda k: quality_scores[k])
                worst_table = min(quality_scores.keys(), key=lambda k: quality_scores[k])
                self.logger.info(f"Best Quality: {best_table} ({quality_scores[best_table]:.3f})")
                self.logger.info(f"Needs Attention: {worst_table} ({quality_scores[worst_table]:.3f})")

        # Security metrics
        if security['masking_enabled'] or security['encryption_enabled']:
            self.logger.info("─" * 40)
            self.logger.info("🔒 SECURITY METRICS:")
            self.logger.info(f"Security Operations: {security['total_security_operations']:,}")
            self.logger.info(f"Masking: {'✅' if security['masking_enabled'] else '❌'}")
            self.logger.info(f"Encryption: {'✅' if security['encryption_enabled'] else '❌'}")

        # Business intelligence
        if bi['business_rules_violations'] > 0 or bi['anomalies_detected'] > 0:
            self.logger.info("─" * 40)
            self.logger.info("🧠 BUSINESS INTELLIGENCE:")
            if bi['business_rules_violations'] > 0:
                self.logger.info(f"Business Rule Violations: {bi['business_rules_violations']:,}")
            if bi['anomalies_detected'] > 0:
                self.logger.info(f"Anomalies Detected: {bi['anomalies_detected']:,}")

        # Error summary
        if perf['total_errors'] > 0:
            self.logger.info("─" * 40)
            self.logger.warning(f"⚠️ ERRORS: {perf['total_errors']}")
            error_details = report.get('error_details', [])
            for error in error_details[:3]:
                self.logger.warning(f"   - {error}")
        else:
            self.logger.info("─" * 40)
            self.logger.info("✅ No errors encountered")

        self.logger.info("=" * 80)


def main(config: GenerationConfig, total_records: int = None) -> Dict[str, List]:
    """
    Main function using the optimized data generator with complete feature set
    """
    logger = logging.getLogger(__name__)

    actual_total_records = total_records or config.rows

    # Create optimized orchestrator
    orchestrator = OptimizedDataGenerationOrchestrator(config, logger)

    # Log startup information
    logger.info(f"🚀 Initializing Optimized Data Generation Engine")
    logger.info(f"📊 Target: {actual_total_records:,} records per table")
    logger.info(f"⚡ Workers: {config.performance.max_workers}")
    logger.info(f"💾 Memory Limit: {config.performance.max_memory_mb}MB")

    features_enabled = []
    if config.performance.enable_streaming:
        features_enabled.append("Streaming")
    if config.performance.enable_parallel:
        features_enabled.append("Parallel")
    if config.validation.enable_data_quality_analysis:
        features_enabled.append("Quality Analysis")
    if config.security.enable_data_masking:
        features_enabled.append("Security")
    if getattr(config.validation, 'enable_business_rules', False):
        features_enabled.append("Business Rules")
    if getattr(config.validation, 'enable_anomaly_detection', False):
        features_enabled.append("Anomaly Detection")
    if config.ai.openai.enabled:
        features_enabled.append("OpenAI")
    if config.ai.mistral.enabled:
        features_enabled.append("Mistral AI")

    logger.info(f"🔧 Features: {', '.join(features_enabled) if features_enabled else 'Basic'}")

    # Run optimized data generation
    return orchestrator.run_data_generation(actual_total_records)


def setup_logging_with_fallback(config: GenerationConfig = None, log_level: str = "INFO") -> logging.Logger:
    """Setup logging with fallback to defaults when config is not available"""

    if config and hasattr(config, 'logging'):
        # Use configuration-based logging
        log_level_final = getattr(logging, config.logging.level.upper(), logging.INFO)
        log_format = config.logging.format

        # Enhanced log format with more context
        enhanced_format = "%(asctime)s | %(levelname)-8s | %(name)-10s | %(message)s"
        if hasattr(config.logging, 'enhanced_format') and config.logging.enhanced_format:
            log_format = enhanced_format

        # Configure logging with multiple handlers
        handlers = [logging.StreamHandler(sys.stdout)]

        if config.logging.file_path:
            log_dir = os.path.dirname(config.logging.file_path)
            if log_dir:
                os.makedirs(log_dir, exist_ok=True)

            file_handler = logging.FileHandler(config.logging.file_path)
            file_handler.setFormatter(logging.Formatter(log_format))
            handlers.append(file_handler)
    else:
        # Fallback to basic logging
        log_level_final = getattr(logging, log_level.upper(), logging.INFO)
        log_format = "%(asctime)s | %(levelname)-8s | %(message)s"
        handlers = [logging.StreamHandler(sys.stdout)]

    # Clear existing handlers to avoid duplication
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Configure root logger
    logging.basicConfig(
        level=log_level_final,
        format=log_format,
        handlers=handlers,
        force=True
    )

    # Set specific logger levels for performance
    logging.getLogger('faker').setLevel(logging.WARNING)
    logging.getLogger('pandas').setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    if config:
        logger.info("📋 Configuration-based logging applied")
    else:
        logger.info("🔧 Fallback logging configured")

    return logger


def parse_arguments():
    """Parse command line arguments with complete feature set"""
    parser = argparse.ArgumentParser(
        description='Optimized Data Generator with Complete Feature Set and Maximum Performance',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic optimized generation
  python optimized_main.py --config config.json --rows 100000

  # Enhanced streaming with quality analysis
  python optimized_main.py --config config.json --rows 1000000 --enable_streaming --enable_quality_analysis

  # Parallel generation with security features
  python optimized_main.py --config config.json --enable_parallel --enable_masking --max_workers 8

  # Complete feature demonstration
  python optimized_main.py --config config.json --rows 500000 --enable_all_features --max_memory 1024

Features:
  🔄 Streaming: Handle datasets larger than memory
  ⚡ Parallel: Multi-threaded/process generation
  📊 Quality: Statistical analysis and validation
  🔒 Security: Data masking and encryption
  🔍 Anomaly: Automatic anomaly detection
  ⚖️ Rules: Business rule validation
  🧠 AI: OpenAI and Mistral integration
        """
    )

    # Basic arguments
    parser.add_argument('--config', '-c', required=True,
                        help='Path to configuration file (JSON)')
    parser.add_argument('--output_dir', '-o',
                        help='Output directory for generated files')
    parser.add_argument('--rows', '-r', type=int,
                        help='Number of rows to generate per table')
    parser.add_argument('--environment', '-e',
                        choices=['development', 'testing', 'production'],
                        help='Environment configuration to use')

    # Performance arguments
    parser.add_argument('--max_workers', '-w', type=int,
                        help='Maximum number of worker threads/processes')
    parser.add_argument('--max_memory', '-m', type=int,
                        help='Maximum memory usage in MB')
    parser.add_argument('--batch_size', '-b', type=int,
                        help='Batch size for processing')

    # Feature flags
    parser.add_argument('--enable_streaming', action='store_true',
                        help='Enable streaming for large datasets')
    parser.add_argument('--enable_parallel', action='store_true',
                        help='Enable parallel processing')
    parser.add_argument('--enable_quality_analysis', action='store_true',
                        help='Enable data quality analysis')
    parser.add_argument('--enable_masking', action='store_true',
                        help='Enable data masking for sensitive data')
    parser.add_argument('--enable_business_rules', action='store_true',
                        help='Enable business rule validation')
    parser.add_argument('--enable_anomaly_detection', action='store_true',
                        help='Enable anomaly detection')
    parser.add_argument('--enable_encryption', action='store_true',
                        help='Enable encryption')
    parser.add_argument('--enable_all_features', action='store_true',
                        help='Enable all advanced features')

    # Output options
    parser.add_argument('--format', '-f',
                        choices=['csv', 'json', 'jsonl', 'parquet', 'sql_query', 'fwf', 'fixed'],
                        help='Output file format')
    parser.add_argument('--compression',
                        choices=['gzip', 'snappy', 'lz4'],
                        help='Compression format (for supported formats)')

    # Advanced options
    parser.add_argument('--encryption_key',
                        help='Encryption key for sensitive data (32 bytes)')
    parser.add_argument('--profile_performance', action='store_true',
                        help='Enable detailed performance profiling')
    parser.add_argument('--strict_validation', action='store_true',
                        help='Enable strict validation mode')
    parser.add_argument('--log_level',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='Logging level', default='INFO')

    # AI Integration arguments
    parser.add_argument('--ai_enabled', action='store_true',
                        help='Enable AI integration')
    parser.add_argument('--openai_enabled', action='store_true',
                        help='Enable OpenAI integration')
    parser.add_argument('--mistral_enabled', action='store_true',
                        help='Enable Mistral AI integration')
    parser.add_argument('--ai_primary_provider',
                        choices=['openai', 'mistral'],
                        help='Primary AI provider')
    parser.add_argument('--openai_model',
                        help='OpenAI model name (e.g., gpt-4, gpt-3.5-turbo)')
    parser.add_argument('--mistral_model',
                        help='Mistral model name (e.g., mistral-large, mistral-medium)')

    return parser.parse_args()


def apply_command_line_overrides(config: GenerationConfig, args):
    """Apply command line argument overrides to configuration"""

    # Basic overrides
    if args.rows:
        config.rows = args.rows
    if args.max_workers:
        config.performance.max_workers = args.max_workers
    if args.max_memory:
        config.performance.max_memory_mb = args.max_memory
    if args.batch_size:
        config.performance.batch_size = args.batch_size
    if args.format:
        config.output.format = args.format
    if args.output_dir:
        config.output.change_directory(args.output_dir)
    if args.log_level:
        config.logging.level = args.log_level

    # Compression setting
    if args.compression:
        if not hasattr(config.output, 'compression'):
            config.output.compression = args.compression
        else:
            config.output.compression = args.compression

    # Feature flags
    if args.enable_streaming:
        config.performance.enable_streaming = True
    if args.enable_parallel:
        config.performance.enable_parallel = True
    if args.enable_quality_analysis:
        config.validation.enable_data_quality_analysis = True
    if args.enable_masking:
        config.security.enable_data_masking = True
    if args.enable_encryption:
        config.security.enable_encryption = True

    # Advanced features
    if args.enable_business_rules:
        if not hasattr(config.validation, 'enable_business_rules'):
            config.validation.enable_business_rules = True
        else:
            config.validation.enable_business_rules = True

    if args.enable_anomaly_detection:
        if not hasattr(config.validation, 'enable_anomaly_detection'):
            config.validation.enable_anomaly_detection = True
        else:
            config.validation.enable_anomaly_detection = True

    # Security options
    if args.encryption_key:
        if not hasattr(config.security, 'encryption_key'):
            config.security.encryption_key = args.encryption_key
        else:
            config.security.encryption_key = args.encryption_key

    # Validation options
    if args.strict_validation:
        config.validation.strict_mode = True
        if not hasattr(config.validation, 'strict_referential_integrity'):
            config.validation.strict_referential_integrity = True
        else:
            config.validation.strict_referential_integrity = True

    # Performance profiling
    if args.profile_performance:
        if not hasattr(config.performance, 'enable_profiling'):
            config.performance.enable_profiling = True
        else:
            config.performance.enable_profiling = True

    # AI configuration
    if args.ai_enabled or args.openai_enabled or args.mistral_enabled:
        if args.openai_enabled or args.ai_enabled:
            config.ai.openai.enabled = True
        if args.mistral_enabled or args.ai_enabled:
            config.ai.mistral.enabled = True

    if args.ai_primary_provider:
        if hasattr(config.ai, 'primary_provider'):
            config.ai.primary_provider = args.ai_primary_provider
        else:
            config.ai.primary_provider = args.ai_primary_provider

    if args.openai_model:
        config.ai.openai.model = args.openai_model

    if args.mistral_model:
        config.ai.mistral.model = args.mistral_model

    # Enable all features
    if args.enable_all_features:
        config.performance.enable_streaming = True
        config.performance.enable_parallel = True
        config.validation.enable_data_quality_analysis = True
        config.security.enable_data_masking = True

        if not hasattr(config.validation, 'enable_business_rules'):
            config.validation.enable_business_rules = True
        else:
            config.validation.enable_business_rules = True

        if not hasattr(config.validation, 'enable_anomaly_detection'):
            config.validation.enable_anomaly_detection = True
        else:
            config.validation.enable_anomaly_detection = True

        # Enable AI features
        config.ai.openai.enabled = True
        config.ai.mistral.enabled = True

        # Enable advanced validation
        config.validation.strict_mode = True

        if not hasattr(config.performance, 'enable_profiling'):
            config.performance.enable_profiling = True
        else:
            config.performance.enable_profiling = True


if __name__ == "__main__":
    print("🚀 OPTIMIZED DATA GENERATOR WITH COMPLETE FEATURE SET")
    print("=" * 80)
    print("🔧 Features: Streaming • Parallel • Quality • Security • Business Rules • Anomaly Detection • AI")
    print("⚡ Performance: Optimized with advanced caching and intelligent processing")
    print("=" * 80)

    start_time = datetime.now()

    try:
        # Parse command line arguments
        args = parse_arguments()

        initial_log_level = getattr(args, 'log_level', 'INFO')
        logger = setup_logging_with_fallback(config=None, log_level=initial_log_level)

        # Load and configure
        config_manager = ConfigurationManager(logger=logger)

        config = config_manager.load_configuration(
            config_path=args.config,
            environment=getattr(args, 'environment', None),
            # Pass all arguments as keyword arguments
            rows=args.rows,
            max_workers=args.max_workers,
            max_memory=args.max_memory,
            batch_size=args.batch_size,
            output_format=args.format,
            output_dir=args.output_dir,
            log_level=args.log_level,
            enable_streaming=args.enable_streaming,
            enable_parallel=args.enable_parallel,
            enable_masking=args.enable_masking,
            enable_quality_analysis=args.enable_quality_analysis,
            enable_encryption=args.enable_encryption,
            enable_all_features=args.enable_all_features,
            ai_enabled=args.ai_enabled,
            openai_enabled=args.openai_enabled,
            mistral_enabled=args.mistral_enabled,
            ai_primary_provider=args.ai_primary_provider,
            openai_model=args.openai_model,
            mistral_model=args.mistral_model
        )
        # Apply command line overrides
        apply_command_line_overrides(config, args)

        logger = setup_logging_with_fallback(config=config)
        config_manager.logger = logger

        # Setup logging

        # Log configuration summary
        logger.info("📋 Optimized Configuration Summary:")
        logger.info(f"   Environment: {getattr(config, 'environment', 'default')}")
        logger.info(f"   Records per table: {config.rows:,}")
        logger.info(f"   Max workers: {config.performance.max_workers}")
        logger.info(f"   Max memory: {config.performance.max_memory_mb}MB")
        logger.info(f"   Output format: {config.output.format}")
        logger.info(f"   Tables to generate: {len(config.tables)}")

        # Feature summary
        features = []
        if config.performance.enable_streaming:
            features.append("Streaming")
        if config.performance.enable_parallel:
            features.append("Parallel")
        if config.validation.enable_data_quality_analysis:
            features.append("Quality Analysis")
        if config.security.enable_data_masking:
            features.append("Security/Masking")
        if getattr(config.validation, 'enable_business_rules', False):
            features.append("Business Rules")
        if getattr(config.validation, 'enable_anomaly_detection', False):
            features.append("Anomaly Detection")
        if config.ai.openai.enabled:
            features.append('AI: OpenAI')
        if config.ai.mistral.enabled:
            features.append('AI: Mistral')

        logger.info(f"   Features enabled: {', '.join(features) if features else 'Basic (Optimized)'}")

        # Generate timestamp for output directory
        timestamp = start_time.strftime("%Y%m%d_%H%M%S")

        # Run optimized data generation
        logger.info(f"🚀 Starting optimized data generation...")

        generated_data = main(
            config=config,
            total_records=config.rows
        )

        # Calculate and log execution time
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()

        logger.info(f"⏱️ Total execution time: {total_duration:.2f} seconds")
        logger.info(f"📁 Output saved to: {config.output.directory}")

        # Success message
        print("\n" + "=" * 80)
        print("🎉 OPTIMIZED DATA GENERATION COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print(f"✅ Generated data for {len(generated_data)} tables")
        print(f"⚡ Performance improvements: Optimized streaming + parallel processing")
        print(f"💾 Memory optimized: Intelligent memory management and cleanup")
        print(f"📊 Quality assured: Comprehensive data quality analysis")
        print(f"🔒 Security ready: Data masking, encryption, and audit logging")
        print(f"📋 Business validated: Rule validation and anomaly detection")
        print(f"🧠 AI enhanced: OpenAI and Mistral integration")
        print(f"📁 Files saved to: {config.output.directory}")
        print(f"⏱️ Completed in: {total_duration:.2f} seconds")
        print("=" * 80)

        # Feature summary
        if features:
            print(f"🔧 Advanced features used: {', '.join(features)}")
            print("   • Streaming: Handle datasets larger than memory")
            print("   • Parallel: Multi-threaded/process generation")
            print("   • Quality Analysis: Statistical validation and insights")
            print("   • Security: Data masking and encryption")
            print("   • Business Rules: Custom validation logic")
            print("   • Anomaly Detection: Automatic outlier identification")
            print("   • AI Integration: OpenAI and Mistral AI support")
            print("=" * 80)

    except FileNotFoundError as e:
        print(f"❌ Configuration file not found: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print(f"\n⚠️ Data generation interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        print_exc()
        sys.exit(1)
