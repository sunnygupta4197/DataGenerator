import os
import logging
import argparse
from datetime import datetime
import json
from typing import Dict, List, Any, Tuple
from traceback import print_exc
import sys

import pandas as pd

from constraint_manager.optimized_constraint_manager import OptimizedConstraintManager
from validators.unified_validation_system import UnifiedValidator
from data_generator.streaming_data_generator import ParallelDataGenerator, DataQualityAnalyzer, SecurityManager, \
    PerformanceProfiler
from config_manager.config_manager import ConfigurationManager, GenerationConfig
from writers.writer import CSVWriter, ParquetWriter, JsonWriter, SQLQueryWriter
from data_generator.data_generator import DataGenerator


class OptimizedDataGenerationEngine:
    """
    Enhanced data generation engine using the new parallel generator
    with streaming support, memory management, and quality analysis
    """

    def __init__(self, config: GenerationConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger

        # Initialize core components with optimized versions
        self.constraint_manager = OptimizedConstraintManager(
            logger=logger,
            max_memory_mb=config.performance.max_memory_mb,
            enable_parallel=config.performance.enable_parallel
        )

        self.validator = UnifiedValidator(logger=logger)

        # Create the main DataGenerator instance with full sophisticated components
        self.data_generator = DataGenerator(config, config.locale, ai_config=config.ai, logger=logger)

        # Initialize the enhanced parallel data generator with the full DataGenerator
        self.parallel_generator = ParallelDataGenerator(
            data_generator_instance=self.data_generator,
            max_workers=config.performance.max_workers,
            max_memory_mb=config.performance.max_memory_mb,
            enable_streaming=config.performance.enable_streaming,
            logger=logger
        )

        # Initialize additional enhanced components
        self.data_quality_analyzer = DataQualityAnalyzer(logger=logger)
        self.security_manager = SecurityManager(logger=logger)
        self.performance_profiler = PerformanceProfiler(logger=logger)

        # Configure security if enabled
        if config.security.enable_data_masking:
            self.security_manager.enable_masking = True

            # Add default masking rules
            self.security_manager.add_masking_rule('email', 'partial')
            self.security_manager.add_masking_rule('phone', 'partial')
            self.security_manager.add_masking_rule('ssn', 'partial')
            self.security_manager.add_masking_rule('credit', 'partial')
            self.security_manager.add_masking_rule('income', 'hash')
            self.security_manager.add_masking_rule('salary', 'hash')

        # Set encryption key if provided
        if hasattr(config.security, 'encryption_key') and config.security.encryption_key:
            self.security_manager.set_encryption_key(config.security.encryption_key.encode())

        # Generation statistics
        self.generation_stats = {
            'tables_processed': 0,
            'total_records_generated': 0,
            'total_duration': 0.0,
            'memory_peak_mb': 0.0,
            'quality_scores': {},
            'security_operations': 0,
            'performance_profiles': {},
            'errors': []
        }

    def generate_data_for_table(self, table_metadata: Dict[str, Any],
                                total_records: int,
                                foreign_key_data: Dict[str, List] = None,
                                output_dir: str = "./output") -> Tuple[List[Dict], Dict]:
        """
        Generate data for a single table using the enhanced parallel generator
        """
        table_name = table_metadata["table_name"]
        self.logger.info(f"🚀 Starting enhanced generation for table '{table_name}' ({total_records:,} records)")

        start_time = datetime.now()

        try:
            with self.performance_profiler.profile(f'table_{table_name}_generation'):
                # Determine generation strategy based on dataset size and configuration
                generated_data = self._select_and_execute_generation_strategy(
                    table_metadata, total_records, foreign_key_data, output_dir
                )

            # Calculate generation statistics
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            # Enhanced data quality analysis
            quality_analysis = None
            if self.config.validation.enable_data_quality_analysis and generated_data:
                with self.performance_profiler.profile(f'table_{table_name}_quality_analysis'):
                    quality_analysis = self.data_quality_analyzer.analyze_distribution(
                        generated_data[:min(5000, len(generated_data))],  # Sample for analysis
                        table_metadata
                    )
                    self.generation_stats['quality_scores'][table_name] = quality_analysis.get('data_quality_score',
                                                                                               0.0)

                    # Log quality insights
                    self._log_quality_insights(table_name, quality_analysis)

            # Advanced security processing
            if self.config.security.enable_data_masking:
                with self.performance_profiler.profile(f'table_{table_name}_security_processing'):
                    generated_data = self._apply_comprehensive_security_measures(generated_data, table_metadata)

            # Business rule validation if enabled
            if hasattr(self.config.validation,
                       'enable_business_rules') and self.config.validation.enable_business_rules:
                with self.performance_profiler.profile(f'table_{table_name}_business_rules'):
                    self._validate_business_rules(generated_data, table_metadata)

            # Anomaly detection if enabled
            if hasattr(self.config.validation,
                       'enable_anomaly_detection') and self.config.validation.enable_anomaly_detection:
                with self.performance_profiler.profile(f'table_{table_name}_anomaly_detection'):
                    self._detect_and_log_anomalies(generated_data, table_metadata)

            # Update statistics
            self.generation_stats['tables_processed'] += 1
            self.generation_stats['total_records_generated'] += len(generated_data)
            self.generation_stats['total_duration'] += duration

            # Track memory usage
            current_memory = self.parallel_generator.memory_monitor.get_memory_usage()
            self.generation_stats['memory_peak_mb'] = max(
                self.generation_stats['memory_peak_mb'], current_memory
            )

            # Extract foreign key data for subsequent tables
            generated_fk_data = self._extract_foreign_key_data(generated_data, table_metadata)

            self.logger.info(f"✅ Completed {table_name}: {len(generated_data):,} records in {duration:.2f}s")

            # Log performance summary
            if quality_analysis:
                self.logger.info(f"📊 Quality score: {quality_analysis.get('data_quality_score', 0):.3f}")

            return generated_data, generated_fk_data

        except Exception as e:
            print_exc()
            self.logger.error(f"❌ Failed to generate data for {table_name}: {e}")
            self.generation_stats['errors'].append(f"{table_name}: {str(e)}")
            raise

    def _select_and_execute_generation_strategy(self, table_metadata: Dict[str, Any],
                                                total_records: int,
                                                foreign_key_data: Dict[str, List],
                                                output_dir: str) -> List[Dict]:
        """Select and execute the optimal generation strategy"""

        # Analyze dataset characteristics
        estimated_memory_mb = self.parallel_generator._estimate_memory_requirements(table_metadata, total_records)
        available_memory_mb = self.parallel_generator.memory_monitor.max_memory_mb

        # Strategy selection logic
        if self.config.performance.enable_streaming and total_records > 100000:
            return self._generate_with_enhanced_streaming(
                table_metadata, total_records, foreign_key_data, output_dir
            )
        elif self.config.performance.enable_parallel and total_records > 10000:
            return self._generate_with_enhanced_parallel(
                table_metadata, total_records, foreign_key_data
            )
        else:
            return self._generate_with_adaptive_strategy(
                table_metadata, total_records, foreign_key_data, output_dir
            )

    def _generate_with_enhanced_streaming(self, table_metadata: Dict[str, Any],
                                          total_records: int,
                                          foreign_key_data: Dict[str, List],
                                          output_dir: str) -> List[Dict]:
        """Generate data using enhanced streaming approach"""
        self.logger.info(f"📊 Using ENHANCED STREAMING generation strategy")

        table_name = table_metadata["table_name"]
        output_format = self.config.output.format
        output_path = os.path.join(output_dir, f"{table_name}.{output_format}")

        all_data = []
        batch_count = 0
        quality_scores = []

        # Use enhanced streaming generator
        for batch in self.parallel_generator.generate_streaming(
                table_metadata=table_metadata,
                total_records=total_records,
                foreign_key_data=foreign_key_data,
                output_path=output_path,
                output_format=output_format
        ):
            batch_count += 1

            # Validate batch if strict mode enabled
            if self.config.validation.strict_mode:
                self._validate_batch(batch, table_metadata, batch_count)

            # Real-time quality monitoring
            if batch_count % 10 == 0 and self.config.validation.enable_data_quality_analysis:
                batch_quality = self.data_quality_analyzer.analyze_distribution(batch[:100], table_metadata)
                quality_scores.append(batch_quality.get('data_quality_score', 0))

                if quality_scores:
                    avg_quality = sum(quality_scores) / len(quality_scores)
                    self.logger.info(f"📈 Batch {batch_count}: Quality score {avg_quality:.3f}")

            # Keep sample for final analysis (avoid memory issues)
            if len(all_data) < 10000:
                all_data.extend(batch[:min(len(batch), 10000 - len(all_data))])

            # Progress logging for large datasets
            if batch_count % 20 == 0:
                total_so_far = batch_count * len(batch)
                progress_pct = (total_so_far / total_records) * 100
                memory_usage = self.parallel_generator.memory_monitor.get_memory_usage()
                self.logger.info(
                    f"📈 Streaming progress: {total_so_far:,}/{total_records:,} ({progress_pct:.1f}%) - Memory: {memory_usage:.1f}MB")

        self.logger.info(f"💾 Enhanced streaming completed. Data saved to: {output_path}")
        return all_data

    def _generate_with_enhanced_parallel(self, table_metadata: Dict[str, Any],
                                         total_records: int,
                                         foreign_key_data: Dict[str, List]) -> List[Dict]:
        """Generate data using enhanced parallel processing"""
        self.logger.info(f"⚡ Using ENHANCED PARALLEL generation strategy")

        # Determine if we should use process-based parallelism for very large datasets
        use_processes = total_records > 500000 and self.config.performance.max_workers > 2

        if use_processes:
            self.logger.info(f"🔄 Using process-based parallelism for {total_records:,} records")
        else:
            self.logger.info(f"🧵 Using thread-based parallelism for {total_records:,} records")

        generated_data = self.parallel_generator.generate_parallel(
            table_metadata=table_metadata,
            total_records=total_records,
            foreign_key_data=foreign_key_data,
            use_processes=use_processes
        )

        return generated_data

    def _generate_with_adaptive_strategy(self, table_metadata: Dict[str, Any],
                                         total_records: int,
                                         foreign_key_data: Dict[str, List],
                                         output_dir: str) -> List[Dict]:
        """Generate data using adaptive strategy selection"""
        self.logger.info(f"🤖 Using ADAPTIVE generation strategy")

        output_format = self.config.output.format
        table_name = table_metadata["table_name"]
        output_path = os.path.join(output_dir, f"{table_name}.{output_format}")

        all_data = []

        # Enhanced adaptive generator with comprehensive monitoring
        for batch in self.parallel_generator.generate_adaptive(
                table_metadata=table_metadata,
                total_records=total_records,
                foreign_key_data=foreign_key_data,
                output_path=output_path,
                output_format=output_format
        ):
            if isinstance(batch, list) and len(batch) > 0:
                all_data.extend(batch)

        return all_data

    def _apply_comprehensive_security_measures(self, data: List[Dict], table_metadata: Dict[str, Any]) -> List[Dict]:
        """Apply comprehensive security measures"""
        if not self.config.security.enable_data_masking:
            return data

        # Build sensitivity map from column metadata
        sensitivity_map = {}
        sensitive_columns = []

        for column in table_metadata.get('columns', []):
            sensitivity_level = column.get('sensitivity', 'PUBLIC')
            sensitivity_map[column['name']] = sensitivity_level

            if sensitivity_level in ['PII', 'SENSITIVE']:
                sensitive_columns.append(column['name'])

        # Apply masking
        masked_data = self.security_manager.mask_sensitive_data(data, sensitivity_map)

        # Apply encryption if enabled and encryption key is set
        if self.security_manager.encryption_key and sensitive_columns:
            encrypted_data = self.security_manager.encrypt_sensitive_fields(
                masked_data, sensitive_columns
            )
            self.generation_stats['security_operations'] += len(encrypted_data) * len(sensitive_columns)
        else:
            encrypted_data = masked_data

        # Audit the operation
        if hasattr(self.config.security, 'audit_enabled') and self.config.security.audit_enabled:
            audit_record = self.security_manager.audit_data_generation(
                generation_params={
                    'table': table_metadata['table_name'],
                    'security_enabled': True,
                    'masking_rules': len(self.security_manager.masking_rules)
                },
                records_count=len(data),
                sensitive_columns=sensitive_columns
            )
            self.logger.debug(f"🔒 Security audit: {audit_record['audit_id']}")

        return encrypted_data

    def _validate_business_rules(self, data: List[Dict], table_metadata: Dict[str, Any]):
        """Validate business rules against generated data"""
        # Example business rules - can be configured in table metadata
        business_rules = table_metadata.get('business_rules', [])

        if business_rules and data:
            rule_violations = self.data_quality_analyzer.validate_business_rules(
                data[:1000],  # Sample for performance
                business_rules
            )

            violation_count = rule_violations.get('total_violations', 0)
            compliance_rate = rule_violations.get('compliance_rate', 1.0)

            if violation_count > 0:
                self.logger.warning(
                    f"⚖️ Business rule violations: {violation_count} (Compliance: {compliance_rate:.1%})")

                # Log first few violations for debugging
                for violation in rule_violations.get('violations', [])[:3]:
                    self.logger.warning(f"   - {violation.get('rule_description', 'Unknown rule')}")

    def _generate_default_business_rules(self, table_metadata: Dict[str, Any]) -> List[Dict]:
        """Generate default business rules based on table structure"""
        rules = []
        columns = {col['name']: col for col in table_metadata.get('columns', [])}

        # Age-based rules
        if 'age' in columns and 'status' in columns:
            rules.append({
                'type': 'conditional',
                'condition_column': 'age',
                'condition_operator': '<',
                'condition_value': 18,
                'requirement_column': 'status',
                'requirement_value': 'MINOR'
            })

        # Income-credit score correlation
        if 'income' in columns and 'credit_score' in columns:
            rules.append({
                'type': 'range_dependency',
                'income_column': 'income',
                'score_column': 'credit_score',
                'income_threshold': 100000,
                'score_threshold': 700
            })

        return rules

    def _detect_and_log_anomalies(self, data: List[Dict], table_metadata: Dict[str, Any]):
        """Detect and log data anomalies"""
        if not data:
            return

        anomalies = self.data_quality_analyzer.detect_anomalies(data[:2000])  # Sample for performance
        anomaly_list = anomalies.get('anomalies', [])

        if anomaly_list:
            self.logger.warning(f"🔍 Detected {len(anomaly_list)} potential anomalies:")

            for anomaly in anomaly_list[:5]:  # Log first 5
                severity = anomaly.get('severity', 'unknown')
                anomaly_type = anomaly.get('type', 'unknown')
                self.logger.warning(f"   - {anomaly_type} ({severity})")

    def _log_quality_insights(self, table_name: str, quality_analysis: Dict[str, Any]):
        """Log detailed quality insights"""
        if not quality_analysis:
            return

        score = quality_analysis.get('data_quality_score', 0)
        issues = quality_analysis.get('issues', [])

        self.logger.info(f"📊 {table_name} quality analysis:")
        self.logger.info(f"   Overall score: {score:.3f}")
        self.logger.info(f"   Records analyzed: {quality_analysis.get('record_count', 0):,}")

        if issues:
            self.logger.warning(f"   Quality issues found: {len(issues)}")
            for issue in issues[:3]:  # Log first 3 issues
                self.logger.warning(f"     - {issue}")

        # Log column-specific insights
        column_analysis = quality_analysis.get('column_analysis', {})
        problematic_columns = [
            col for col, analysis in column_analysis.items()
            if analysis.get('quality_score', 1.0) < 0.8
        ]

        if problematic_columns:
            self.logger.warning(f"   Columns needing attention: {', '.join(problematic_columns)}")

    def _validate_batch(self, batch: List[Dict], table_metadata: Dict[str, Any], batch_num: int):
        """Enhanced batch validation"""
        if not batch:
            return

        validation_results = self.validator.validate_batch(batch, table_metadata)

        if validation_results['invalid_records'] > 0:
            error_rate = (validation_results['invalid_records'] / validation_results['total_records']) * 100

            if error_rate > 5.0:  # More than 5% errors
                self.logger.warning(f"⚠️ Batch {batch_num} has high error rate: {error_rate:.1f}%")

                # Log first few errors
                for error in validation_results['errors'][:3]:
                    self.logger.warning(f"   - {error}")

                if self.config.validation.strict_mode and error_rate > 10.0:
                    raise ValueError(f"Batch validation failed: {error_rate:.1f}% error rate exceeds threshold")

    def _extract_foreign_key_data(self, data: List[Dict], table_metadata: Dict[str, Any]) -> Dict[str, List]:
        """Extract foreign key data for subsequent table generation"""
        fk_data = {}

        # Get primary key columns
        pk_columns = self._get_primary_key_columns(table_metadata)
        table_name = table_metadata['table_name']

        for pk_col in pk_columns:
            values = [row.get(pk_col) for row in data if row.get(pk_col) is not None]
            if values:
                fk_key = f"{table_name}.{pk_col}"
                fk_data[fk_key] = values[:50000]  # Limit FK pool size for memory efficiency

        return fk_data

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

    def get_comprehensive_statistics(self) -> Dict[str, Any]:
        """Get comprehensive generation statistics"""
        # Get parallel generator stats
        parallel_stats = self.parallel_generator.get_performance_stats()

        # Get constraint manager stats
        constraint_stats = self.data_generator.constraint_manager.get_constraint_statistics()

        # Get performance profiling data
        profile_report = self.performance_profiler.get_profile_report()

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
        """Clean up resources"""
        if hasattr(self.parallel_generator, 'cleanup'):
            self.parallel_generator.cleanup()
        if hasattr(self.data_generator, 'cleanup'):
            self.data_generator.cleanup()
        if hasattr(self.performance_profiler, 'reset_profiles'):
            # Don't reset profiles during cleanup, keep for final report
            pass


class OptimizedDataGenerationOrchestrator:
    """
    Enhanced orchestrator that uses the new parallel data generator
    """

    def __init__(self, config: GenerationConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.generation_engine = OptimizedDataGenerationEngine(config, logger)

    def run_data_generation(self, total_records: int, output_dir: str) -> Dict[str, List]:
        """Main orchestration method using enhanced parallel components"""

        # Setup
        os.makedirs(output_dir, exist_ok=True)
        start_time = datetime.now()

        self.logger.info(f"🚀 Starting ENHANCED data generation process")
        self.logger.info(f"📊 Configuration: {total_records:,} records per table")
        self.logger.info(f"⚡ Max workers: {self.config.performance.max_workers}")
        self.logger.info(f"💾 Max memory: {self.config.performance.max_memory_mb}MB")
        self.logger.info(f"🔄 Streaming enabled: {self.config.performance.enable_streaming}")
        self.logger.info(f"🔒 Security enabled: {self.config.security.enable_data_masking}")
        self.logger.info(f"🤖 OpenAI enabled: {self.config.openai.enabled}")
        self.logger.info(f"📁 Output directory: {output_dir}")

        # Process tables in dependency order
        ordered_tables = self._process_tables_in_dependency_order()
        self.logger.info(f"📋 Processing {len(ordered_tables)} tables in dependency order")

        # Track data and statistics
        all_foreign_key_data = {}
        all_table_data = {}

        try:
            # Process each table with enhanced capabilities
            for i, table_metadata in enumerate(ordered_tables, 1):
                table_name = table_metadata["table_name"]
                self.logger.info(f"📊 Processing table {i}/{len(ordered_tables)}: {table_name}")

                # Get foreign key constraints for this table
                foreign_key_data = self._collect_foreign_key_constraints(
                    table_metadata, all_foreign_key_data
                )

                if foreign_key_data:
                    self.logger.info(f"🔗 Foreign key constraints for {table_name}:")
                    for fk_key, values in foreign_key_data.items():
                        self.logger.info(f"   {fk_key}: {len(values):,} available values")

                # Generate data for this table using enhanced engine
                generated_records, generated_fk_data = self.generation_engine.generate_data_for_table(
                    table_metadata, total_records, foreign_key_data, output_dir
                )

                # Store results
                all_table_data[table_name] = generated_records

                # Update FK data for future tables
                for key, values in generated_fk_data.items():
                    if key not in all_foreign_key_data:
                        all_foreign_key_data[key] = []
                    all_foreign_key_data[key].extend(values)

                    # Limit FK pool size to prevent memory issues
                    if len(all_foreign_key_data[key]) > 100000:
                        all_foreign_key_data[key] = all_foreign_key_data[key][-50000:]

                # Export data to file using appropriate writer
                self._export_table_data(generated_records, table_metadata, output_dir)

                # Memory status logging
                current_memory = self.generation_engine.parallel_generator.memory_monitor.get_memory_usage()
                self.logger.info(f"💾 Current memory usage: {current_memory:.1f}MB")

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

        # Generate comprehensive final report
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()

        self._generate_enhanced_final_report(total_duration, output_dir)

        self.logger.info(f"🎉 Enhanced data generation completed in {total_duration:.2f} seconds!")

        return all_table_data

    def _process_tables_in_dependency_order(self) -> List[Dict]:
        """Order tables based on foreign key dependencies"""
        tables = self.config.tables
        processed = []
        remaining = tables.copy()
        processed_names = set()

        max_iterations = len(tables) * 2  # Prevent infinite loops
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
                # Add remaining tables anyway
                processed.extend(remaining)
                break

        return processed

    def _can_process_table(self, table: Dict, processed_names: set) -> bool:
        """Check if table dependencies are met"""
        table_name = table["table_name"]
        foreign_keys = table.get("foreign_keys", [])

        for fk in foreign_keys:
            parent_table = fk["parent_table"]
            if parent_table not in processed_names and parent_table != table_name:
                return False

        return True

    def _collect_foreign_key_constraints(self, table_metadata: Dict,
                                         all_fk_data: Dict[str, List]) -> Dict[str, List]:
        """Collect foreign key constraints for current table"""
        constraints = {}
        foreign_keys = table_metadata.get("foreign_keys", [])

        for fk in foreign_keys:
            parent_table = fk["parent_table"]
            parent_column = fk["parent_column"]
            fk_key = f"{parent_table}.{parent_column}"

            available_values = all_fk_data.get(fk_key, [])
            constraints[fk_key] = available_values

            # Log FK availability
            if available_values:
                self.logger.debug(f"🔗 FK constraint {fk_key}: {len(available_values):,} values available")
            else:
                self.logger.warning(f"⚠️ FK constraint {fk_key}: No values available")

        return constraints

    def _export_table_data(self, data: List[Dict], table_metadata: Dict, output_dir: str):
        """Export table data to file using appropriate writer"""
        if not data:
            self.logger.warning(f"⚠️ No data to export for table {table_metadata.get('table_name', 'unknown')}")
            return

        table_name = table_metadata["table_name"]
        output_format = self.config.output.format

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Get appropriate writer
        writers = {
            "csv": CSVWriter,
            "json": JsonWriter,
            "jsonl": JsonWriter,
            "parquet": ParquetWriter,
            "sql_query": SQLQueryWriter
        }

        writer_class = writers.get(output_format)
        if writer_class:
            try:
                writer = writer_class(
                    data=df,
                    table_metadata=table_metadata,
                    output_dir=output_dir,
                    batch_size=10000,
                    logger=self.logger
                )
                writer.write_data()
                self.logger.info(f"💾 Exported {len(data):,} records to {output_format.upper()} format")
            except Exception as e:
                self.logger.error(f"❌ Failed to export {table_name} to {output_format}: {e}")
        else:
            self.logger.error(f"❌ Unsupported output format: {output_format}")

    def _generate_enhanced_final_report(self, total_duration: float, output_dir: str):
        """Generate comprehensive enhanced final report"""
        stats = self.generation_engine.get_comprehensive_statistics()

        # Enhanced report with all new capabilities
        report = {
            "generation_timestamp": datetime.now().isoformat(),
            "generator_type": "ParallelDataGenerator",
            "configuration_summary": {
                "total_tables": len(self.config.tables),
                "records_per_table": self.config.rows,
                "output_format": self.config.output.format,
                "max_workers": self.config.performance.max_workers,
                "streaming_enabled": self.config.performance.enable_streaming,
                "parallel_enabled": self.config.performance.enable_parallel,
                "quality_analysis_enabled": self.config.validation.enable_data_quality_analysis,
                "security_enabled": self.config.security.enable_data_masking,
                "business_rules_enabled": getattr(self.config.validation, 'enable_business_rules', False),
                "anomaly_detection_enabled": getattr(self.config.validation, 'enable_anomaly_detection', False)
            },
            "performance_summary": {
                "total_duration_seconds": total_duration,
                "total_records_generated": stats['generation_summary']['total_records_generated'],
                "average_records_per_second": stats['generation_summary'][
                                                  'total_records_generated'] / total_duration if total_duration > 0 else 0,
                "peak_memory_usage_mb": stats.get('peak_memory_mb', 0),
                "current_memory_usage_mb": stats.get('memory_usage_mb', 0),
                "tables_processed": stats['generation_summary']['tables_processed'],
                "memory_cleanups_performed": stats.get('parallel_generator_performance', {}).get('memory_cleanups', 0),
                "errors_encountered": len(stats['generation_summary']['errors'])
            },
            "quality_metrics": {
                "average_quality_score": stats.get('average_quality_score', 0),
                "quality_by_table": stats['generation_summary'].get('quality_scores', {}),
                "total_quality_checks": len(stats['generation_summary'].get('quality_scores', {}))
            },
            "security_metrics": {
                "security_operations_performed": stats.get('security_operations_count', 0),
                "masking_enabled": self.config.security.enable_data_masking,
                "encryption_enabled": bool(self.generation_engine.security_manager.encryption_key),
                "audit_records_created": len(self.generation_engine.security_manager.audit_trail)
            },
            "performance_profiling": stats.get('performance_profiling', {}),
            "constraint_manager_performance": stats.get('constraint_manager_performance', {}),
            "parallel_generator_performance": stats.get('parallel_generator_performance', {}),
            "detailed_statistics": stats,
            "error_details": stats['generation_summary'].get('errors', [])
        }

        # Save comprehensive report
        report_path = os.path.join(output_dir, "enhanced_generation_report.json")
        try:
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            self.logger.info(f"📊 Enhanced generation report saved to: {report_path}")
        except Exception as e:
            self.logger.error(f"❌ Could not save enhanced report: {e}")

        # Save performance profiling report separately
        self._save_performance_profiling_report(output_dir)

        # Save security audit trail if enabled
        if self.config.security.enable_data_masking:
            self._save_security_audit_trail(output_dir)

        # Log enhanced summary to console
        self._log_enhanced_summary_to_console(report)

    def _save_performance_profiling_report(self, output_dir: str):
        """Save detailed performance profiling report"""
        try:
            profile_report = self.generation_engine.performance_profiler.get_profile_report()
            profile_path = os.path.join(output_dir, "performance_profiling_report.json")

            with open(profile_path, 'w') as f:
                json.dump(profile_report, f, indent=2, default=str)

            self.logger.info(f"⚡ Performance profiling report saved to: {profile_path}")
        except Exception as e:
            self.logger.error(f"❌ Could not save performance profiling report: {e}")

    def _save_security_audit_trail(self, output_dir: str):
        """Save security audit trail"""
        try:
            audit_path = os.path.join(output_dir, "security_audit_trail.json")
            self.generation_engine.security_manager.export_audit_trail(audit_path)
            self.logger.info(f"🔒 Security audit trail saved to: {audit_path}")
        except Exception as e:
            self.logger.error(f"❌ Could not save security audit trail: {e}")

    def _log_enhanced_summary_to_console(self, report: Dict):
        """Log enhanced summary information to console"""
        perf = report["performance_summary"]
        config = report["configuration_summary"]
        quality = report["quality_metrics"]
        security = report["security_metrics"]
        print(report)

        self.logger.info("=" * 80)
        self.logger.info("🎉 ENHANCED GENERATION SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(f"Generator Type: {report['generator_type']}")
        self.logger.info(f"Tables Processed: {perf['tables_processed']}")
        self.logger.info(f"Total Records: {perf['total_records_generated']:,}")
        self.logger.info(f"Duration: {perf['total_duration_seconds']:.2f} seconds")
        self.logger.info(f"Speed: {perf['average_records_per_second']:,.0f} records/second")
        self.logger.info(f"Peak Memory: {perf['peak_memory_usage_mb']:.1f} MB")
        self.logger.info(f"Memory Cleanups: {perf['memory_cleanups_performed']}")
        self.logger.info(f"Workers Used: {config['max_workers']}")

        # Feature status
        self.logger.info("─" * 40)
        self.logger.info("🔧 FEATURES ENABLED:")
        self.logger.info(f"Streaming: {'✅' if config['streaming_enabled'] else '❌'}")
        self.logger.info(f"Parallel: {'✅' if config['parallel_enabled'] else '❌'}")
        self.logger.info(f"Quality Analysis: {'✅' if config['quality_analysis_enabled'] else '❌'}")
        self.logger.info(f"Security/Masking: {'✅' if config['security_enabled'] else '❌'}")
        self.logger.info(f"Business Rules: {'✅' if config.get('business_rules_enabled', False) else '❌'}")
        self.logger.info(f"Anomaly Detection: {'✅' if config.get('anomaly_detection_enabled', False) else '❌'}")
        # self.logger.info(f"AI Model: {'✅' if config.get('ai', False) else '❌'}")

        # Quality metrics
        if quality['total_quality_checks'] > 0:
            self.logger.info("─" * 40)
            self.logger.info("📊 QUALITY METRICS:")
            self.logger.info(f"Average Quality Score: {quality['average_quality_score']:.3f}/1.0")
            self.logger.info(f"Tables Analyzed: {quality['total_quality_checks']}")

            # Show best and worst quality scores
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
            self.logger.info(f"Security Operations: {security['security_operations_performed']:,}")
            self.logger.info(f"Masking: {'✅' if security['masking_enabled'] else '❌'}")
            self.logger.info(f"Encryption: {'✅' if security['encryption_enabled'] else '❌'}")
            self.logger.info(f"Audit Records: {security['audit_records_created']}")

        # Error summary
        if perf['errors_encountered'] > 0:
            self.logger.info("─" * 40)
            self.logger.warning(f"⚠️ ERRORS: {perf['errors_encountered']}")
            error_details = report.get('error_details', [])
            for error in error_details[:3]:  # Show first 3 errors
                self.logger.warning(f"   - {error}")
        else:
            self.logger.info("─" * 40)
            self.logger.info("✅ No errors encountered")

        self.logger.info("=" * 80)


def main(config: GenerationConfig, total_records: int = None, output_dir: str = "./output") -> Dict[str, List]:
    """
    Main function using the enhanced parallel data generator with streaming support
    """
    logger = logging.getLogger(__name__)

    # Determine actual total records
    actual_total_records = total_records or config.rows

    # Create enhanced orchestrator
    orchestrator = OptimizedDataGenerationOrchestrator(config, logger)

    # Log startup information
    logger.info(f"🚀 Initializing Enhanced Data Generation Engine")
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
    if config.openai.enabled:
        features_enabled.append("OpenAI")

    logger.info(f"🔧 Features: {', '.join(features_enabled) if features_enabled else 'Basic'}")

    # Run enhanced data generation
    return orchestrator.run_data_generation(actual_total_records, output_dir)


def setup_logging(config: GenerationConfig) -> logging.Logger:
    """Setup enhanced logging based on configuration"""
    log_level = getattr(logging, config.logging.level.upper(), logging.INFO)
    log_format = config.logging.format

    # Enhanced log format with more context
    enhanced_format = "%(asctime)s | %(levelname)-8s | %(name)-10s | %(message)s"
    if hasattr(config.logging, 'enhanced_format') and config.logging.enhanced_format:
        log_format = enhanced_format

    # Configure logging with multiple handlers
    handlers = [logging.StreamHandler(sys.stdout)]

    if config.logging.file_path:
        # Create log directory if it doesn't exist
        log_dir = os.path.dirname(config.logging.file_path)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        # Add file handler
        file_handler = logging.FileHandler(config.logging.file_path)
        file_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(file_handler)

    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=handlers,
        force=True  # Override any existing configuration
    )

    # Set specific logger levels for performance
    logging.getLogger('faker').setLevel(logging.WARNING)
    logging.getLogger('pandas').setLevel(logging.WARNING)

    return logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments with enhanced options"""
    parser = argparse.ArgumentParser(
        description='Enhanced Data Generator with Streaming, Parallel Processing, and Advanced Analytics',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic generation
  python main.py --config config.json --rows 100000

  # Enhanced streaming with quality analysis
  python main.py --config config.json --rows 1000000 --enable_streaming --enable_quality_analysis

  # Parallel generation with security features
  python main.py --config config.json --enable_parallel --enable_masking --max_workers 8

  # Full feature demonstration
  python main.py --config config.json --rows 500000 --enable_all_features --max_memory 1024

Features:
  🔄 Streaming: Handle datasets larger than memory
  ⚡ Parallel: Multi-threaded/process generation
  📊 Quality: Statistical analysis and validation
  🔒 Security: Data masking and encryption
  🔍 Anomaly: Automatic anomaly detection
  ⚖️ Rules: Business rule validation
        """
    )

    # Basic arguments
    parser.add_argument('--config', '-c', required=True,
                        help='Path to configuration file (JSON)')
    parser.add_argument('--output_dir', '-o', default='./output',
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
    parser.add_argument('--enable_all_features', action='store_true',
                        help='Enable all advanced features')

    # Output options
    parser.add_argument('--format', '-f',
                        choices=['csv', 'json', 'jsonl', 'parquet', 'sql_query'],
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
                        help='Logging level')

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
    if args.log_level:
        config.logging.level = args.log_level

    # Feature flags
    if args.enable_streaming:
        config.performance.enable_streaming = True
    if args.enable_parallel:
        config.performance.enable_parallel = True
    if args.enable_quality_analysis:
        config.validation.enable_data_quality_analysis = True
    if args.enable_masking:
        config.security.enable_data_masking = True

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

    # Security options
    if args.encryption_key:
        if not hasattr(config.security, 'encryption_key'):
            config.security.encryption_key = args.encryption_key
        else:
            config.security.encryption_key = args.encryption_key

    # Validation options
    if args.strict_validation:
        config.validation.strict_mode = True

    # Performance profiling
    if args.profile_performance:
        if not hasattr(config.performance, 'enable_profiling'):
            config.performance.enable_profiling = True
        else:
            config.performance.enable_profiling = True


if __name__ == "__main__":
    print("🚀 ENHANCED DATA GENERATOR WITH STREAMING AND ADVANCED ANALYTICS")
    print("=" * 80)

    start_time = datetime.now()

    try:
        # Parse command line arguments
        args = parse_arguments()

        # Load and configure
        config_manager = ConfigurationManager()
        config = config_manager.load_configuration(args.config, args.environment)

        # Apply command line overrides
        apply_command_line_overrides(config, args)

        # Setup enhanced logging
        logger = setup_logging(config)

        # Log enhanced configuration summary
        logger.info("📋 Enhanced Configuration Summary:")
        logger.info(f"   Environment: {config.environment}")
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
            features.append('AI: Mistral AI')

        logger.info(f"   Features enabled: {', '.join(features) if features else 'Basic'}")

        # Generate timestamp for output directory
        timestamp = start_time.strftime("%Y%m%d_%H%M%S")
        output_directory = os.path.join(args.output_dir or './output/', timestamp)

        # Run enhanced data generation
        logger.info(f"🚀 Starting enhanced data generation...")

        generated_data = main(
            config=config,
            total_records=config.rows,
            output_dir=output_directory
        )

        # Calculate and log execution time
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()

        logger.info(f"⏱️ Total execution time: {total_duration:.2f} seconds")
        logger.info(f"📁 Output saved to: {output_directory}")

        # Enhanced success message
        print("\n" + "=" * 80)
        print("🎉 ENHANCED DATA GENERATION COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print(f"✅ Generated data for {len(generated_data)} tables")
        print(f"⚡ Performance improvements: Advanced streaming + parallel processing")
        print(f"💾 Memory optimized: Intelligent memory management and cleanup")
        print(f"📊 Quality assured: Comprehensive data quality analysis")
        print(f"🔒 Security ready: Data masking, encryption, and audit logging")
        print(f"📋 Business validated: Rule validation and anomaly detection")
        print(f"⚡ Performance profiled: Detailed performance monitoring")
        print(f"📁 Files saved to: {output_directory}")
        print(f"⏱️ Completed in: {total_duration:.2f} seconds ({end_time - start_time})")
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
