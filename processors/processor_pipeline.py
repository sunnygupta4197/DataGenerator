from typing import List, Tuple, Dict, Any
from .quality_processor import QualityAnalysisProcessor
from .security_processor import SecurityProcessor
from .business_rules_processor import BusinessRulesProcessor
from .anomaly_processor import AnomalyDetectionProcessor
from .validation_processor import ValidationProcessor


class ProcessorPipeline:
    """Enhanced pipeline for processing data through multiple processors"""

    def __init__(self, config, logger, quality_analyzer, security_manager,
                 business_rules_engine, anomaly_detector, validator):
        self.config = config
        self.logger = logger

        # Initialize all processors
        self.processors = [
            ValidationProcessor(config, logger, validator),
            QualityAnalysisProcessor(config, logger, quality_analyzer),
            AnomalyDetectionProcessor(config, logger, anomaly_detector),
            BusinessRulesProcessor(config, logger, business_rules_engine),
            SecurityProcessor(config, logger, security_manager),
        ]

        # Filter enabled processors
        self.enabled_processors = [p for p in self.processors if p.enabled]

        if self.enabled_processors:
            processor_names = [p.get_processor_name() for p in self.enabled_processors]
            self.logger.info(f"Enabled processors: {', '.join(processor_names)}")
        else:
            self.logger.warning("No processors are enabled in the pipeline")

        # Initialize processor metrics tracking
        self._processor_metrics = {}
        self._total_batches_processed = 0

    def process_batch(self, batch: List[Dict], table_metadata: Dict[str, Any],
                      batch_count: int) -> Tuple[List[Dict], Dict[str, Any]]:
        """Process batch through all enabled processors with enhanced error handling"""
        if not batch:
            return batch, {}

        current_batch = batch
        combined_metrics = {
            'pipeline_start_time': self._get_current_timestamp(),
            'original_batch_size': len(batch),
            'processors_executed': [],
            'processors_failed': [],
            'total_processing_time': 0
        }

        self._total_batches_processed += 1

        for processor in self.enabled_processors:
            if not processor.enabled or (processor.batch_interval != -1 and batch_count % processor.batch_interval != 0):
                continue

            processor_name = processor.get_processor_name()
            start_time = self._get_current_timestamp()

            try:
                # Process batch through current processor
                current_batch, processor_metrics = processor.process(
                    current_batch, table_metadata, batch_count
                )

                # Track successful execution
                combined_metrics['processors_executed'].append(processor_name)

                # Merge processor metrics with namespace
                for key, value in processor_metrics.items():
                    namespaced_key = f"{processor_name.lower().replace(' ', '_')}_{key}"
                    combined_metrics[namespaced_key] = value

                # Track processor performance
                execution_time = self._get_current_timestamp() - start_time
                self._update_processor_metrics(processor_name, execution_time, True)

                # Log processor execution
                self.logger.debug(
                    f"Processor '{processor_name}' completed for batch {batch_count} "
                    f"in {execution_time:.3f}s"
                )

            except Exception as e:
                # Track failed execution
                combined_metrics['processors_failed'].append(processor_name)
                execution_time = self._get_current_timestamp() - start_time
                self._update_processor_metrics(processor_name, execution_time, False)

                self.logger.error(
                    f"Processor '{processor_name}' failed for batch {batch_count}: {e}"
                )

                # Add error metrics
                error_key = f"{processor_name.lower().replace(' ', '_')}_error"
                combined_metrics[error_key] = str(e)

        # Add final pipeline metrics
        combined_metrics['final_batch_size'] = len(current_batch)
        combined_metrics['pipeline_end_time'] = self._get_current_timestamp()
        combined_metrics['total_processing_time'] = (
                combined_metrics['pipeline_end_time'] - combined_metrics['pipeline_start_time']
        )

        # Calculate batch reduction if any
        if combined_metrics['original_batch_size'] != combined_metrics['final_batch_size']:
            reduction_pct = (
                    (combined_metrics['original_batch_size'] - combined_metrics['final_batch_size']) /
                    combined_metrics['original_batch_size'] * 100
            )
            combined_metrics['batch_size_reduction_percent'] = reduction_pct

            self.logger.info(
                f"Batch {batch_count} size changed: "
                f"{combined_metrics['original_batch_size']} → {combined_metrics['final_batch_size']} "
                f"({reduction_pct:.1f}% reduction)"
            )

        return current_batch, combined_metrics

    def _get_current_timestamp(self) -> float:
        """Get current timestamp for performance tracking"""
        import time
        return time.time()

    def _update_processor_metrics(self, processor_name: str, execution_time: float, success: bool):
        """Update processor performance metrics"""
        if processor_name not in self._processor_metrics:
            self._processor_metrics[processor_name] = {
                'total_executions': 0,
                'successful_executions': 0,
                'failed_executions': 0,
                'total_execution_time': 0.0,
                'average_execution_time': 0.0,
                'success_rate': 0.0
            }

        metrics = self._processor_metrics[processor_name]
        metrics['total_executions'] += 1
        metrics['total_execution_time'] += execution_time

        if success:
            metrics['successful_executions'] += 1
        else:
            metrics['failed_executions'] += 1

        # Update calculated metrics
        metrics['average_execution_time'] = (
                metrics['total_execution_time'] / metrics['total_executions']
        )
        metrics['success_rate'] = (
                metrics['successful_executions'] / metrics['total_executions']
        )

    def get_pipeline_summary(self) -> Dict[str, Any]:
        """Get comprehensive pipeline performance summary"""
        return {
            'total_batches_processed': self._total_batches_processed,
            'enabled_processors': [p.get_processor_name() for p in self.enabled_processors],
            'disabled_processors': [p.get_processor_name() for p in self.processors if not p.enabled],
            'processor_metrics': self._processor_metrics.copy(),
            'pipeline_health': self._calculate_pipeline_health()
        }

    def _calculate_pipeline_health(self) -> Dict[str, Any]:
        """Calculate overall pipeline health metrics"""
        if not self._processor_metrics:
            return {'status': 'no_data', 'score': 0}

        total_success_rate = sum(
            metrics['success_rate'] for metrics in self._processor_metrics.values()
        ) / len(self._processor_metrics)

        avg_execution_time = sum(
            metrics['average_execution_time'] for metrics in self._processor_metrics.values()
        ) / len(self._processor_metrics)

        # Simple health scoring
        health_score = total_success_rate * 100

        if health_score >= 95:
            status = 'excellent'
        elif health_score >= 90:
            status = 'good'
        elif health_score >= 80:
            status = 'fair'
        else:
            status = 'poor'

        return {
            'status': status,
            'score': health_score,
            'average_success_rate': total_success_rate,
            'average_execution_time': avg_execution_time
        }

    def get_processor_by_name(self, processor_name: str):
        """Get processor instance by name"""
        for processor in self.processors:
            if processor.get_processor_name() == processor_name:
                return processor
        return None

    def disable_processor(self, processor_name: str) -> bool:
        """Temporarily disable a processor"""
        processor = self.get_processor_by_name(processor_name)
        if processor and processor in self.enabled_processors:
            self.enabled_processors.remove(processor)
            self.logger.info(f"Disabled processor: {processor_name}")
            return True
        return False

    def enable_processor(self, processor_name: str) -> bool:
        """Re-enable a disabled processor"""
        processor = self.get_processor_by_name(processor_name)
        if processor and processor not in self.enabled_processors and processor.enabled:
            self.enabled_processors.append(processor)
            self.logger.info(f"Enabled processor: {processor_name}")
            return True
        return False

    def reset_processor_metrics(self):
        """Reset all processor performance metrics"""
        self._processor_metrics.clear()
        self._total_batches_processed = 0
        self.logger.info("Reset all processor metrics")

    def get_detailed_processor_stats(self, processor_name: str = None) -> Dict[str, Any]:
        """Get detailed statistics for a specific processor or all processors"""
        if processor_name:
            processor = self.get_processor_by_name(processor_name)
            if not processor:
                return {'error': f'Processor {processor_name} not found'}

            stats = {
                'processor_name': processor_name,
                'enabled': processor in self.enabled_processors,
                'config_enabled': processor.enabled,
                'performance_metrics': self._processor_metrics.get(processor_name, {})
            }

            # Add processor-specific stats if available
            if hasattr(processor, 'get_validation_summary'):
                stats['validation_summary'] = processor.get_validation_summary()
            elif hasattr(processor, 'get_anomaly_summary'):
                stats['anomaly_summary'] = processor.get_anomaly_summary()
            elif hasattr(processor, 'export_audit_trail'):
                stats['security_capabilities'] = ['masking', 'encryption', 'auditing']

            return stats
        else:
            # Return stats for all processors
            all_stats = {}
            for processor in self.processors:
                proc_name = processor.get_processor_name()
                all_stats[proc_name] = self.get_detailed_processor_stats(proc_name)
            return all_stats

    def validate_pipeline_configuration(self) -> Dict[str, Any]:
        """Validate the current pipeline configuration"""
        validation_results = {
            'valid': True,
            'warnings': [],
            'errors': [],
            'recommendations': []
        }

        # Check if any processors are enabled
        if not self.enabled_processors:
            validation_results['valid'] = False
            validation_results['errors'].append("No processors are enabled")

        # Check for common configuration issues
        processor_names = [p.get_processor_name() for p in self.enabled_processors]

        # Recommend validation processor if not enabled
        if 'Data Validation' not in processor_names:
            validation_results['warnings'].append(
                "Data Validation processor is not enabled - data quality issues may go undetected"
            )

        # Check if security is enabled without validation
        if 'Security/Masking/Encryption' in processor_names and 'Data Validation' not in processor_names:
            validation_results['recommendations'].append(
                "Enable Data Validation processor when using Security processor for better data integrity"
            )

        # Check processor ordering
        validation_idx = next((i for i, p in enumerate(self.enabled_processors)
                               if p.get_processor_name() == 'Data Validation'), -1)
        security_idx = next((i for i, p in enumerate(self.enabled_processors)
                             if p.get_processor_name() == 'Security/Masking/Encryption'), -1)

        if validation_idx != -1 and security_idx != -1 and validation_idx > security_idx:
            validation_results['warnings'].append(
                "Data Validation should run before Security processing for optimal results"
            )

        # Check for performance concerns
        if len(self.enabled_processors) > 4:
            validation_results['recommendations'].append(
                "Consider processor execution frequency settings to optimize performance with many processors"
            )

        return validation_results

    def optimize_processor_execution(self) -> Dict[str, Any]:
        """Suggest optimizations based on processor performance metrics"""
        optimizations = {
            'suggestions': [],
            'performance_issues': [],
            'configuration_recommendations': []
        }

        for processor_name, metrics in self._processor_metrics.items():
            # Check for slow processors
            if metrics['average_execution_time'] > 1.0:
                optimizations['performance_issues'].append(
                    f"{processor_name} has high average execution time: {metrics['average_execution_time']:.3f}s"
                )
                optimizations['suggestions'].append(
                    f"Consider reducing sample size or execution frequency for {processor_name}"
                )

            # Check for failing processors
            if metrics['success_rate'] < 0.9:
                optimizations['performance_issues'].append(
                    f"{processor_name} has low success rate: {metrics['success_rate']:.1%}"
                )
                optimizations['suggestions'].append(
                    f"Review configuration and error logs for {processor_name}"
                )

            # Check for underutilized processors
            if metrics['total_executions'] < self._total_batches_processed / 10:
                optimizations['configuration_recommendations'].append(
                    f"{processor_name} executes infrequently - consider adjusting execution frequency"
                )

        return optimizations