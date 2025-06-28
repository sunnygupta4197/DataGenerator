from typing import List, Dict, Any, Tuple
from .base_processor import BaseDataProcessor


class ValidationProcessor(BaseDataProcessor):
    """Processor for comprehensive data validation using the validation system"""

    def __init__(self, config, logger, validator):
        super().__init__(config, logger)
        self.validator = validator
        self._validation_cache = {}
        self._error_patterns = {}

    def set_batch_interval(self):
        return -1

    def _is_enabled(self) -> bool:
        return getattr(self.config.validation, 'enable_data_validation', True)

    def get_processor_name(self) -> str:
        return "Data Validation"

    def process(self, batch: List[Dict], table_metadata: Dict[str, Any],
                batch_count: int) -> Tuple[List[Dict], Dict[str, Any]]:
        """Validate data batch against table metadata constraints"""
        metrics = {
            'total_records_validated': 0,
            'valid_records': 0,
            'invalid_records': 0,
            'validation_errors': 0,
            'type_validation_errors': 0,
            'constraint_validation_errors': 0,
            'null_constraint_violations': 0,
            'length_constraint_violations': 0,
            'rule_validation_errors': 0,
            'validation_success_rate': 0.0,
            'most_common_error_type': None
        }

        try:
            # Get table name for caching validation results
            table_name = table_metadata.get('table_name', 'unknown')

            # Determine validation frequency based on config and batch number
            validation_frequency = self._get_validation_frequency(table_name)

            if batch_count % validation_frequency != 0:
                return batch, metrics

            # Sample batch for validation if too large
            validation_sample = self._get_validation_sample(batch, table_metadata)

            # Perform batch validation
            validation_results = self.validator.validate_batch(
                validation_sample, table_metadata
            )

            # Extract metrics from validation results
            metrics['total_records_validated'] = validation_results['total_records']
            metrics['valid_records'] = validation_results['valid_records']
            metrics['invalid_records'] = validation_results['invalid_records']
            metrics['validation_errors'] = len(validation_results['errors'])

            # Calculate success rate
            if metrics['total_records_validated'] > 0:
                metrics['validation_success_rate'] = (
                        metrics['valid_records'] / metrics['total_records_validated']
                )

            # Analyze error patterns
            error_analysis = self._analyze_validation_errors(
                validation_results['errors'], table_name
            )

            # Update metrics with detailed error breakdown
            metrics.update(error_analysis['error_counts'])
            metrics['most_common_error_type'] = error_analysis['most_common_error']

            # Log validation results
            self._log_validation_results(batch_count, metrics, validation_results)

            # Update validation cache
            self._update_validation_cache(table_name, metrics, error_analysis)

            # Apply validation-based filtering if enabled
            if getattr(self.config.validation, 'filter_invalid_records', False):
                filtered_batch = self._filter_invalid_records(
                    batch, validation_results, validation_sample
                )
                return filtered_batch, metrics

        except Exception as e:
            self.logger.warning(f"Data validation failed for batch {batch_count}: {e}")
            metrics['validation_errors'] = 1

        return batch, metrics

    def _get_validation_frequency(self, table_name: str) -> int:
        """Determine how often to validate based on table history"""
        base_frequency = getattr(self.config.validation, 'validation_frequency', 10)

        # Check cache for historical validation success rates
        if table_name in self._validation_cache:
            historical_success_rate = self._validation_cache[table_name].get('avg_success_rate', 1.0)

            # Validate more frequently if success rate is low
            if historical_success_rate < 0.8:
                return max(1, base_frequency // 3)  # Validate every 3-4 batches
            elif historical_success_rate < 0.95:
                return max(1, base_frequency // 2)  # Validate every 5 batches
            else:
                return base_frequency * 2  # Validate less frequently for clean data

        return base_frequency

    def _get_validation_sample(self, batch: List[Dict], table_metadata: Dict[str, Any]) -> List[Dict]:
        """Get sample for validation to optimize performance"""
        max_validation_size = getattr(self.config.validation, 'max_validation_sample', 100)

        if len(batch) <= max_validation_size:
            return batch

        # Sample strategy: take from beginning, middle, and end
        sample_size = min(max_validation_size, len(batch))
        step = len(batch) // sample_size

        sampled_indices = []
        for i in range(0, len(batch), step):
            sampled_indices.append(i)
            if len(sampled_indices) >= sample_size:
                break

        return [batch[i] for i in sampled_indices[:sample_size]]

    def _analyze_validation_errors(self, errors: List[str], table_name: str) -> Dict[str, Any]:
        """Analyze validation errors to identify patterns"""
        error_counts = {
            'type_validation_errors': 0,
            'constraint_validation_errors': 0,
            'null_constraint_violations': 0,
            'length_constraint_violations': 0,
            'rule_validation_errors': 0
        }

        error_type_frequency = {}

        for error in errors:
            error_lower = error.lower()

            # Categorize errors
            if 'type' in error_lower and 'does not match' in error_lower:
                error_counts['type_validation_errors'] += 1
                error_type = 'type_mismatch'
            elif 'cannot be null' in error_lower:
                error_counts['null_constraint_violations'] += 1
                error_type = 'null_violation'
            elif 'length' in error_lower and ('above' in error_lower or 'below' in error_lower):
                error_counts['length_constraint_violations'] += 1
                error_type = 'length_violation'
            elif any(keyword in error_lower for keyword in ['invalid', 'format', 'pattern', 'validation']):
                error_counts['rule_validation_errors'] += 1
                error_type = 'rule_violation'
            else:
                error_counts['constraint_validation_errors'] += 1
                error_type = 'other_constraint'

            # Track error type frequency
            error_type_frequency[error_type] = error_type_frequency.get(error_type, 0) + 1

        # Find most common error type
        most_common_error = max(error_type_frequency.items(), key=lambda x: x[1])[0] if error_type_frequency else None

        # Update error patterns cache
        if table_name not in self._error_patterns:
            self._error_patterns[table_name] = {}

        for error_type, count in error_type_frequency.items():
            if error_type not in self._error_patterns[table_name]:
                self._error_patterns[table_name][error_type] = []
            self._error_patterns[table_name][error_type].append(count)

            # Keep only recent history (last 10 validations)
            if len(self._error_patterns[table_name][error_type]) > 10:
                self._error_patterns[table_name][error_type] = self._error_patterns[table_name][error_type][-10:]

        return {
            'error_counts': error_counts,
            'most_common_error': most_common_error,
            'error_type_distribution': error_type_frequency
        }

    def _log_validation_results(self, batch_count: int, metrics: Dict[str, Any],
                                validation_results: Dict[str, Any]):
        """Log validation results with appropriate level"""
        success_rate = metrics['validation_success_rate']
        total_errors = metrics['validation_errors']

        if success_rate < 0.8 or total_errors > 20:
            # High error rate - log as warning
            self.logger.warning(
                f"Batch {batch_count} validation issues: "
                f"{metrics['invalid_records']}/{metrics['total_records_validated']} invalid "
                f"(Success rate: {success_rate:.1%}, Errors: {total_errors})"
            )

            # Log sample of errors
            sample_errors = validation_results['errors'][:3]
            for error in sample_errors:
                self.logger.warning(f"  - {error}")

            if len(validation_results['errors']) > 3:
                self.logger.warning(f"  ... and {len(validation_results['errors']) - 3} more errors")

        elif success_rate < 0.95 or total_errors > 5:
            # Moderate error rate - log as info
            self.logger.info(
                f"Batch {batch_count} validation summary: "
                f"{metrics['valid_records']}/{metrics['total_records_validated']} valid "
                f"(Success rate: {success_rate:.1%})"
            )
        else:
            # Low error rate - log as debug
            self.logger.debug(
                f"Batch {batch_count} validation passed: "
                f"{metrics['valid_records']}/{metrics['total_records_validated']} valid "
                f"(Success rate: {success_rate:.1%})"
            )

    def _update_validation_cache(self, table_name: str, metrics: Dict[str, Any],
                                 error_analysis: Dict[str, Any]):
        """Update validation cache with current results"""
        if table_name not in self._validation_cache:
            self._validation_cache[table_name] = {
                'success_rates': [],
                'error_counts': [],
                'validation_count': 0
            }

        cache_entry = self._validation_cache[table_name]

        # Add current success rate
        cache_entry['success_rates'].append(metrics['validation_success_rate'])
        cache_entry['error_counts'].append(metrics['validation_errors'])
        cache_entry['validation_count'] += 1

        # Keep only recent history
        if len(cache_entry['success_rates']) > 20:
            cache_entry['success_rates'] = cache_entry['success_rates'][-20:]
            cache_entry['error_counts'] = cache_entry['error_counts'][-20:]

        # Calculate average success rate
        cache_entry['avg_success_rate'] = sum(cache_entry['success_rates']) / len(cache_entry['success_rates'])
        cache_entry['avg_error_count'] = sum(cache_entry['error_counts']) / len(cache_entry['error_counts'])

        # Store most recent error patterns
        cache_entry['recent_error_types'] = error_analysis['error_type_distribution']

    def _filter_invalid_records(self, original_batch: List[Dict],
                                validation_results: Dict[str, Any],
                                validation_sample: List[Dict]) -> List[Dict]:
        """Filter out invalid records if filtering is enabled"""
        if len(validation_sample) == len(original_batch):
            # Full validation was performed
            valid_indices = validation_results['valid_indices']
            return [original_batch[i] for i in valid_indices]
        else:
            # Partial validation - only filter if success rate is very low
            success_rate = validation_results['valid_records'] / validation_results['total_records']
            if success_rate < 0.5:
                self.logger.warning(
                    f"Low validation success rate ({success_rate:.1%}) detected. "
                    f"Recommend full batch validation."
                )
            return original_batch

    def get_validation_summary(self, table_name: str = None) -> Dict[str, Any]:
        """Get validation summary for specific table or all tables"""
        if table_name:
            if table_name in self._validation_cache:
                cache_data = self._validation_cache[table_name]
                error_patterns = self._error_patterns.get(table_name, {})

                return {
                    'table_name': table_name,
                    'total_validations': cache_data['validation_count'],
                    'average_success_rate': cache_data['avg_success_rate'],
                    'average_error_count': cache_data['avg_error_count'],
                    'recent_success_rates': cache_data['success_rates'][-5:],
                    'common_error_patterns': error_patterns
                }
            else:
                return {'table_name': table_name, 'status': 'No validation history'}

        # Return summary for all tables
        summary = {
            'total_tables_validated': len(self._validation_cache),
            'tables': {}
        }

        for table, cache_data in self._validation_cache.items():
            summary['tables'][table] = {
                'validations': cache_data['validation_count'],
                'avg_success_rate': cache_data['avg_success_rate'],
                'recent_trend': 'improving' if len(cache_data['success_rates']) >= 2 and
                                               cache_data['success_rates'][-1] > cache_data['success_rates'][
                                                   -2] else 'stable'
            }

        return summary

    def clear_validation_cache(self, table_name: str = None):
        """Clear validation cache"""
        if table_name:
            self._validation_cache.pop(table_name, None)
            self._error_patterns.pop(table_name, None)
        else:
            self._validation_cache.clear()
            self._error_patterns.clear()

    def get_validation_stats(self) -> Dict[str, Any]:
        """Get validation performance statistics"""
        return self.validator.get_performance_stats()