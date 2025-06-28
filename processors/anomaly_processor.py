from typing import List, Dict, Any, Tuple
from .base_processor import BaseDataProcessor


class AnomalyDetectionProcessor(BaseDataProcessor):
    """Processor for anomaly detection and statistical outlier analysis"""

    def __init__(self, config, logger, anomaly_detector):
        super().__init__(config, logger)
        self.anomaly_detector = anomaly_detector
        self._anomaly_cache = {}

    def set_batch_interval(self):
        return 20

    def _is_enabled(self) -> bool:
        return getattr(self.config.validation, 'enable_anomaly_detection', False)

    def get_processor_name(self) -> str:
        return "Anomaly Detection"

    def process(self, batch: List[Dict], table_metadata: Dict[str, Any],
                batch_count: int) -> Tuple[List[Dict], Dict[str, Any]]:
        """Detect anomalies in data batch"""
        metrics = {
            'total_anomalies': 0,
            'anomaly_rate': 0.0,
            'duplicate_records': 0,
            'statistical_outliers': 0,
            'rare_categorical_values': 0,
            'high_missing_data_columns': 0,
            'anomaly_severity_high': 0,
            'anomaly_severity_medium': 0,
            'anomaly_severity_low': 0
        }

        try:
            # Get table name for caching
            table_name = table_metadata.get('table_name', 'unknown')

            # Use cached settings if available
            detection_settings = self._get_detection_settings(table_name, table_metadata)

            # Perform anomaly detection
            anomaly_results = self.anomaly_detector.detect_anomalies(
                batch, max_sample=detection_settings.get('max_sample', 1000)
            )

            # Extract metrics from results
            metrics['total_anomalies'] = anomaly_results.get('total_anomalies', 0)
            metrics['anomaly_rate'] = anomaly_results.get('anomaly_rate', 0.0)

            # Process individual anomaly types
            anomalies = anomaly_results.get('anomalies', [])
            for anomaly in anomalies:
                anomaly_type = anomaly.get('type', '')
                severity = anomaly.get('severity', 'low')
                count = anomaly.get('count', 1)

                # Update type-specific metrics
                if anomaly_type == 'duplicate_records':
                    metrics['duplicate_records'] = count
                elif anomaly_type == 'statistical_outlier':
                    metrics['statistical_outliers'] += count
                elif anomaly_type == 'rare_categorical_values':
                    metrics['rare_categorical_values'] += count
                elif anomaly_type == 'high_missing_data':
                    metrics['high_missing_data_columns'] = len(anomaly.get('columns', []))

                # Update severity metrics
                if severity == 'high':
                    metrics['anomaly_severity_high'] += 1
                elif severity == 'medium':
                    metrics['anomaly_severity_medium'] += 1
                else:
                    metrics['anomaly_severity_low'] += 1

            # Log significant anomalies
            if metrics['total_anomalies'] > 0:
                anomaly_rate_pct = metrics['anomaly_rate'] * 100
                high_severity_count = metrics['anomaly_severity_high']

                if high_severity_count > 0 or anomaly_rate_pct > 5.0:
                    self.logger.warning(
                        f"Batch {batch_count} anomalies detected: {metrics['total_anomalies']} "
                        f"(Rate: {anomaly_rate_pct:.1f}%, High severity: {high_severity_count})"
                    )
                else:
                    self.logger.debug(
                        f"Batch {batch_count} minor anomalies: {metrics['total_anomalies']} "
                        f"(Rate: {anomaly_rate_pct:.1f}%)"
                    )

            # Cache detection results for future batches
            self._update_anomaly_cache(table_name, anomaly_results, detection_settings)

        except Exception as e:
            self.logger.warning(f"Anomaly detection failed for batch {batch_count}: {e}")

        return batch, metrics

    def _get_detection_settings(self, table_name: str, table_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Get anomaly detection settings with caching"""
        if table_name in self._anomaly_cache:
            cached_settings = self._anomaly_cache[table_name].get('settings', {})
            if cached_settings:
                return cached_settings

        # Default settings based on table metadata and config
        settings = {
            'max_sample': getattr(self.config.validation, 'anomaly_max_sample', 1000),
            'outlier_threshold': getattr(self.config.validation, 'outlier_z_score_threshold', 3.0),
            'rare_value_threshold': getattr(self.config.validation, 'rare_value_percentage', 0.01),
            'missing_data_threshold': getattr(self.config.validation, 'high_missing_threshold', 0.1),
            'min_cardinality': getattr(self.config.validation, 'min_cardinality_threshold', 10)
        }

        # Adjust settings based on table size estimate
        estimated_size = table_metadata.get('estimated_rows', 10000)
        if estimated_size > 100000:
            settings['max_sample'] = min(2000, settings['max_sample'])
        elif estimated_size < 1000:
            settings['max_sample'] = min(500, settings['max_sample'])

        return settings

    def _update_anomaly_cache(self, table_name: str, anomaly_results: Dict[str, Any],
                              settings: Dict[str, Any]):
        """Update anomaly detection cache with results"""
        if table_name not in self._anomaly_cache:
            self._anomaly_cache[table_name] = {}

        cache_entry = self._anomaly_cache[table_name]
        cache_entry['settings'] = settings
        cache_entry['last_anomaly_rate'] = anomaly_results.get('anomaly_rate', 0.0)
        cache_entry['anomaly_types_seen'] = set(
            anomaly.get('type') for anomaly in anomaly_results.get('anomalies', [])
        )

        # Keep cache size manageable
        if len(self._anomaly_cache) > 50:
            # Remove oldest entries
            oldest_table = next(iter(self._anomaly_cache))
            del self._anomaly_cache[oldest_table]

    def get_anomaly_summary(self, table_name: str = None) -> Dict[str, Any]:
        """Get summary of anomaly detection results"""
        if table_name and table_name in self._anomaly_cache:
            return self._anomaly_cache[table_name]

        return {
            'cached_tables': list(self._anomaly_cache.keys()),
            'total_cached_entries': len(self._anomaly_cache)
        }

    def clear_anomaly_cache(self, table_name: str = None):
        """Clear anomaly detection cache"""
        if table_name:
            self._anomaly_cache.pop(table_name, None)
        else:
            self._anomaly_cache.clear()