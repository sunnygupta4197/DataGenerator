import logging
from typing import Dict, List, Any, Tuple, Optional

import pandas as pd
import numpy as np


class AnomalyDetector:
    """High-performance anomaly detection"""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self._detection_cache = {}

    def detect_anomalies(self, data: List[Dict], max_sample: int = 1000) -> Dict[str, Any]:
        """Detect anomalies in data using statistical methods"""
        if not data:
            return {'anomalies': [], 'total_anomalies': 0, 'anomaly_rate': 0.0}

        # Sample for performance
        sample_data = data[:min(max_sample, len(data))]
        df = pd.DataFrame(sample_data)

        anomalies = []

        # Numerical anomaly detection using Z-scores
        numerical_cols = df.select_dtypes(include=[np.number]).columns

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

        for col in numerical_cols:
            if df[col].notna().sum() > 10:
                z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
                outliers = df[z_scores > 3].index.tolist()

                if outliers:
                    anomalies.extend([{
                        'type': 'statistical_outlier',
                        'column': col,
                        'severity': 'high' if len(outliers) > len(df) * 0.05 else 'medium',
                        'count': len(outliers)
                    }])

        # Categorical anomaly detection
        categorical_cols = df.select_dtypes(include=['object']).columns

        for col in categorical_cols:
            value_counts = df[col].value_counts()
            rare_threshold = len(df) * 0.01
            rare_values = value_counts[value_counts < rare_threshold]

            if len(rare_values) > 0:
                anomalies.append({
                    'type': 'rare_categorical_values',
                    'column': col,
                    'severity': 'low',
                    'count': len(rare_values)
                })

        # Missing data pattern detection
        missing_rates = df.isnull().mean()
        high_missing = missing_rates[missing_rates > 0.1].index.tolist()

        if high_missing:
            anomalies.append({
                'type': 'high_missing_data',
                'columns': high_missing,
                'severity': 'medium',
                'max_missing_rate': missing_rates.max()
            })

        anomaly_rate = len(anomalies) / len(df) if df.shape[0] > 0 else 0.0

        return {
            'anomalies': anomalies,
            'total_anomalies': len(anomalies),
            'anomaly_rate': anomaly_rate,
            'sample_size': len(sample_data)
        }

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

