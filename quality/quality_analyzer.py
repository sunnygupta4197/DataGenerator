import logging
from typing import Dict, List, Any
import pandas as pd


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