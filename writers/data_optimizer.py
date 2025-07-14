import logging
from typing import Dict, List, Any, Optional, Union
import pandas as pd
import warnings


class DataOptimizer:
    """Optimized DataOptimizer that works directly with validated schema from SchemaValidator"""

    def __init__(self, logger: logging.Logger, schema: dict = None, target_format: str = None):
        self.logger = logger
        self.validated_schema = schema  # Already validated and converted schema
        self.target_format = target_format or 'csv'

        # Direct mapping from validated schema
        self.column_mappings = {}
        self.datetime_formats = {}

        if self.validated_schema:
            self._build_column_mappings()

    def _build_column_mappings(self):
        """Build column mappings directly from validated schema"""
        table_name = self.validated_schema.get('table_name')
        columns = self.validated_schema.get('columns', [])

        for column in columns:
            col_name = column.get('name')
            if not col_name:
                continue

            # Store all the validated information
            mapping = {
                'type': column.get('type'),
                'nullable': column.get('nullable', True),
                'rule': column.get('rule'),
                'length': column.get('length'),
                'constraint': column.get('constraint', []),
                'default': column.get('default'),
                'precision': column.get('precision'),
                'total_digits': column.get('total_digits'),
                'table_name': table_name,
                'corrections': column.get('corrections', [])
            }

            # Extract datetime format if available
            rule = column.get('rule')
            if isinstance(rule, dict) and rule.get('format'):
                self.datetime_formats[col_name] = rule['format']

            # Store mapping
            self.column_mappings[col_name] = mapping
            if table_name:
                self.column_mappings[f"{table_name}.{col_name}"] = mapping

    def optimize_dataframe(self, df: pd.DataFrame, target_format: str = None) -> pd.DataFrame:
        """Main optimization method that applies database-specific conversions"""
        if df.empty:
            return df

        format_name = target_format or self.target_format

        # Apply format-specific conversions
        df_optimized = self._apply_format_conversions(df, format_name)
        print(df_optimized)

        return df_optimized

    def _apply_format_conversions(self, df: pd.DataFrame, target_format: str) -> pd.DataFrame:
        """Apply format-specific conversions based on target format"""
        df_converted = df.copy()

        for col_name in df.columns:
            mapping = self.column_mappings.get(col_name)
            if not mapping:
                self.logger.debug(f"No mapping found for column '{col_name}', skipping optimization")
                continue

            # Apply conversion based on target format
            try:
                if target_format.lower() == 'parquet':
                    df_converted[col_name] = self._convert_for_parquet(df_converted[col_name], mapping)
                elif target_format.lower() in ['excel', 'xlsx', 'xls']:
                    df_converted[col_name] = self._convert_for_excel(df_converted[col_name], mapping)
                elif target_format.lower() in ['csv', 'tsv']:
                    df_converted[col_name] = self._convert_for_csv(df_converted[col_name], mapping)
                elif target_format.lower() in ['json', 'jsonl']:
                    df_converted[col_name] = self._convert_for_json(df_converted[col_name], mapping)
                else:
                    # Default conversion
                    df_converted[col_name] = self._convert_for_default(df_converted[col_name], mapping)

            except Exception as e:
                self.logger.warning(f"Failed to convert column '{col_name}': {e}")
                # Keep original data if conversion fails
                continue

        return df_converted

    def _convert_for_parquet(self, series: pd.Series, mapping: Dict[str, Any]) -> pd.Series:
        """Convert for Parquet format - preserve numeric types, handle decimals properly"""
        col_type = mapping.get('type', '').lower()
        rule = mapping.get('rule', {})
        precision = mapping.get('precision')

        if col_type == 'int':
            return pd.to_numeric(series, errors='coerce').astype('Int64')

        elif col_type == 'float':
            # Handle decimal precision if specified
            if precision is not None:
                numeric_series = pd.to_numeric(series, errors='coerce')
                return self._handle_decimal_precision(numeric_series, precision, 'parquet')
            else:
                return pd.to_numeric(series, errors='coerce').astype('float64')

        elif col_type == 'bool':
            return series.astype('boolean')  # Nullable boolean

        elif col_type == 'str':
            # Handle length constraints for strings
            length = mapping.get('length')
            if isinstance(length, dict) and 'max' in length:
                return series.astype(str).str.slice(0, length['max'])
            elif isinstance(length, int):
                return series.astype(str).str.slice(0, length)
            else:
                return series.astype(str)

        return series

    def _convert_for_excel(self, series: pd.Series, mapping: Dict[str, Any]) -> pd.Series:
        """Convert for Excel format - handle Excel-specific limitations"""
        col_type = mapping.get('type', '').lower()
        rule = mapping.get('rule', {})
        precision = mapping.get('precision')

        if col_type == 'int':
            # Check for large integers that Excel can't handle
            numeric_series = pd.to_numeric(series, errors='coerce')
            max_val = numeric_series.max() if len(numeric_series) > 0 and numeric_series.notna().any() else 0
            if max_val > 10 ** 15:  # Excel's safe integer limit
                return series.astype(str)
            return numeric_series.astype('int64')

        elif col_type == 'float':
            numeric_series = pd.to_numeric(series, errors='coerce')
            if precision is not None:
                return numeric_series.round(precision)
            return numeric_series.astype('float64')

        elif col_type == 'bool':
            return series.astype(bool)

        elif col_type == 'str':
            # Handle datetime formatting for Excel
            if self._is_datetime_rule(rule):
                return self._format_datetime_for_excel(series, rule)
            return series.astype(str)

        return series

    def _convert_for_csv(self, series: pd.Series, mapping: Dict[str, Any]) -> pd.Series:
        """Convert for CSV format - string formatting with proper precision"""
        col_type = mapping.get('type', '').lower()
        rule = mapping.get('rule', {})
        precision = mapping.get('precision')

        if col_type == 'int':
            return pd.to_numeric(series, errors='coerce').astype('Int64')

        elif col_type == 'float':
            numeric_series = pd.to_numeric(series, errors='coerce')
            if precision is not None:
                # Format as string with specific decimal places for CSV
                def format_decimal(value):
                    if pd.isna(value):
                        return ""
                    return f"{value:.{precision}f}"

                return numeric_series.apply(format_decimal)
            return numeric_series

        elif col_type == 'bool':
            return series.astype(str).str.lower()

        elif col_type == 'str':
            # Handle datetime formatting
            if self._is_datetime_rule(rule):
                return self._format_datetime_with_rule(series, rule)
            return series.astype(str)

        return series

    def _convert_for_json(self, series: pd.Series, mapping: Dict[str, Any]) -> pd.Series:
        """Convert for JSON format - JSON-compatible types"""
        col_type = mapping.get('type', '').lower()
        rule = mapping.get('rule', {})

        if col_type == 'int':
            return pd.to_numeric(series, errors='coerce').astype('Int64')

        elif col_type == 'float':
            numeric_series = pd.to_numeric(series, errors='coerce')
            return numeric_series.where(pd.notna(numeric_series), None)

        elif col_type == 'bool':
            return series.astype(bool)

        elif col_type == 'str':
            if self._is_datetime_rule(rule):
                # Format datetime as ISO string for JSON
                return self._format_datetime_for_json(series, rule)
            return series.astype(str)

        return series

    def _convert_for_default(self, series: pd.Series, mapping: Dict[str, Any]) -> pd.Series:
        """Default conversion - preserve original types as much as possible"""
        col_type = mapping.get('type', '').lower()

        if col_type == 'int':
            return pd.to_numeric(series, errors='coerce').astype('Int64')
        elif col_type == 'float':
            return pd.to_numeric(series, errors='coerce').astype('float64')
        elif col_type == 'bool':
            return series.astype('boolean')
        else:
            return series.astype(str)

    def _handle_decimal_precision(self, series: pd.Series, precision: int, format_type: str) -> pd.Series:
        """Handle decimal precision based on format type"""
        if format_type == 'parquet':
            # For Parquet, try to use PyArrow decimal if available
            try:
                import pyarrow as pa
                from decimal import Decimal

                total_digits = 18  # Default
                scale = precision

                def to_decimal(value):
                    if pd.isna(value):
                        return None
                    return Decimal(str(value)).quantize(Decimal('0.' + '0' * scale))

                decimal_series = series.apply(to_decimal)
                decimal_type = pa.decimal128(total_digits, scale)
                decimal_array = pa.array(decimal_series.tolist(), type=decimal_type)
                result = pa.table([decimal_array], names=[series.name]).to_pandas()[series.name]
                return result
            except ImportError:
                # Fallback to regular float with rounding
                return series.round(precision)
        else:
            # For other formats, round to specified precision
            return series.round(precision)

    def _is_datetime_rule(self, rule: Union[str, Dict]) -> bool:
        """Check if rule indicates datetime data"""
        if isinstance(rule, dict):
            rule_type = rule.get('type', '').lower()
            return rule_type in ['date_range', 'time_range', 'timestamp_range']
        elif isinstance(rule, str):
            return rule.lower() in ['date', 'time', 'datetime', 'timestamp']
        return False

    def _format_datetime_with_rule(self, series: pd.Series, rule: Dict) -> pd.Series:
        """Format datetime using rule specifications"""
        rule_type = rule.get('type', '').lower()
        format_str = rule.get('format')
        col_name = series.name

        if format_str:
            # Use the validated format from schema
            try:
                converted = pd.to_datetime(series, format=format_str, errors='coerce')

                # Format output based on rule type
                if rule_type == 'date_range':
                    return converted.dt.strftime('%Y-%m-%d')
                elif rule_type == 'time_range':
                    return converted.dt.strftime('%H:%M:%S')
                elif rule_type == 'timestamp_range':
                    return converted.dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    return converted.dt.strftime(format_str)

            except Exception as e:
                self.logger.warning(f"Failed to format datetime for column {col_name}: {e}")

        # Fallback to auto-detection
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            converted = pd.to_datetime(series, errors='coerce')

        if rule_type == 'date_range':
            return converted.dt.strftime('%Y-%m-%d')
        elif rule_type == 'time_range':
            return converted.dt.strftime('%H:%M:%S')
        else:
            return converted.dt.strftime('%Y-%m-%d %H:%M:%S')

    def _format_datetime_for_excel(self, series: pd.Series, rule: Dict) -> pd.Series:
        """Format datetime specifically for Excel"""
        try:
            # Try to convert to pandas datetime for Excel
            converted = pd.to_datetime(series, errors='coerce')
            return converted
        except:
            return self._format_datetime_with_rule(series, rule)

    def _format_datetime_for_json(self, series: pd.Series, rule: Dict) -> pd.Series:
        """Format datetime specifically for JSON (ISO format)"""
        rule_type = rule.get('type', '').lower()
        try:
            converted = pd.to_datetime(series, errors='coerce')
            if rule_type == 'date_range':
                return converted.dt.strftime('%Y-%m-%d')
            else:
                return converted.dt.strftime('%Y-%m-%dT%H:%M:%S')
        except:
            return series.astype(str)

    def get_column_info(self, column_name: str) -> Dict[str, Any]:
        """Get column information from validated schema"""
        return self.column_mappings.get(column_name, {})

    def has_mappings(self) -> bool:
        """Check if column mappings are available"""
        return bool(self.column_mappings)

    def get_supported_formats(self) -> List[str]:
        """Get list of supported output formats"""
        return ['parquet', 'excel', 'xlsx', 'xls', 'csv', 'tsv', 'json', 'jsonl']

    def get_validation_info(self, column_name: str) -> Dict[str, Any]:
        """Get validation and correction information for a column"""
        mapping = self.column_mappings.get(column_name, {})
        return {
            'original_type': self._get_original_type(mapping),
            'converted_type': mapping.get('type'),
            'corrections_applied': len(mapping.get('corrections', [])),
            'has_length_constraint': mapping.get('length') is not None,
            'has_precision': mapping.get('precision') is not None,
            'nullable': mapping.get('nullable', True),
            'constraints': mapping.get('constraint', [])
        }

    def _get_original_type(self, mapping: Dict[str, Any]) -> Optional[str]:
        """Extract original type before conversion from corrections"""
        corrections = mapping.get('corrections', [])
        for correction in corrections:
            if correction.get('field') == 'type' and correction.get('old_value'):
                return correction['old_value']
        return mapping.get('type')

    def get_conversion_summary(self) -> Dict[str, Any]:
        """Get summary of all type conversions and optimizations"""
        summary = {
            'total_columns': len(self.column_mappings),
            'conversions': {},
            'constraints': {},
            'datetime_columns': 0
        }

        for col_name, mapping in self.column_mappings.items():
            if '.' in col_name:  # Skip qualified names
                continue

            # Count conversions
            original_type = self._get_original_type(mapping)
            current_type = mapping.get('type')
            if original_type and original_type != current_type:
                conversion_key = f"{original_type} -> {current_type}"
                summary['conversions'][conversion_key] = summary['conversions'].get(conversion_key, 0) + 1

            # Count constraints
            if mapping.get('length'):
                summary['constraints']['length'] = summary['constraints'].get('length', 0) + 1
            if mapping.get('precision'):
                summary['constraints']['precision'] = summary['constraints'].get('precision', 0) + 1
            if mapping.get('constraint'):
                summary['constraints']['database'] = summary['constraints'].get('database', 0) + 1

            # Count datetime columns
            rule = mapping.get('rule', {})
            if self._is_datetime_rule(rule):
                summary['datetime_columns'] += 1

        return summary