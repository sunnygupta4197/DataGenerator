import logging
from typing import Dict, List, Any, Optional, Union
import pandas as pd
import warnings


class DataOptimizer:
    def __init__(self, logger: logging.Logger, schema: dict = None, target_format: str = None):
        self.logger = logger
        self.validated_schema = schema
        self.target_format = target_format or 'csv'
        self.column_mappings = {}
        self.datetime_formats = {}

        if self.validated_schema:
            self._build_column_mappings()

    def _build_column_mappings(self):
        table_name = self.validated_schema.get('table_name')
        columns = self.validated_schema.get('columns', [])

        for column in columns:
            col_name = column.get('name')
            if not col_name:
                continue
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
            rule = column.get('rule')
            if isinstance(rule, dict) and rule.get('format'):
                self.datetime_formats[col_name] = rule['format']

            self.column_mappings[col_name] = mapping
            if table_name:
                self.column_mappings[f"{table_name}.{col_name}"] = mapping

    def optimize_dataframe(self, df: pd.DataFrame, target_format: str = None) -> pd.DataFrame:
        if df.empty:
            return df
        format_name = target_format or self.target_format
        df_optimized = self._apply_format_conversions(df, format_name)
        return df_optimized

    def _apply_format_conversions(self, df: pd.DataFrame, target_format: str) -> pd.DataFrame:
        df_converted = df.copy()
        for col_name in df.columns:
            mapping = self.column_mappings.get(col_name)
            if not mapping:
                self.logger.debug(f"No mapping found for column '{col_name}', skipping optimization")
                continue
            if self._should_format_as_datetime_string(mapping):
                self.logger.debug(f"Detected string column with datetime rule '{col_name}', formatting as string")
                try:
                    rule = mapping.get('rule', {})
                    df_converted[col_name] = self._format_datetime_string_column(df_converted[col_name], rule, mapping)
                    continue
                except Exception as e:
                    self.logger.warning(f"Failed to format datetime string column '{col_name}': {e}")

            elif self._should_convert_to_datetime(mapping):
                self.logger.debug(f"Detected datetime column '{col_name}', converting from numeric timestamp")
                try:
                    rule = mapping.get('rule', {})
                    original_type = self._get_original_type(mapping)
                    df_converted[col_name] = self._convert_to_proper_datetime(df_converted[col_name], rule,
                                                                              original_type or 'datetime')

                    rule = mapping.get('rule', {})
                    original_type = self._get_original_type(mapping) or 'datetime'

                    if target_format.lower() in ['json', 'jsonl']:
                        df_converted[col_name] = self._format_datetime_for_json_from_datetime(df_converted[col_name],
                                                                                              rule)
                    else:
                        df_converted[col_name] = self._format_datetime_as_string_from_datetime(df_converted[col_name],
                                                                                               rule, original_type)
                    continue
                except Exception as e:
                    self.logger.warning(f"Failed to convert datetime column '{col_name}': {e}")
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
                    df_converted[col_name] = self._convert_for_default(df_converted[col_name], mapping)

            except Exception as e:
                self.logger.warning(f"Failed to convert column '{col_name}': {e}")
                continue

        return df_converted

    def _should_format_as_datetime_string(self, mapping: Dict[str, Any]) -> bool:
        col_type = mapping.get('type', '').lower()
        rule = mapping.get('rule', {})
        original_type = self._get_original_type(mapping)
        if original_type and original_type.lower() in ['date', 'datetime', 'timestamp', 'time']:
            return False

        if col_type == 'str' and self._is_datetime_rule(rule):
            return True

        return False

    def _format_datetime_string_column(self, series: pd.Series, rule: Dict, mapping: Dict[str, Any]) -> pd.Series:
        try:
            if pd.api.types.is_datetime64_any_dtype(series):
                datetime_series = series
            else:
                if self._is_numeric_timestamp(series):
                    datetime_series = self._convert_numeric_timestamp(series)
                else:
                    format_str = rule.get('format') if isinstance(rule, dict) else None
                    if format_str:
                        datetime_series = pd.to_datetime(series, format=format_str, errors='coerce')
                    else:
                        datetime_series = pd.to_datetime(series, errors='coerce')

            rule_type = rule.get('type', '').lower() if isinstance(rule, dict) else ''
            format_str = rule.get('format') if isinstance(rule, dict) else None

            if format_str:
                return datetime_series.dt.strftime(format_str)
            elif rule_type == 'date_range':
                return datetime_series.dt.strftime('%Y-%m-%d')
            elif rule_type == 'time_range':
                return datetime_series.dt.strftime('%H:%M:%S')
            elif rule_type == 'timestamp_range':
                return datetime_series.dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                return datetime_series.dt.strftime('%Y-%m-%d %H:%M:%S')

        except Exception as e:
            self.logger.warning(f"Failed to format datetime string column {series.name}: {e}")
            return series.astype(str)

    def _should_convert_to_datetime(self, mapping: Dict[str, Any]) -> bool:
        original_type = self._get_original_type(mapping)
        if original_type and original_type.lower() in ['date', 'datetime', 'timestamp', 'time']:
            return True

        col_type = mapping.get('type', '').lower()
        if col_type in ['date', 'datetime', 'timestamp', 'time']:
            return True

        return False

    def _format_datetime_as_string_from_datetime(self, series: pd.Series, rule: Dict, original_type: str) -> pd.Series:
        try:
            if original_type and original_type.lower() == 'date':
                return series.dt.strftime('%Y-%m-%d')
            elif original_type and original_type.lower() == 'time':
                return series.dt.strftime('%H:%M:%S')
            elif original_type and original_type.lower() in ['datetime', 'timestamp']:
                return series.dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                rule_type = rule.get('type', '').lower() if isinstance(rule, dict) else ''
                if rule_type == 'date_range':
                    return series.dt.strftime('%Y-%m-%d')
                elif rule_type == 'time_range':
                    return series.dt.strftime('%H:%M:%S')
                else:
                    return series.dt.strftime('%Y-%m-%d %H:%M:%S')

        except Exception as e:
            self.logger.warning(f"Failed to format datetime as string for {series.name}: {e}")
            return series.astype(str)

    def _format_datetime_for_json_from_datetime(self, series: pd.Series, rule: Dict) -> pd.Series:
        try:
            rule_type = rule.get('type', '').lower() if isinstance(rule, dict) else ''
            if rule_type == 'date_range':
                return series.dt.strftime('%Y-%m-%d')
            else:
                return series.dt.strftime('%Y-%m-%dT%H:%M:%S')
        except Exception as e:
            self.logger.warning(f"Failed to format datetime for JSON for {series.name}: {e}")
            return series.astype(str)

    def _convert_for_parquet(self, series: pd.Series, mapping: Dict[str, Any]) -> pd.Series:
        col_type = mapping.get('type', '').lower()
        rule = mapping.get('rule', {})
        precision = mapping.get('precision')
        if col_type == 'int':
            return pd.to_numeric(series, errors='coerce').astype('Int64')

        elif col_type == 'float':
            if precision is not None:
                numeric_series = pd.to_numeric(series, errors='coerce')
                return self._handle_decimal_precision(numeric_series, precision, 'parquet')
            else:
                return pd.to_numeric(series, errors='coerce').astype('float64')

        elif col_type == 'bool':
            return series.astype('boolean')  # Nullable boolean

        elif col_type == 'str':
            length = mapping.get('length')
            if isinstance(length, dict) and 'max' in length:
                return series.astype(str).str.slice(0, length['max'])
            elif isinstance(length, int):
                return series.astype(str).str.slice(0, length)
            else:
                return series.astype(str)

        return series

    def _convert_for_excel(self, series: pd.Series, mapping: Dict[str, Any]) -> pd.Series:
        col_type = mapping.get('type', '').lower()
        rule = mapping.get('rule', {})
        precision = mapping.get('precision')

        original_type = self._get_original_type(mapping)
        if original_type and original_type.lower() in ['date', 'datetime', 'timestamp', 'time'] and col_type != 'str':
            return self._convert_to_proper_datetime(series, rule, original_type)

        if col_type == 'int':
            numeric_series = pd.to_numeric(series, errors='coerce')
            max_val = numeric_series.max() if len(numeric_series) > 0 and numeric_series.notna().any() else 0
            if max_val > 10 ** 15:
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
            length = mapping.get('length')
            if isinstance(length, dict) and 'max' in length:
                return series.astype(str).str.slice(0, length['max'])
            elif isinstance(length, int):
                return series.astype(str).str.slice(0, length)
            else:
                return series.astype(str)

        return series

    def _convert_for_csv(self, series: pd.Series, mapping: Dict[str, Any]) -> pd.Series:
        col_type = mapping.get('type', '').lower()
        rule = mapping.get('rule', {})
        precision = mapping.get('precision')

        original_type = self._get_original_type(mapping)
        if original_type and original_type.lower() in ['date', 'datetime', 'timestamp', 'time'] and col_type != 'str':
            return self._format_datetime_as_string(series, rule, original_type)

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
            length = mapping.get('length')
            if isinstance(length, dict) and 'max' in length:
                return series.astype(str).str.slice(0, length['max'])
            elif isinstance(length, int):
                return series.astype(str).str.slice(0, length)
            else:
                return series.astype(str)

        return series

    def _format_datetime_as_string(self, series: pd.Series, rule: Dict, original_type: str) -> pd.Series:
        try:
            if self._is_numeric_timestamp(series):
                converted = self._convert_numeric_timestamp(series)
            else:
                format_str = rule.get('format') if isinstance(rule, dict) else None
                if format_str:
                    converted = pd.to_datetime(series, format=format_str, errors='coerce')
                else:
                    converted = pd.to_datetime(series, errors='coerce')

            if original_type.lower() == 'date':
                return converted.dt.strftime('%Y-%m-%d')
            elif original_type.lower() == 'time':
                return converted.dt.strftime('%H:%M:%S')
            elif original_type.lower() in ['datetime', 'timestamp']:
                return converted.dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                rule_type = rule.get('type', '').lower() if isinstance(rule, dict) else ''
                if rule_type == 'date_range':
                    return converted.dt.strftime('%Y-%m-%d')
                elif rule_type == 'time_range':
                    return converted.dt.strftime('%H:%M:%S')
                else:
                    return converted.dt.strftime('%Y-%m-%d %H:%M:%S')

        except Exception as e:
            self.logger.warning(f"Failed to format datetime as string for {series.name}: {e}")
            return series.astype(str)

    def _convert_for_json(self, series: pd.Series, mapping: Dict[str, Any]) -> pd.Series:
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
            length = mapping.get('length')
            if isinstance(length, dict) and 'max' in length:
                return series.astype(str).str.slice(0, length['max'])
            elif isinstance(length, int):
                return series.astype(str).str.slice(0, length)
            else:
                return series.astype(str)

        return series

    def _convert_for_default(self, series: pd.Series, mapping: Dict[str, Any]) -> pd.Series:
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
        if format_type == 'parquet':
            try:
                import pyarrow as pa
                from decimal import Decimal

                total_digits = 18
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
                return series.round(precision)
        else:
            return series.round(precision)

    def _is_datetime_rule(self, rule: Union[str, Dict]) -> bool:
        if isinstance(rule, dict):
            rule_type = rule.get('type', '').lower()
            return rule_type in ['date_range', 'time_range', 'timestamp_range', 'date', 'time', 'timestamp']
        elif isinstance(rule, str):
            return rule.lower() in ['date', 'time', 'datetime', 'timestamp']
        return False

    def _format_datetime_with_rule(self, series: pd.Series, rule: Dict) -> pd.Series:
        rule_type = rule.get('type', '').lower()
        format_str = rule.get('format')
        col_name = series.name
        if self._is_numeric_timestamp(series):
            try:
                converted = self._convert_numeric_timestamp(series)
                if format_str:
                    return converted.dt.strftime(format_str)
                elif rule_type == 'date_range':
                    return converted.dt.strftime('%Y-%m-%d')
                elif rule_type == 'time_range':
                    return converted.dt.strftime('%H:%M:%S')
                elif rule_type == 'timestamp_range':
                    return converted.dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    return converted.dt.strftime('%Y-%m-%d %H:%M:%S')

            except Exception as e:
                self.logger.warning(f"Failed to convert numeric timestamp for {col_name}: {e}")
                return series.astype(str)
        if format_str:
            try:
                converted = pd.to_datetime(series, format=format_str, errors='coerce')

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

        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                converted = pd.to_datetime(series, errors='coerce')

            if rule_type == 'date_range':
                return converted.dt.strftime('%Y-%m-%d')
            elif rule_type == 'time_range':
                return converted.dt.strftime('%H:%M:%S')
            else:
                return converted.dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return series.astype(str)

    def _convert_to_proper_datetime(self, series: pd.Series, rule: Dict, original_type: str) -> pd.Series:
        """Convert to proper datetime object for database compatibility"""
        try:
            if self._is_numeric_timestamp(series):
                converted = self._convert_numeric_timestamp(series)
                self.logger.debug(f"Converted numeric timestamp to datetime for column {series.name}")
                return converted

            format_str = rule.get('format') if isinstance(rule, dict) else None

            if format_str:
                try:
                    converted = pd.to_datetime(series, format=format_str, errors='coerce')
                    self.logger.debug(
                        f"Converted to datetime using schema format {format_str} for column {series.name}")
                    return converted
                except Exception as e:
                    self.logger.debug(f"Schema format {format_str} failed, trying auto-detection: {e}")

            # Fallback to pandas auto-detection
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                converted = pd.to_datetime(series, errors='coerce')

            self.logger.debug(f"Converted to datetime using auto-detection for column {series.name}")
            return converted

        except Exception as e:
            self.logger.warning(f"Failed to convert to proper datetime for column {series.name}: {e}")
            return series

    def should_preserve_as_datetime(self, column_name: str) -> bool:
        mapping = self.column_mappings.get(column_name, {})

        original_type = self._get_original_type(mapping)
        if original_type and original_type.lower() in ['date', 'datetime', 'timestamp', 'time']:
            return True

        col_type = mapping.get('type', '').lower()
        if col_type in ['date', 'datetime', 'timestamp', 'time']:
            return True

        return False

    def _is_numeric_timestamp(self, series: pd.Series) -> bool:
        try:
            numeric_series = pd.to_numeric(series, errors='coerce')
            non_null_count = numeric_series.notna().sum()

            if non_null_count == 0:
                return False
            sample_values = numeric_series.dropna().head(5)

            for value in sample_values:
                if self._is_reasonable_timestamp(value):
                    return True

            return False
        except:
            return False

    def _is_reasonable_timestamp(self, value: float) -> bool:
        if 1e8 <= value <= 3e9:  # Seconds (1973-2065)
            return True
        elif 1e11 <= value <= 3e12:  # Milliseconds
            return True
        elif 1e14 <= value <= 3e15:  # Microseconds
            return True
        elif 1e17 <= value <= 3e18:  # Nanoseconds
            return True

        return False

    def _convert_numeric_timestamp(self, series: pd.Series) -> pd.Series:
        numeric_series = pd.to_numeric(series, errors='coerce')
        sample_values = numeric_series.dropna().head(5)

        if len(sample_values) == 0:
            return pd.to_datetime(series, errors='coerce')

        sample_value = sample_values.iloc[0]

        try:
            if 1e8 <= sample_value <= 3e9:  # Seconds
                self.logger.debug(f"Detected seconds timestamp: {sample_value}")
                result = pd.to_datetime(numeric_series, unit='s', errors='coerce')
            elif 1e11 <= sample_value <= 3e12:  # Milliseconds
                self.logger.debug(f"Detected milliseconds timestamp: {sample_value}")
                result = pd.to_datetime(numeric_series, unit='ms', errors='coerce')
            elif 1e14 <= sample_value <= 3e15:  # Microseconds
                self.logger.debug(f"Detected microseconds timestamp: {sample_value}")
                result = pd.to_datetime(numeric_series, unit='us', errors='coerce')
            elif 1e17 <= sample_value <= 3e18:  # Nanoseconds
                self.logger.debug(f"Detected nanoseconds timestamp: {sample_value}")
                result = pd.to_datetime(numeric_series, unit='ns', errors='coerce')
            else:
                self.logger.warning(f"Unknown timestamp format for value: {sample_value}")
                result = pd.to_datetime(numeric_series, errors='coerce')

            if result.notna().any():
                self.logger.info(
                    f"Successfully converted numeric timestamp {sample_value} to datetime: {result.dropna().iloc[0] if len(result.dropna()) > 0 else 'N/A'}")
                return result
            else:
                self.logger.warning(f"Timestamp conversion resulted in all NaT values")
                return pd.to_datetime(series, errors='coerce')

        except Exception as e:
            self.logger.warning(f"Failed to convert numeric timestamp: {e}")
            return pd.to_datetime(series, errors='coerce')

    def _is_date_type_column(self, mapping: Dict[str, Any]) -> bool:
        corrections = mapping.get('corrections', [])
        for correction in corrections:
            if correction.get('field') == 'type':
                old_value = correction.get('old_value', '').lower()
                if old_value in ['date', 'datetime', 'timestamp', 'time']:
                    return True
        rule = mapping.get('rule', {})
        return self._is_datetime_rule(rule)

    def _format_datetime_for_json(self, series: pd.Series, rule: Dict) -> pd.Series:
        rule_type = rule.get('type', '').lower()

        try:
            # Check if we have numeric timestamps first
            if self._is_numeric_timestamp(series):
                converted = self._convert_numeric_timestamp(series)
            else:
                converted = pd.to_datetime(series, errors='coerce')

            if rule_type == 'date_range':
                return converted.dt.strftime('%Y-%m-%d')
            else:
                return converted.dt.strftime('%Y-%m-%dT%H:%M:%S')
        except:
            return series.astype(str)

    def get_column_info(self, column_name: str) -> Dict[str, Any]:
        return self.column_mappings.get(column_name, {})

    def has_mappings(self) -> bool:
        return bool(self.column_mappings)

    def get_supported_formats(self) -> List[str]:
        return ['parquet', 'excel', 'xlsx', 'xls', 'csv', 'tsv', 'json', 'jsonl']

    def get_validation_info(self, column_name: str) -> Dict[str, Any]:
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
        corrections = mapping.get('corrections', [])
        for correction in corrections:
            if correction.get('field') == 'type' and correction.get('old_value'):
                return correction['old_value']
        return mapping.get('type')

    def get_conversion_summary(self) -> Dict[str, Any]:
        summary = {
            'total_columns': len(self.column_mappings),
            'conversions': {},
            'constraints': {},
            'datetime_columns': 0,
            'string_datetime_columns': 0
        }

        for col_name, mapping in self.column_mappings.items():
            if '.' in col_name:
                continue

            original_type = self._get_original_type(mapping)
            current_type = mapping.get('type')
            if original_type and original_type != current_type:
                conversion_key = f"{original_type} -> {current_type}"
                summary['conversions'][conversion_key] = summary['conversions'].get(conversion_key, 0) + 1

            if mapping.get('length'):
                summary['constraints']['length'] = summary['constraints'].get('length', 0) + 1
            if mapping.get('precision'):
                summary['constraints']['precision'] = summary['constraints'].get('precision', 0) + 1
            if mapping.get('constraint'):
                summary['constraints']['database'] = summary['constraints'].get('database', 0) + 1

            rule = mapping.get('rule', {})
            if self._is_datetime_rule(rule):
                if current_type == 'str':
                    summary['string_datetime_columns'] += 1
                else:
                    summary['datetime_columns'] += 1

        return summary