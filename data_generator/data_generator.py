import random
from faker import Faker
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any, Optional, Callable, Generator
import pandas as pd

from .value_generator import ValueGenerator
from constraint_manager.optimized_constraint_manager import OptimizedConstraintManager
from validators.unified_validation_system import UnifiedValidator

try:
    import openai

    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("Warning: OpenAI package not available. AI fallback will be disabled.")


class DataTypeConverter:
    """Handles data type conversions - separated for clarity"""

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def convert_to_type(self, value: str, data_type: str) -> Any:
        """Convert string value to specified data type"""
        if value is None or value == 'None':
            return None

        converters = {
            'int': self._to_int,
            'integer': self._to_int,
            'float': self._to_float,
            'double': self._to_float,
            'decimal': self._to_float,
            'bool': self._to_bool,
            'boolean': self._to_bool,
            'date': self._to_date,
            'datetime': self._to_datetime,
            'timestamp': self._to_datetime,
        }

        converter = converters.get(data_type.lower(), str)

        try:
            return converter(value)
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Could not convert '{value}' to {data_type}: {e}")
            return value

    def _to_int(self, value: str) -> int:
        return int(value)

    def _to_float(self, value: str) -> float:
        return float(value)

    def _to_bool(self, value: str) -> bool:
        return str(value).lower() in ('true', '1', 'yes', 'on')

    def _to_date(self, value: str):
        if isinstance(value, str):
            return datetime.strptime(value, '%Y-%m-%d').date()
        return value

    def _to_datetime(self, value: str):
        if isinstance(value, str):
            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        return value


class ForeignKeyManager:
    """Handles foreign key relationships - separated for clarity"""

    def __init__(self, constraint_manager: OptimizedConstraintManager, converter: DataTypeConverter, logger: logging.Logger):
        self.constraint_manager = constraint_manager
        self.converter = converter
        self.logger = logger

    def get_fk_values(self, parent_table: str, parent_column: str,
                      generated_data: Dict[str, pd.DataFrame],
                      expected_data_type: str = None,
                      sample_size: int = None) -> List[Any]:
        """Get foreign key values with type conversion"""
        # Try constraint manager first
        available_values = self.constraint_manager.get_fk_values(parent_table, parent_column, sample_size)

        # Fallback to generated data
        if not available_values and parent_table in generated_data:
            df = generated_data[parent_table]
            if not df.empty and parent_column in df.columns:
                unique_values = df[parent_column].dropna().unique().tolist()
                self.constraint_manager.update_fk_pool(parent_table, parent_column, unique_values)
                available_values = unique_values

        # Convert to expected data type
        if expected_data_type and available_values:
            available_values = [
                self.converter.convert_to_type(str(value), expected_data_type)
                for value in available_values
            ]

        self.logger.debug(f"Retrieved {len(available_values)} FK values from {parent_table}.{parent_column}")
        return available_values

    def get_fk_values_with_relationship(self, fk_config: Dict, generated_data: Dict[str, pd.DataFrame],
                                        expected_data_type: str = None, record_count: int = 1,
                                        sample_size: int = None) -> List[Any]:
        """Get FK values considering relationship type"""
        parent_table = fk_config["parent_table"]
        parent_column = fk_config["parent_column"]

        available_values = self.get_fk_values(
            parent_table, parent_column, generated_data, expected_data_type, sample_size
        )

        if not available_values:
            self.logger.warning(f"No available values for FK {parent_table}.{parent_column}")
            return []

        return self.constraint_manager.get_fk_values_with_relationship(
            fk_config, available_values, record_count
        )


class BatchGenerator:
    """Handles batch generation logic - separated for clarity"""

    def __init__(self, value_generator: ValueGenerator, fk_manager: ForeignKeyManager,
                 constraint_manager: OptimizedConstraintManager, converter: DataTypeConverter, logger: logging.Logger):
        self.value_generator = value_generator
        self.fk_manager = fk_manager
        self.converter = converter
        self.constraint_manager = constraint_manager
        self.logger = logger

        # Track global record counts per table to avoid PK collisions
        self.table_record_counts = {}

    def generate_batch(self, table_metadata: Dict, batch_size: int,
                       generated_data: Dict[str, pd.DataFrame],
                       foreign_key_data: Dict = None) -> List[Dict]:
        """Generate a batch of records"""
        if foreign_key_data is None:
            foreign_key_data = {}

        table_name = table_metadata["table_name"]
        columns = table_metadata["columns"]
        foreign_keys = table_metadata.get("foreign_keys", [])

        # Get current global record count for this table
        current_count = self.table_record_counts.get(table_name, 0)

        # Prepare FK pools
        fk_pools, fk_distributions = self._prepare_fk_pools(
            foreign_keys, table_metadata, batch_size, generated_data, foreign_key_data
        )

        # Generate records with global indexing
        batch_data = []
        for record_idx in range(batch_size):
            # Use global record index instead of batch-local index
            global_record_idx = current_count + record_idx

            row = self._generate_single_record(
                global_record_idx, table_metadata, fk_pools, fk_distributions
            )
            batch_data.append(row)

        # Update global record count for this table
        self.table_record_counts[table_name] = current_count + batch_size

        # Apply batch-level constraints
        return self.constraint_manager.apply_length_constraints_to_batch(batch_data, table_metadata)

    def reset_table_counts(self, table_name: str = None):
        """Reset record counts for a specific table or all tables"""
        if table_name:
            self.table_record_counts.pop(table_name, None)
        else:
            self.table_record_counts.clear()

    def get_table_record_count(self, table_name: str) -> int:
        """Get current record count for a table"""
        return self.table_record_counts.get(table_name, 0)

    # Rest of the methods remain the same...
    def _prepare_fk_pools(self, foreign_keys: List[Dict], table_metadata: Dict,
                          batch_size: int, generated_data: Dict[str, pd.DataFrame],
                          foreign_key_data: Dict) -> tuple:
        """Prepare foreign key pools and distributions"""
        fk_pools = {}
        fk_distributions = {}

        for fk in foreign_keys:
            child_column = fk["child_column"]
            relationship_type = self.constraint_manager.get_relationship_type(fk)
            expected_type = self._get_column_data_type(table_metadata, child_column)

            if relationship_type in ["one_to_many", "one_to_one"]:
                # Pre-calculate distribution
                fk_values = self.fk_manager.get_fk_values_with_relationship(
                    fk, generated_data, expected_type, batch_size
                )
                fk_distributions[child_column] = fk_values
            else:
                # Use regular FK pool
                available_values = self.fk_manager.get_fk_values(
                    fk["parent_table"], fk["parent_column"], generated_data,
                    expected_type, sample_size=10000
                )

                # Add foreign_key_data if available
                fk_key = f"{fk['parent_table']}.{fk['parent_column']}"
                if fk_key in foreign_key_data:
                    additional_values = [
                        self.converter.convert_to_type(str(val), expected_type)
                        for val in foreign_key_data[fk_key]
                    ]
                    available_values.extend(additional_values)

                fk_pools[child_column] = available_values or self._generate_fallback_fk_values(
                    expected_type, batch_size
                )

        return fk_pools, fk_distributions

    def _generate_fallback_fk_values(self, expected_type: str, batch_size: int) -> List[Any]:
        """Generate fallback FK values when none are available"""
        fallback_values = []
        for i in range(min(1000, batch_size)):
            if expected_type == 'int':
                fallback_values.append(random.randint(1, 1000))
            else:
                fallback_values.append(f"default_{i}")
        return fallback_values

    def _generate_single_record(self, global_record_idx: int, table_metadata: Dict,
                                fk_pools: Dict, fk_distributions: Dict) -> Dict:
        """Generate a single record using global record index"""
        table_name = table_metadata["table_name"]
        columns = table_metadata["columns"]
        foreign_keys = table_metadata.get("foreign_keys", [])

        row = {}

        # Generate primary keys using global index
        self._generate_primary_keys(row, table_metadata, global_record_idx)

        # Generate foreign keys
        self._generate_foreign_keys(row, foreign_keys, fk_pools, fk_distributions, global_record_idx)

        # Generate other columns
        self._generate_regular_columns(row, columns, table_name)

        return row

    def _generate_primary_keys(self, row: Dict, table_metadata: Dict, global_record_idx: int):
        """Generate primary key values using global record index"""
        table_name = table_metadata["table_name"]
        columns = table_metadata["columns"]

        # Handle composite primary key
        composite_columns = table_metadata.get("composite_primary_key", [])
        if composite_columns:
            for i, col_name in enumerate(composite_columns):
                col_def = next((c for c in columns if c["name"] == col_name), None)
                if col_def:
                    pk_generator = self._create_pk_generator(col_def, table_name, global_record_idx + i)
                    row[col_name] = self.constraint_manager.generate_unique_pk_value(
                        table_name, col_name, pk_generator
                    )
        else:
            # Handle single primary key
            pk_columns = self.constraint_manager._get_primary_key_columns(table_metadata)
            if pk_columns:
                pk_col_name = pk_columns[0]
                pk_col_def = next((c for c in columns if c["name"] == pk_col_name), None)
                if pk_col_def:
                    pk_generator = self._create_pk_generator(pk_col_def, table_name, global_record_idx)
                    row[pk_col_name] = self.constraint_manager.generate_unique_pk_value(
                        table_name, pk_col_name, pk_generator
                    )

    def _create_pk_generator(self, col_def: Dict, table_name: str, global_index: int) -> Callable:
        """Create primary key generator function using global index"""

        def pk_generator():
            rule = col_def.get("rule")
            col_type = col_def.get("type", "").lower()

            # Special handling for sequential integer PKs (most common case)
            if col_type in ["int", "integer"] and (rule is None or not isinstance(rule, dict)):
                return global_index + 1

            # For all other cases, use the existing ValueGenerator logic
            try:
                generated_value = self.value_generator.generate_by_rule(rule, col_type, col_def['name'])

                # Ensure uniqueness for certain types by appending index
                if generated_value is not None:
                    # For string types, append index to ensure uniqueness if no specific rule handles it
                    if isinstance(generated_value, str) and not self._is_unique_by_nature(rule):
                        return f"{generated_value}_{global_index + 1}"
                    return generated_value

            except Exception as e:
                self.logger.warning(
                    f"Failed to generate PK using ValueGenerator for {table_name}.{col_def['name']}: {e}")

            # Fallback to simple sequential generation based on type
            return self._generate_fallback_pk(col_type, table_name, global_index)

        return pk_generator

    def _is_unique_by_nature(self, rule) -> bool:
        """Check if the rule generates naturally unique values"""
        if isinstance(rule, dict):
            rule_type = rule.get("type", "")
            # These rule types generate unique values by nature
            return rule_type in ["uuid", "sequence", "range"]
        elif isinstance(rule, str):
            # UUID faker methods generate unique values
            return "uuid" in rule.lower()
        return False

    def _generate_fallback_pk(self, col_type: str, table_name: str, global_index: int) -> Any:
        """Generate fallback PK value when all else fails"""
        fallbacks = {
            "int": lambda: global_index + 1,
            "integer": lambda: global_index + 1,
            "float": lambda: round((global_index + 1) * 1.0, 2),
            "double": lambda: round((global_index + 1) * 1.0, 2),
            "decimal": lambda: round((global_index + 1) * 1.0, 2),
            "bool": lambda: global_index % 2 == 0,
            "boolean": lambda: global_index % 2 == 0,
            "date": lambda: (datetime(2000, 1, 1) + timedelta(days=global_index)).date(),
            "datetime": lambda: datetime(2000, 1, 1) + timedelta(days=global_index),
            "timestamp": lambda: (datetime(2000, 1, 1) + timedelta(days=global_index)).strftime("%Y-%m-%d %H:%M:%S"),
        }

        fallback_generator = fallbacks.get(col_type.lower(),
                                           lambda: f"{table_name}_{global_index + 1}")
        return fallback_generator()

    def _generate_foreign_keys(self, row: Dict, foreign_keys: List[Dict],
                               fk_pools: Dict, fk_distributions: Dict, global_record_idx: int):
        """Generate foreign key values"""
        for fk in foreign_keys:
            child_column = fk["child_column"]

            if child_column in fk_distributions:
                # Use pre-calculated distribution
                if global_record_idx < len(fk_distributions[child_column]):
                    row[child_column] = fk_distributions[child_column][global_record_idx]
            elif child_column in fk_pools and fk_pools[child_column]:
                # Use random selection
                row[child_column] = random.choice(fk_pools[child_column])

            # Handle nullable constraint
            if fk.get("nullable", False):
                col_def = {"name": child_column, "nullable": True}
                if self.constraint_manager.should_generate_null(col_def):
                    row[child_column] = None

    def _generate_regular_columns(self, row: Dict, columns: List[Dict], table_name: str):
        """Generate regular column values"""
        for column in columns:
            column_name = column["name"]

            if column_name in row:
                continue

            data_type = column["type"]
            constraints = column.get("constraints", []) + column.get("constraint", [])

            # Handle unique constraints
            if "unique" in constraints:
                row[column_name] = self._generate_unique_value(column, table_name)
            else:
                # Apply conditional rules
                conditional_value = self._apply_conditional_rules(row, column)
                if conditional_value is not None:
                    row[column_name] = conditional_value
                else:
                    # Generate regular value
                    rule = column.get("rule")
                    row[column_name] = self.value_generator.generate_by_rule(rule, data_type, column_name)

            # Handle nullable columns
            if self.constraint_manager.should_generate_null(column):
                row[column_name] = None

            # Apply length constraints
            length_constraint = column.get("length")
            if length_constraint and row[column_name] is not None:
                rule = column.get("rule", {})
                row[column_name] = self.constraint_manager.adjust_value_for_length(
                    row[column_name], length_constraint, rule, data_type
                )

    def _generate_unique_value(self, column_def: Dict, table_name: str, max_attempts: int = 100):
        """Generate unique value for column"""
        column_name = column_def["name"]
        data_type = column_def["type"]
        rule = column_def.get("rule", {})

        def value_generator():
            return self.value_generator.generate_by_rule(rule, data_type, column_name)

        return self.constraint_manager.generate_unique_value(table_name, column_name, value_generator, max_attempts)

    def _apply_conditional_rules(self, row: Dict, column_def: Dict) -> Optional[Any]:
        """Apply conditional rules based on other column values"""
        conditional_rules = column_def.get("conditional_rules", [])

        for condition_rule in conditional_rules:
            when_conditions = condition_rule.get("when", [])
            then_rule = condition_rule.get("then", {}).get("rule", {})

            if self.constraint_manager.evaluate_conditions(when_conditions, row):
                if isinstance(then_rule, dict) and then_rule.get("type") in ["fixed", "default"]:
                    return then_rule.get("value")
                else:
                    base_rule = column_def.get("rule", {})
                    if isinstance(base_rule, dict) and isinstance(then_rule, dict):
                        merged_rule = {**base_rule, **then_rule}
                    else:
                        merged_rule = then_rule
                    return self.value_generator.generate_by_rule(merged_rule, column_def["type"], column_def["name"])

        return None

    def _get_column_data_type(self, table_metadata: Dict, column_name: str) -> str:
        """Get data type for a column"""
        columns = table_metadata.get('columns', [])
        for col in columns:
            if col.get('name') == column_name:
                return col.get('type', 'str')
        return 'str'


class DataGenerator:
    """Main Data Generator class - simplified and focused"""

    def __init__(self, config, locale=None, logger=None, ai_config=None):
        self.config = config
        self.logger = self._setup_logger(logger)

        # Initialize components
        self.faker = Faker(locale) if locale else Faker()
        self.validator = UnifiedValidator(logger=self.logger)

        performance_config = getattr(config, "performance_config", None)
        max_memory_mb = performance_config.get("max_memory_mb", 500) if performance_config else 500
        enable_parallel = getattr(performance_config, "enable_parallel", False) if performance_config else True
        self.constraint_manager = OptimizedConstraintManager(logger=self.logger, max_memory_mb=max_memory_mb, enable_parallel=enable_parallel)

        # Initialize helper classes
        self.converter = DataTypeConverter(self.logger)

        ai_config = getattr(config, 'ai', None) or ai_config
        self.value_generator = ValueGenerator(self.faker, self.logger, ai_config)
        self.fk_manager = ForeignKeyManager(self.constraint_manager, self.converter, self.logger)
        self.batch_generator = BatchGenerator(
            self.value_generator, self.fk_manager, self.constraint_manager, self.converter, self.logger
        )

        # Data storage
        self._generated_data = {}  # table_name -> DataFrame
        self._cache_size_limit = 50000
        self._batch_fk_refresh_threshold = 10000

    def _setup_logger(self, logger: Optional[logging.Logger]) -> logging.Logger:
        """Setup logger with fallback"""
        if logger is None:
            logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
            return logging.getLogger()
        return logger

    # Main generation methods
    def generate_batch_optimized(self, table_metadata: Dict, batch_size: int,
                                 foreign_key_data: Dict = None) -> List[Dict]:
        """Generate optimized batch of data"""
        return self.batch_generator.generate_batch(
            table_metadata, batch_size, self._generated_data, foreign_key_data
        )
    
    def generate_batch_iterator_optimized(self, table_metadata: Dict, batch_size: int, foreign_key_data: Dict = None) -> \
    Generator[list[dict], Any, None]:
        """Generate optimized batch of data"""
        yield self.batch_generator.generate_batch(
            table_metadata, batch_size, self._generated_data, foreign_key_data
        )

    def generate_value(self, rule: Any, data_type: str, column_name: str) -> Any:
        """Generate single value - delegates to ValueGenerator"""
        return self.value_generator.generate_by_rule(rule, data_type, column_name)

    def generate_value_with_distribution(self, rule: Any, data_type: str, column_name: str) -> Any:
        """Generate value with distribution - same as generate_value for simplicity"""
        return self.generate_value(rule, data_type, column_name)

    def generate_unique_value(self, column_def: Dict, table_name: str, max_attempts: int = 100):
        """Generate unique value"""
        return self.batch_generator._generate_unique_value(column_def, table_name, max_attempts)

    def apply_conditional_rules(self, row: Dict, column_def: Dict):
        """Apply conditional rules"""
        return self.batch_generator._apply_conditional_rules(row, column_def)

    # Data management methods
    def store_generated_batch(self, table_name: str, batch_data: List[Dict]):
        """Store generated batch efficiently"""
        if not batch_data:
            return

        df_batch = pd.DataFrame(batch_data)

        if table_name in self._generated_data:
            self._generated_data[table_name] = pd.concat([
                self._generated_data[table_name], df_batch
            ], ignore_index=True)
        else:
            self._generated_data[table_name] = df_batch

        # Periodic refresh for large datasets
        if len(self._generated_data[table_name]) % self._batch_fk_refresh_threshold == 0:
            self._refresh_fk_pools(table_name)

        self.logger.debug(f"Stored batch of {len(batch_data)} records for {table_name}. "
                          f"Total: {len(self._generated_data[table_name])}")

    def get_generated_data_df(self, table_name: str) -> pd.DataFrame:
        """Get generated data as DataFrame"""
        return self._generated_data.get(table_name, pd.DataFrame())

    def get_generated_data_dict(self, table_name: str) -> List[Dict[str, Any]]:
        """Get generated data as list of dictionaries"""
        df = self.get_generated_data_df(table_name)
        return df.to_dict('records') if not df.empty else []

    def clear_generated_data(self, table_name: str = None):
        """Clear generated data"""
        if table_name:
            if table_name in self._generated_data:
                del self._generated_data[table_name]
            self.constraint_manager.reset_table_constraints(table_name)
        else:
            self._generated_data.clear()
            self.constraint_manager.reset_all_constraints()

    # Utility methods
    def reset_constraint_tracking(self):
        """Reset all constraint tracking"""
        self.constraint_manager.reset_all_constraints()
        self._generated_data.clear()
        self.logger.info("Constraint tracking cache reset")

    def _refresh_fk_pools(self, table_name: str = None):
        """Refresh foreign key pools"""
        tables_to_refresh = [table_name] if table_name else list(self._generated_data.keys())

        df_map = {name: self._generated_data[name] for name in tables_to_refresh if name in self._generated_data}
        table_metadata_map = {name: self._get_table_metadata(name) for name in tables_to_refresh}

        self.constraint_manager.refresh_fk_pools_from_dataframe(df_map, table_metadata_map)

    def _get_table_metadata(self, table_name: str) -> Dict:
        """Get table metadata from config"""
        if isinstance(self.config, dict) and 'tables' in self.config:
            for table in self.config['tables']:
                if table.get('table_name') == table_name:
                    return table
        return {}

    def get_openai_cache_statistics(self) -> Dict:
        """Get OpenAI cache statistics"""
        return self.value_generator.get_cache_statistics()

    def clear_openai_cache(self, cache_key: str = None):
        """Clear OpenAI cache"""
        self.value_generator.clear_openai_cache(cache_key)

    def set_openai_cache_size(self, size: int):
        """Set default cache size for OpenAI generation"""
        self.value_generator._default_cache_size = max(10, min(size, 1000))  # Limit between 10-1000

    def get_constraint_statistics(self) -> Dict:
        """Get constraint statistics"""
        return self.constraint_manager.get_constraint_statistics()

    # Legacy method compatibility
    def random_date(self, start: str, end: str):
        """Generate random date between start and end - legacy compatibility"""
        return self.value_generator._generate_date_range({"start": start, "end": end})

    def generate_phone_matching_regex(self, regex_pattern: str) -> str:
        """Generate phone matching regex - legacy compatibility"""
        return self.value_generator._generate_phone_matching_regex(regex_pattern)

    def _convert_value_to_type(self, value: str, data_type: str) -> Any:
        """Convert value to type - legacy compatibility"""
        return self.converter.convert_to_type(value, data_type)

    def _generate_default_value(self, data_type: str) -> Any:
        """Generate default value - legacy compatibility"""
        return self.value_generator._generate_default_by_type(data_type)

    def _get_fk_values_optimized(self, parent_table: str, parent_column: str,
                                 expected_data_type: str = None, sample_size: int = None) -> List[Any]:
        """Get FK values optimized - legacy compatibility"""
        return self.fk_manager.get_fk_values(
            parent_table, parent_column, self._generated_data, expected_data_type, sample_size
        )

    def _get_fk_values_with_relationship(self, fk_config: Dict, expected_data_type: str = None,
                                         record_count: int = 1, sample_size: int = None) -> List[Any]:
        """Get FK values with relationship - legacy compatibility"""
        return self.fk_manager.get_fk_values_with_relationship(
            fk_config, self._generated_data, expected_data_type, record_count, sample_size
        )

    def _get_primary_key_columns(self, table_metadata: Dict) -> List[str]:
        """Get primary key columns - legacy compatibility"""
        return self.constraint_manager._get_primary_key_columns(table_metadata)

    def _get_column_data_type(self, table_metadata: Dict, column_name: str) -> str:
        """Get column data type - legacy compatibility"""
        return self.batch_generator._get_column_data_type(table_metadata, column_name)