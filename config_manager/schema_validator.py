import sys
import re
from typing import Dict, Any, Tuple, List, Optional, Set
import copy
from datetime import datetime

from pandas.io.stata import precision_loss_doc


class SchemaValidator:
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.suggestions = []
        self.critical_errors = []
        self.corrected_schema = None
        self.table_registry = {}

        # Cache for type compatibility to avoid repeated dictionary lookups
        self._type_compatibility_cache = None

        # Pre-compile regex patterns for better performance
        self._email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        self._phone_pattern = re.compile(r'^\+?[\d\s\-\(\)]{10,15}$')

        # Date format validation patterns
        self._date_format_patterns = {
            'DD': r'(?:0[1-9]|[12][0-9]|3[01])',
            'D': r'(?:[1-9]|[12][0-9]|3[01])',
            'MM': r'(?:0[1-9]|1[012])',
            'M': r'(?:[1-9]|1[012])',
            'YYYY': r'\d{4}',
            'YY': r'\d{2}',
            'MMMM': r'(?:January|February|March|April|May|June|July|August|September|October|November|December)',
            'MMM': r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)',
            'HH': r'(?:[01][0-9]|2[0-3])',
            'H': r'(?:[0-9]|1[0-9]|2[0-3])',
            'hh': r'(?:0[1-9]|1[012])',
            'h': r'(?:[1-9]|1[012])',
            'mm': r'(?:[0-5][0-9])',
            'm': r'(?:[0-9]|[1-5][0-9])',
            'ssssss': r'(?:[0-5][0-9])',
            'sssss': r'(?:[0-5][0-9])',
            'ssss': r'(?:[0-5][0-9])',
            'sss': r'(?:[0-5][0-9])',
            'ss': r'(?:[0-5][0-9])',
            's': r'(?:[0-9]|[1-5][0-9])',
            'A': r'(?:AM|PM)',
            'a': r'(?:am|pm)'
        }

        self._sql_datatype_mapping = {
    'varchar': 'str',
    'nvarchar': 'str',
    'nchar': 'str',
    'char': 'str',
    'text': 'str',
    'ntext': 'str',
    'string': 'str',
    'clob': 'str',
    'longtext': 'str',
    'mediumtext': 'str',
    'tinytext': 'str',
    'int': 'int',
    'integer': 'int',
    'bigint': 'int',
    'smallint': 'int',
    'tinyint': 'int',
    'mediumint': 'int',
    'serial': 'int',
    'bigserial': 'int',
    'float': 'float',
    'decimal': 'float',
    'double': 'float',
    'real': 'float',
    'numeric': 'float',
    'money': 'float',
    'boolean': 'bool',
    'bool': 'bool',
    'bit': 'bool',
    'date': 'str',
    'datetime': 'str',
    'timestamp': 'str',
    'time': 'str',
    'year': 'int',
    'uuid': 'str',
    'json': 'str',
    'jsonb': 'str',
    'xml': 'str',
    'binary': 'str',
    'varbinary': 'str',
    'blob': 'str',
    'longblob': 'str',
    'mediumblob': 'str',
    'tinyblob': 'str'
}

    def parse_sql_type(self, sql_type: str) ->Tuple[str, Optional[int], Optional[int]]:
        sql_type = sql_type.strip().lower()
        if sql_type in ['text', 'longtext', 'mediumtext', 'tinytext']:
            return sql_type, None, None
        pattern = r'([a-z]+)(?:\s*\(\s*(\d+)(?:\s*,\s*(\d+))?\s*\))?'
        match = re.match(pattern, sql_type)
        if not match:
            return sql_type, None, None

        base_type = match.group(1)
        length = int(match.group(2)) if match.group(2) else None
        precision = int(match.group(3)) if match.group(3) else None

        return base_type, length, precision

    def convert_sql_to_python_type(self, sql_type: str) -> str:
    """Convert SQL data type to Python data type"""
    try:
        base_type, length, precision = self.parse_sql_type(sql_type)
        python_type = self._sql_datatype_mapping.get(base_type.lower())
        
        if python_type is None:
            # If not found in mapping, try to infer from common patterns
            if 'int' in base_type.lower():
                python_type = 'int'
            elif any(keyword in base_type.lower() for keyword in ['char', 'text', 'string']):
                python_type = 'str'
            elif any(keyword in base_type.lower() for keyword in ['float', 'double', 'decimal', 'numeric']):
                python_type = 'float'
            elif 'bool' in base_type.lower() or 'bit' in base_type.lower():
                python_type = 'bool'
            elif any(keyword in base_type.lower() for keyword in ['date', 'time', 'timestamp']):
                python_type = 'str'
            else:
                python_type = 'str'  # Default fallback
        
        return python_type
    except ValueError:
        # If parsing fails, return the original type
        return sql_type

    def _validate_and_convert_column_type(self, column: Dict[str, Any], table_name: str, col_name: str, 
                                      table_index: int, col_index: int):
    """Validate and convert SQL types to Python types"""
    original_type = column.get('type')
    if not original_type:
        return
    
    original_type_str = str(original_type).strip()
    
    # Check if it's a SQL type that needs conversion
    try:
        base_type, length, precision = self.parse_sql_type(original_type_str)
        
        # If we can parse it as SQL type, convert to Python type
        if base_type.lower() in self._sql_datatype_mapping:
            python_type = self.convert_sql_to_python_type(original_type_str)
            
            if python_type != original_type_str:
                # Apply correction
                self._apply_correction(table_index, col_index, 'type', original_type_str, python_type, 
                                     f'converted_sql_type_{base_type}_to_python')
                
                # Store original SQL type info for reference
                if 'sql_type_info' not in column:
                    column['sql_type_info'] = {
                        'original_type': original_type_str,
                        'base_type': base_type,
                        'length': length,
                        'precision': precision
                    }
                
                self.warnings.append(f"Table '{table_name}', Column '{col_name}': "
                                   f"SQL type '{original_type_str}' converted to Python type '{python_type}'")
                
                # Handle length constraints from SQL type
                if length is not None and not column.get('length'):
                    self._apply_correction(table_index, col_index, 'length', None, length, 
                                         'extracted_from_sql_type')
                    self.suggestions.append(f"Table '{table_name}', Column '{col_name}': "
                                          f"Length constraint {length} extracted from SQL type")
                
    except ValueError:
        # Not a valid SQL type, check if it's a valid Python type
        valid_python_types = {'str', 'int', 'float', 'bool', 'string', 'integer', 'boolean', 'number'}
        if original_type_str.lower() not in valid_python_types:
            # Try to map common variations
            type_variations = {
                'string': 'str',
                'integer': 'int',
                'boolean': 'bool',
                'number': 'float',
                'numeric': 'float',
                'decimal': 'float',
                'double': 'float',
                'real': 'float'
            }
            
            converted_type = type_variations.get(original_type_str.lower())
            if converted_type:
                self._apply_correction(table_index, col_index, 'type', original_type_str, converted_type, 
                                     'normalized_python_type')
                self.warnings.append(f"Table '{table_name}', Column '{col_name}': "
                                   f"Type '{original_type_str}' normalized to '{converted_type}'")
            else:
                self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                                 f"Unknown type '{original_type_str}'. Using 'str' as fallback.")
                self._apply_correction(table_index, col_index, 'type', original_type_str, 'str', 
                                     'unknown_type_fallback')


    def validate_schema(self, schema: Dict[str, Any]) -> Tuple[
        List[str], List[str], List[str], List[str], Dict[str, Any]]:
        """Main validation method - now returns critical_errors as well"""
        self._reset_state()

        # Early validation to catch schema structure issues
        if not self._validate_schema_structure(schema):
            return self.errors, self.warnings, self.suggestions, self.critical_errors, None

        self.corrected_schema = copy.deepcopy(schema)

        self._validate_top_level(schema)
        self._build_table_registry(schema)
        self._validate_all_tables(schema)

        return self.errors, self.warnings, self.suggestions, self.critical_errors, self.corrected_schema

    def _validate_schema_structure(self, schema: Dict[str, Any]) -> bool:
        """Early validation of basic schema structure"""
        if not isinstance(schema, dict):
            self.critical_errors.append("Schema must be a dictionary")
            return False

        if 'tables' not in schema:
            self.critical_errors.append("Missing required top-level field: tables")
            return False

        if not isinstance(schema['tables'], list):
            self.critical_errors.append("'tables' field must be a list")
            return False

        return True

    def _reset_state(self):
        """Reset validation state"""
        self.errors.clear()
        self.warnings.clear()
        self.suggestions.clear()
        self.critical_errors.clear()
        self.table_registry.clear()

    def _validate_top_level(self, schema: Dict[str, Any]):
        """Validate top-level schema properties"""
        rows = schema.get('rows')
        if rows is not None and (not isinstance(rows, int) or rows <= 0):
            self.errors.append("'rows' must be a positive integer")

    def _build_table_registry(self, schema: Dict[str, Any]):
        """Build table registry for FK validation - optimized with early returns"""
        tables = schema.get('tables', [])

        for table in tables:
            table_name = table.get('table_name')
            if not table_name:
                continue

            columns = table.get('columns', [])
            if not columns:
                continue

            # Use dict comprehension for better performance
            column_dict = {
                col['name']: col
                for col in columns
                if col.get('name')
            }

            self.table_registry[table_name] = {
                'columns': column_dict,
                'table_def': table
            }

    def _validate_all_tables(self, schema: Dict[str, Any]):
        """Validate each table - optimized iteration"""
        total_rows = schema.get('rows', 0)

        for i, table in enumerate(schema.get('tables', [])):
            if not self._validate_table_structure(table, i):
                continue
            self._validate_table(table, total_rows, i)

    def _validate_table_structure(self, table: Dict[str, Any], table_index: int) -> bool:
        """Early validation of table structure"""
        table_name = table.get('table_name', f'Table_{table_index}')

        if not table.get('table_name'):
            self.critical_errors.append(f"Table {table_index}: Missing 'table_name' field")
            return False

        if 'columns' not in table:
            self.critical_errors.append(f"Table '{table_name}': Missing 'columns' field")
            return False

        if not isinstance(table['columns'], list):
            self.critical_errors.append(f"Table '{table_name}': 'columns' must be a list")
            return False

        return True

    def _validate_table(self, table: Dict[str, Any], total_rows: int, table_index: int):
        """Validate individual table structure"""
        table_name = table['table_name']

        self._validate_primary_keys(table, table_name)
        self._validate_all_columns(table, table_name, total_rows, table_index)
        self._validate_row_capacity_constraints(table, table_name, total_rows, table_index)

        foreign_keys = table.get('foreign_keys')
        if foreign_keys:
            self._validate_foreign_keys(foreign_keys, table_name)

    def _validate_row_capacity_constraints(self, table: Dict[str, Any], table_name: str,
                                           total_rows: int, table_index: int):
        """Validate that column constraints can accommodate the required number of rows"""
        if total_rows <= 0:
            return

        columns = table.get('columns', [])

        for col_index, column in enumerate(columns):
            if not isinstance(column, dict):
                continue

            col_name = column.get('name', f'Column_{col_index}')
            constraints = self._get_constraints(column)

            # Check if this is a unique constraint column (PK or unique)
            is_unique_constraint = 'PK' in constraints or 'unique' in constraints

            if is_unique_constraint:
                max_capacity = self._calculate_column_max_capacity(column, table_name, col_name)
                if max_capacity is not None and max_capacity < total_rows:
                    self._handle_insufficient_capacity(column, table_name, col_name, table_index,
                                                       col_index, max_capacity, total_rows)

    def _calculate_column_max_capacity(self, column: Dict[str, Any], table_name: str, col_name: str) -> Optional[int]:
        """Calculate maximum number of unique values this column can generate"""
        col_type = column.get('type', '').lower()
        rule = column.get('rule', {})
        length_constraint = column.get('length')

        # Handle rule-based capacity calculation
        if isinstance(rule, dict):
            rule_capacity = self._calculate_rule_capacity(rule, column)
            if rule_capacity is not None:
                return rule_capacity

        # Handle length-based capacity calculation
        if length_constraint is not None:
            length_capacity = self._calculate_length_capacity(length_constraint, col_type)
            if length_capacity is not None:
                return length_capacity

        # Handle type-based capacity (for types without specific constraints)
        return self._calculate_type_capacity(col_type)

    def _calculate_rule_capacity(self, rule: Dict[str, Any], column: Dict[str, Any]) -> Optional[int]:
        """Calculate capacity based on rule constraints"""
        rule_type = rule.get('type', '').lower()

        if rule_type == 'range':
            min_val = rule.get('min')
            max_val = rule.get('max')
            if min_val is not None and max_val is not None:
                return max_val - min_val + 1

        elif rule_type == 'choice':
            choices = rule.get('value', [])
            if isinstance(choices, list):
                return len(choices)

        elif rule_type == 'sequence':
            # For sequences, capacity depends on the data type limits
            start = rule.get('start', 1)
            step = rule.get('step', 1)

            # Calculate based on reasonable limits (e.g., 32-bit int max)
            max_int = 2 ** 31 - 1
            if step > 0:
                return max((max_int - start) // step + 1, 0)
            else:
                return None  # Infinite or undefined for negative steps

        return None

    def _calculate_length_capacity(self, length_constraint, col_type: str) -> Optional[int]:
        """Calculate capacity based on length constraints"""
        if isinstance(length_constraint, int):
            if length_constraint <= 0:
                return 0

            # For numeric types, calculate digit-based capacity
            if col_type in {'int', 'integer', 'number'}:
                if length_constraint == 1:
                    return 10  # 0-9
                else:
                    # For length n: 10^(n-1) to 10^n - 1
                    return 9 * (10 ** (length_constraint - 1))

            # For string types, calculate character-based capacity
            elif col_type in {'str', 'string', 'text'}:
                # Assuming ASCII characters (95 printable characters)
                return 95 ** length_constraint

        elif isinstance(length_constraint, dict):
            min_len = length_constraint.get('min', 1)
            max_len = length_constraint.get('max')

            if max_len is not None:
                return self._calculate_length_capacity(max_len, col_type)

        return None

    def _calculate_type_capacity(self, col_type: str) -> Optional[int]:
        """Calculate capacity based on data type limits"""
        type_capacities = {
            'bool': 2,
            'boolean': 2,
            'tinyint': 256,  # -128 to 127 or 0 to 255
            'smallint': 65536,  # 2^16
            'int': 2 ** 31,  # 2^31 for signed int
            'integer': 2 ** 31,
            'bigint': 2 ** 63,
            'float': None,  # Effectively unlimited for practical purposes
            'double': None,
            'decimal': None,
            'str': None,  # Effectively unlimited
            'string': None,
            'text': None,
            'date': None,  # Large range
            'datetime': None,
            'timestamp': None,
            'uuid': None  # 2^128 - effectively unlimited
        }

        return type_capacities.get(col_type.lower())

    def _handle_insufficient_capacity(self, column: Dict[str, Any], table_name: str, col_name: str,
                                      table_index: int, col_index: int, max_capacity: int, required_rows: int):
        """Handle cases where column capacity is insufficient for required rows"""

        shortage = required_rows - max_capacity

        # Add to critical errors - this prevents schema from being processed
        self.critical_errors.append(
            f"Table '{table_name}', Column '{col_name}': CRITICAL - Insufficient capacity for unique constraint. "
            f"Can generate {max_capacity:,} unique values but need {required_rows:,} rows "
            f"(shortage: {shortage:,}). Schema cannot be processed until this is resolved."
        )

        # Suggest corrections based on column configuration
        suggestions = self._generate_capacity_correction_suggestions(column, table_name, col_name,
                                                                     max_capacity, required_rows)
        # Keep as critical error and add suggestions
        self.suggestions.extend([f"Table '{table_name}', Column '{col_name}': {suggestion}"
                                 for suggestion in suggestions])

        # Add additional critical guidance
        self.critical_errors.append(
            f"Table '{table_name}', Column '{col_name}': REQUIRED ACTION - "
            f"Must implement one of the suggested corrections before schema can be used for data generation."
        )

    def _generate_capacity_correction_suggestions(self, column: Dict[str, Any], table_name: str, col_name: str,
                                                  current_capacity: int, required_capacity: int) -> List[str]:
        """Generate suggestions for fixing capacity issues"""
        suggestions = []
        col_type = column.get('type', '').lower()
        rule = column.get('rule', {})
        length_constraint = column.get('length')

        # Priority 1: Suggest converting length to rule for numeric types
        if isinstance(length_constraint, int) and col_type in {'int', 'integer', 'number'}:
            # Calculate optimal starting value to accommodate required capacity
            # For length=8 with 100M rows, suggest starting from 10000000 (8 digits)
            min_start_value = 10 ** (length_constraint - 1)  # e.g., 10^7 = 10,000,000 for length 8
            max_end_value = (10 ** length_constraint) - 1  # e.g., 10^8 - 1 = 99,999,999 for length 8

            available_range = max_end_value - min_start_value + 1

            if available_range >= required_capacity:
                # Current length can accommodate if we use rule instead
                suggested_max = min_start_value + required_capacity - 1
                suggestions.append(
                    f"Replace length {length_constraint} with rule: "
                    f"{{\"type\": \"range\", \"min\": {min_start_value}, \"max\": {suggested_max}}} "
                    f"(provides {required_capacity:,} unique values)"
                )
            else:
                # Need to increase length and use rule
                required_digits = len(str(required_capacity + min_start_value - 1))
                new_min_start = 10 ** (required_digits - 1)
                new_max_end = new_min_start + required_capacity - 1
                suggestions.append(
                    f"Replace length {length_constraint} with rule: "
                    f"{{\"type\": \"range\", \"min\": {new_min_start}, \"max\": {new_max_end}}} "
                    f"and remove length constraint (provides {required_capacity:,} unique values)"
                )

        # Priority 2: Adjust existing rule
        elif isinstance(rule, dict):
            rule_type = rule.get('type', '').lower()

            if rule_type == 'range':
                current_min = rule.get('min', 0)
                current_max = rule.get('max', current_capacity - 1)
                new_max = current_min + required_capacity - 1
                suggestions.append(f"Increase range max from {current_max:,} to {new_max:,}")

            elif rule_type == 'choice':
                current_choices = len(rule.get('value', []))
                needed_choices = required_capacity - current_choices
                suggestions.append(f"Add {needed_choices:,} more choices to choice list")

        # Priority 3: Suggest length adjustments (fallback)
        elif isinstance(length_constraint, int) and col_type in {'int', 'integer', 'number'}:
            required_digits = len(str(required_capacity))
            if required_digits > length_constraint:
                suggestions.append(f"Increase length from {length_constraint} to {required_digits}")

        # Priority 4: Suggest type changes for overflow
        if col_type in {'int', 'integer'} and required_capacity > 2 ** 31:
            suggestions.append("Change type from 'int' to 'bigint' for larger capacity")

        # Last resort: Suggest removing unique constraint
        constraints = self._get_constraints(column)
        if 'unique' in constraints and 'PK' not in constraints:
            suggestions.append("Consider removing 'unique' constraint if duplicates are acceptable")

        return suggestions

    def _validate_primary_keys(self, table: Dict[str, Any], table_name: str):
        """Validate primary key constraints - optimized counting"""
        pk_count = 0
        for col in table.get('columns', []):
            constraints = self._get_constraints(col)
            if 'PK' in constraints:
                pk_count += 1
                if pk_count > 1:  # Early exit on multiple PKs
                    self.errors.append(f"Table '{table_name}': Multiple primary keys defined")
                    return

        if pk_count == 0:
            self.warnings.append(f"Table '{table_name}': No primary key defined")

    def _validate_all_columns(self, table: Dict[str, Any], table_name: str, total_rows: int, table_index: int):
        """Validate all columns in a table"""
        columns = table.get('columns', [])

        for col_index, column in enumerate(columns):
            if not self._validate_column_structure(column, table_name, col_index):
                continue
            self._validate_column(column, table_name, total_rows, table_index, col_index)

    def _validate_column_structure(self, column: Dict[str, Any], table_name: str, col_index: int) -> bool:
        """Early validation of column structure"""
        if not isinstance(column, dict):
            self.critical_errors.append(f"Table '{table_name}': Column {col_index} must be a dictionary")
            return False

        col_name = column.get('name')
        if not col_name:
            self.critical_errors.append(f"Table '{table_name}': Column {col_index} missing 'name' field")
            return False

        if not column.get('type'):
            self.critical_errors.append(f"Table '{table_name}', Column '{col_name}': Missing 'type' field")
            return False

        return True

    def _validate_column(self, column: Dict[str, Any], table_name: str, total_rows: int,
                         table_index: int, col_index: int):
        """Main column validation - optimized with early extractions"""
        col_name = column['name']
        self._validate_and_convert_column_type(column, table_name, col_name, table_index, col_index)
    
        col_type = column['type']
        constraints = self._get_constraints(column)
        rule = column.get('rule')
        length_constraint = column.get('length')

        # Validate and convert length constraint if needed
        self._validate_and_convert_length_constraint(column, table_name, col_name, table_index, col_index,
                                                     length_constraint)

        # Constraint-based validation
        constraint_info = self._analyze_constraints(constraints, column)
        self._handle_nullable_logic(column, table_name, col_name, table_index, col_index, constraint_info)

        # Rule-based validation
        if rule and isinstance(rule, dict):
            self._validate_rule(column, table_name, col_name, table_index, col_index, rule, length_constraint)

        # Type-rule consistency
        self._validate_type_rule_consistency(column, table_name, col_name, table_index, col_index)

    def _validate_and_convert_length_constraint(self, column: Dict[str, Any], table_name: str, col_name: str,
                                                table_index: int, col_index: int, length_constraint):
        """Validate and convert length constraint - convert int > 1 to rule field for numeric types"""
        if length_constraint is None:
            return

        if isinstance(length_constraint, int):
            if length_constraint > 1:
                # Check if this column type should use digit range conversion
                if self._should_convert_to_digit_range(column):
                    # Check if rule already exists and has appropriate range
                    existing_rule = column.get('rule', {})

                    # Skip conversion if rule already exists with range type and min/max values
                    if (isinstance(existing_rule, dict) and
                            existing_rule.get('type') == 'range' and
                            'min' in existing_rule and 'max' in existing_rule):
                        # Rule already exists with range - just validate consistency
                        self._validate_length_rule_consistency(column, table_name, col_name,
                                                               table_index, col_index, length_constraint, existing_rule)
                        return

                    # Convert integer length > 1 to min/max range in rule field
                    # For length n: min = 10^(n-1), max = 10^n - 1
                    min_val = 10 ** (length_constraint - 1)
                    max_val = (10 ** length_constraint) - 1

                    # Create or update the rule field
                    if isinstance(existing_rule, str):
                        # If rule is a string, convert to dict
                        new_rule = {
                            'type': 'range',
                            'min': min_val,
                            'max': max_val
                        }
                    elif isinstance(existing_rule, dict):
                        new_rule = existing_rule.copy()
                        new_rule.update({
                            'type': 'range',
                            'min': min_val,
                            'max': max_val
                        })
                    else:
                        # Create new rule dict
                        new_rule = {
                            'type': 'range',
                            'min': min_val,
                            'max': max_val
                        }

                    # Apply corrections to both rule and length fields
                    self._apply_correction(table_index, col_index, 'rule', existing_rule, new_rule,
                                           'converted_length_to_rule_range')
                    self._apply_correction(table_index, col_index, 'length', length_constraint, None,
                                           'removed_after_rule_conversion')

                    self.warnings.append(f"Table '{table_name}', Column '{col_name}': "
                                         f"Length {length_constraint} converted to rule range {{\"min\": {min_val}, \"max\": {max_val}}} and length removed")
            elif length_constraint == 1:
                # Length 1 stays as fixed length for single digits (0-9) or single character
                pass  # Keep as is
            elif length_constraint <= 0:
                self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                                   f"Length must be positive, got {length_constraint}")
                # Correct to a default positive value
                self._apply_correction(table_index, col_index, 'length', length_constraint, 1,
                                       'corrected_invalid_length')

        elif isinstance(length_constraint, dict):
            # Validate existing min/max dictionary
            self._validate_length_range_dict(column, table_name, col_name, table_index, col_index, length_constraint)

        else:
            # Invalid length constraint type
            self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                               f"Length constraint must be integer or dictionary with min/max, got {type(length_constraint).__name__}")
            # Remove invalid constraint
            self._apply_correction(table_index, col_index, 'length', length_constraint, None,
                                   'removed_invalid_length_type')

    def _validate_length_rule_consistency(self, column: Dict[str, Any], table_name: str, col_name: str,
                                          table_index: int, col_index: int, length_constraint: int,
                                          rule: Dict[str, Any]):
        """Validate consistency between existing length constraint and rule range"""
        rule_min = rule.get('min')
        rule_max = rule.get('max')

        if rule_min is None or rule_max is None:
            return

        # Calculate expected length range from rule
        min_digits = len(str(rule_min))
        max_digits = len(str(rule_max))

        # Check if length constraint is consistent with rule range
        if length_constraint < min_digits:
            self.warnings.append(f"Table '{table_name}', Column '{col_name}': "
                                 f"Length {length_constraint} is less than minimum digits ({min_digits}) "
                                 f"required for rule range {rule_min}-{rule_max}")
            # Suggest removing the conflicting length constraint
            self._apply_correction(table_index, col_index, 'length', length_constraint, None,
                                   'removed_conflicting_with_rule_range')
        elif length_constraint > max_digits:
            self.suggestions.append(f"Table '{table_name}', Column '{col_name}': "
                                    f"Length {length_constraint} is greater than maximum digits ({max_digits}) "
                                    f"needed for rule range {rule_min}-{rule_max}")
        else:
            # Length is within acceptable range - just remove it since rule is more specific
            self.suggestions.append(f"Table '{table_name}', Column '{col_name}': "
                                    f"Removing redundant length constraint {length_constraint} "
                                    f"since rule range {rule_min}-{rule_max} is more specific")
            self._apply_correction(table_index, col_index, 'length', length_constraint, None,
                                   'removed_redundant_with_rule_range')

    def _should_convert_to_digit_range(self, column: Dict[str, Any]) -> bool:
        """Determine if column should have length converted to digit range based on type and rule"""
        col_type = column.get('type', '').lower()
        rule = column.get('rule', {})

        # Don't convert for string types that represent fixed-format data
        string_types_with_fixed_formats = {
            'uuid', 'str', 'string', 'text', 'varchar', 'char'
        }

        # Don't convert if rule indicates fixed format
        if isinstance(rule, dict):
            rule_type = rule.get('type', '').lower()
            # Fixed format rules that should maintain exact character length
            fixed_format_rules = {
                'uuid', 'email', 'phone_number', 'url', 'date', 'choice'
            }
            if rule_type in fixed_format_rules:
                return False

            # If rule has prefix/suffix, it's likely a formatted string
            if rule.get('prefix') or rule.get('suffix'):
                return False

        elif isinstance(rule, str):
            # String rules that indicate fixed formats
            fixed_format_string_rules = {
                'uuid', 'email', 'phone', 'url', 'address', 'city', 'text',
                'first_name', 'last_name'
            }
            if rule.lower() in fixed_format_string_rules:
                return False

        # Convert to digit range only for numeric types without fixed format rules
        numeric_types = {'int', 'integer', 'number'}
        if col_type in numeric_types:
            return True

        # For other types, don't convert
        return False

    def _validate_length_range_dict(self, column: Dict[str, Any], table_name: str, col_name: str,
                                    table_index: int, col_index: int, length_constraint: Dict):
        """Validate length range dictionary structure"""
        min_val = length_constraint.get('min')
        max_val = length_constraint.get('max')

        # Check if both min and max are present
        if min_val is None and max_val is None:
            self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                               f"Length range must have at least 'min' or 'max' key")
            self._apply_correction(table_index, col_index, 'length', length_constraint, None,
                                   'removed_empty_length_range')
            return

        # Validate min value
        if min_val is not None:
            if not isinstance(min_val, int) or min_val <= 0:
                self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                                   f"Length 'min' must be positive integer, got {min_val}")
                # Correct min to 1
                new_constraint = length_constraint.copy()
                new_constraint['min'] = 1
                self._apply_correction(table_index, col_index, 'length', length_constraint, new_constraint,
                                       'corrected_invalid_min_length')

        # Validate max value
        if max_val is not None:
            if not isinstance(max_val, int) or max_val <= 0:
                self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                                   f"Length 'max' must be positive integer, got {max_val}")
                # Correct max or remove if min exists
                new_constraint = length_constraint.copy()
                if min_val is not None and min_val > 0:
                    new_constraint['max'] = max(min_val, 10)  # Set reasonable max
                else:
                    new_constraint['max'] = 10  # Default max
                self._apply_correction(table_index, col_index, 'length', length_constraint, new_constraint,
                                       'corrected_invalid_max_length')

        # Validate min <= max relationship
        if (min_val is not None and max_val is not None and
                isinstance(min_val, int) and isinstance(max_val, int) and
                min_val > max_val):
            self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                               f"Length min ({min_val}) > max ({max_val})")
            # Swap values to fix the relationship
            new_constraint = {
                'min': max_val,
                'max': min_val
            }
            self._apply_correction(table_index, col_index, 'length', length_constraint, new_constraint,
                                   'swapped_min_max_length_values')
            self.warnings.append(f"Table '{table_name}', Column '{col_name}': "
                                 f"Swapped min/max values to fix constraint")

    def _get_constraints(self, column: Dict[str, Any]) -> List[str]:
        """Get constraints as a normalized list - optimized"""
        constraints = column.get('constraints') or column.get('constraint', [])

        if isinstance(constraints, str):
            return [constraints]
        elif isinstance(constraints, list):
            return constraints
        else:
            return []

    def _analyze_constraints(self, constraints: List[str], column: Dict[str, Any]) -> Dict[str, bool]:
        """Analyze constraint implications - using set for faster lookups"""
        constraint_set = set(constraints)
        return {
            'is_primary_key': 'PK' in constraint_set,
            'is_unique': 'unique' in constraint_set,
            'has_default': 'default' in column
        }

    def _validate_rule(self, column: Dict[str, Any], table_name: str, col_name: str,
                       table_index: int, col_index: int, rule: Dict[str, Any], length_constraint):
        """Validate rule structure and handle special cases"""
        rule_type = rule.get('type')
        if not rule_type:
            self.errors.append(f"Table '{table_name}', Column '{col_name}': Rule missing 'type' field")
            return

        # Basic rule structure validation
        self._validate_basic_rule_structure(rule, table_name, col_name)

        # NEW: Validate date/time format fields
        self._validate_date_time_format(rule, table_name, col_name, table_index, col_index)

        # Special rule type handling - optimized with dict dispatch
        rule_handlers = {
            'choice': self._handle_choice_rule,
            'range': self._handle_range_rule_for_pk,
            'sequence': self._handle_sequence_rule_for_pk,
            'date_range': self._handle_date_range_rule,
            'time_range': self._handle_time_range_rule,
            'timestamp_range': self._handle_timestamp_range_rule
        }

        handler = rule_handlers.get(rule_type)
        if handler:
            if rule_type == 'choice':
                handler(column, table_name, col_name, table_index, col_index, rule, length_constraint)
            elif rule_type in ['date_range', 'time_range', 'timestamp_range']:
                handler(column, table_name, col_name, table_index, col_index, rule)
            elif self._analyze_constraints(self._get_constraints(column), column)['is_primary_key']:
                handler(column, table_name, col_name, table_index, col_index, rule, length_constraint)

    def _validate_date_time_format(self, rule: Dict[str, Any], table_name: str, col_name: str,
                                   table_index: int, col_index: int):
        """Validate and convert date/time format field from human-readable to Python format"""
        rule_type = rule.get('type', '').lower()
        format_field = rule.get('format')

        if format_field is None:
            return  # Format is optional

        if not isinstance(format_field, str):
            self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                               f"Format field must be a string, got {type(format_field).__name__}")
            # Remove invalid format
            new_rule = {k: v for k, v in rule.items() if k != 'format'}
            self._apply_correction(table_index, col_index, 'rule', rule, new_rule,
                                   'removed_invalid_format_type')
            return

        # Convert human-readable format to Python strftime format
        if rule_type in ['date', 'date_range', 'time', 'time_range', 'timestamp', 'timestamp_range']:
            converted_format = self._convert_human_format_to_python(format_field, rule_type)

            if converted_format != format_field:
                # Update the rule with converted format
                new_rule = rule.copy()
                new_rule['format'] = converted_format
                self._apply_correction(table_index, col_index, 'rule', rule, new_rule,
                                       'converted_human_format_to_python')
                self._apply_correction(table_index, col_index, 'type', rule_type, 'str',
                                       'custom_format_for_data')
                self.warnings.append(f"Table '{table_name}', Column '{col_name}': "
                                     f"Format converted from '{format_field}' to '{converted_format}'")

    def _convert_human_format_to_python(self, format_string: str, rule_type: str) -> str:
        """Convert human-readable format patterns to Python strftime patterns"""
        # First, auto-correct common mistakes
        corrected_format = self._auto_correct_format_mistakes(format_string)

        # Simple and safe replacement - only use non-conflicting patterns
        python_format = corrected_format

        # Only include patterns that won't cause conflicts
        # Exclude single-character patterns that could match within already-converted patterns
        replacements = [
            # Date patterns - multi-character only
            ('MMMM', '%B'),  # Full month name
            ('MMM', '%b'),  # Abbreviated month name
            ('YYYY', '%Y'),  # 4-digit year
            ('dddd', '%A'),  # Full day name
            ('ddd', '%a'),  # Abbreviated day name
            ('DD', '%d'),  # Day with leading zero
            ('MM', '%m'),  # Month with leading zero
            ('YY', '%y'),  # 2-digit year

            # Time patterns - multi-character only
            ('HH', '%H'),  # Hour 24-format with leading zero
            ('hh', '%I'),  # Hour 12-format with leading zero
            ('mm', '%M'),  # Minute with leading zero
            ('ssssss', '%f'),  # MiliSecond with leading zero
            ('sssss', '%f'),  # MiliSecond with leading zero
            ('ssss', '%f'),  # MiliSecond with leading zero
            ('ss', '%S'),  # Second with leading zero

            # AM/PM patterns
            ('AM', '%p'),  # AM/PM uppercase
            ('PM', '%p'),  # AM/PM uppercase
            ('am', '%p'),  # AM/PM lowercase
            ('pm', '%p'),  # AM/PM lowercase
            ('A', '%p'),  # AM/PM single char (safe as it's not in converted patterns)
            ('a', '%p'),  # AM/PM single char (safe as it's not in converted patterns)
        ]

        # Apply replacements one by one
        for human_pattern, python_pattern in replacements:
            python_format = python_format.replace(human_pattern, python_pattern)

        # Validate the resulting Python format
        self._validate_python_format(python_format, rule_type, format_string)

        return python_format

    def _auto_correct_format_mistakes(self, format_string: str) -> str:
        """Auto-correct common format mistakes"""
        # Common format mistakes and their corrections
        corrections = {
            # Case corrections
            'dd': 'DD',
            'yyyy': 'YYYY',
            'yy': 'YY',

            # Word-based corrections
            'min': 'mm',  # minutes
            'minute': 'mm',
            'minutes': 'mm',
            'sec': 'ss',  # seconds
            'second': 'ss',
            'seconds': 'ss',
            'miliseconds': 'sss',
            'milisecond': 'sss',
            'microseconds': 'ssssss',
            'microsecond': 'ssssss',
            'am': 'A',
            'pm': 'A',
            'AM': 'A',
            'PM': 'A',
            'month': 'MMMM',
            'mon': 'MMM',
            'day': 'DD',
            'year': 'YYYY'
        }

        corrected = format_string

        # Apply corrections word by word to avoid partial matches
        import re

        # Split by non-letter characters to preserve separators
        parts = re.split(r'([^A-Za-z]+)', corrected)

        for i, part in enumerate(parts):
            if part in corrections:
                parts[i] = corrections[part]

        corrected = ''.join(parts)

        return corrected

    def _validate_python_format(self, python_format: str, rule_type: str, original_format: str):
        """Validate the converted Python format string"""
        try:
            # Test the format with a sample datetime
            test_datetime = datetime(2023, 12, 25, 14, 30, 45)

            if rule_type in ['date', 'date_range']:
                test_datetime.date().strftime(python_format)
            elif rule_type in ['time', 'time_range']:
                test_datetime.strftime(python_format)
            elif rule_type in ['timestamp', 'timestamp_range']:
                test_datetime.strftime(python_format)

        except ValueError as e:
            self.warnings.append(f"Converted format '{python_format}' (from '{original_format}') "
                                 f"may be invalid: {str(e)}")

    def _validate_date_format_pattern(self, format_pattern: str, table_name: str, col_name: str,
                                      table_index: int, col_index: int, rule: Dict[str, Any]):
        """Simplified validation - just check if format is reasonable"""
        # Basic validation - check for obviously invalid patterns
        if not format_pattern.strip():
            self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                               f"Date format cannot be empty")
            return

        # Check for some common date components
        has_day = any(pattern in format_pattern for pattern in ['DD', 'D', 'dd', 'd'])
        has_month = any(pattern in format_pattern for pattern in ['MM', 'M', 'mm', 'MMMM', 'MMM', 'month', 'mon'])
        has_year = any(pattern in format_pattern for pattern in ['YYYY', 'YY', 'yyyy', 'yy', 'year'])

        missing_components = []
        if not has_day:
            missing_components.append('day')
        if not has_month:
            missing_components.append('month')
        if not has_year:
            missing_components.append('year')

        if missing_components:
            self.suggestions.append(f"Table '{table_name}', Column '{col_name}': "
                                    f"Date format '{format_pattern}' appears to be missing: {', '.join(missing_components)}. "
                                    f"Consider formats like 'DD/MM/YYYY', 'MM/DD/YYYY', or 'YYYY-MM-DD'")

    def _validate_time_format_pattern(self, format_pattern: str, table_name: str, col_name: str,
                                      table_index: int, col_index: int, rule: Dict[str, Any]):
        """Simplified validation - just check if format is reasonable"""
        if not format_pattern.strip():
            self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                               f"Time format cannot be empty")
            return

        # Check for basic time components
        has_hour = any(pattern in format_pattern for pattern in ['HH', 'H', 'hh', 'h'])
        has_minute = any(pattern in format_pattern for pattern in ['mm', 'm', 'min'])
        has_ampm = any(pattern in format_pattern for pattern in ['A', 'a', 'AM', 'PM', 'am', 'pm'])

        # Check for 12/24 hour consistency
        has_24h = any(pattern in format_pattern for pattern in ['HH', 'H'])
        has_12h = any(pattern in format_pattern for pattern in ['hh', 'h'])

        if has_24h and has_ampm:
            self.warnings.append(f"Table '{table_name}', Column '{col_name}': "
                                 f"Inconsistent time format: 24-hour format with AM/PM. "
                                 f"Consider using 'hh' or 'h' for 12-hour format")

        if not has_hour:
            self.suggestions.append(f"Table '{table_name}', Column '{col_name}': "
                                    f"Time format missing hour component")
        if not has_minute:
            self.suggestions.append(f"Table '{table_name}', Column '{col_name}': "
                                    f"Time format missing minute component")

    def _validate_timestamp_format_pattern(self, format_pattern: str, table_name: str, col_name: str,
                                           table_index: int, col_index: int, rule: Dict[str, Any]):
        """Simplified validation - just check if format has both date and time parts"""
        if not format_pattern.strip():
            self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                               f"Timestamp format cannot be empty")
            return

        # Check for both date and time components
        has_date_component = any(pattern in format_pattern for pattern in
                                 ['DD', 'D', 'MM', 'M', 'YYYY', 'YY', 'dd', 'mm', 'yyyy', 'yy'])
        has_time_component = any(pattern in format_pattern for pattern in
                                 ['HH', 'H', 'hh', 'h', 'mm', 'm', 'ssssss', 'sssss', 'ssss','sss', 'ss', 's'])

        if not has_date_component:
            self.warnings.append(f"Table '{table_name}', Column '{col_name}': "
                                 f"Timestamp format appears to be missing date components")
        if not has_time_component:
            self.warnings.append(f"Table '{table_name}', Column '{col_name}': "
                                 f"Timestamp format appears to be missing time components")

    def _suggest_format_corrections(self, invalid_token: str, valid_patterns: List[str]) -> List[str]:
        """Suggest corrections for invalid format tokens - simplified"""
        suggestions = []
        invalid_lower = invalid_token.lower()

        # Basic common corrections
        basic_corrections = {
            'dd': 'DD', 'd': 'DD',
            'mm': 'MM', 'm': 'MM',
            'yyyy': 'YYYY', 'yy': 'YY',
            'hh': 'HH', 'h': 'HH'
        }

        if invalid_lower in basic_corrections:
            suggestions.append(basic_corrections[invalid_lower])

        return suggestions[:2]  # Limit to 2 suggestions

    def _handle_date_range_rule(self, column: Dict[str, Any], table_name: str, col_name: str,
                                table_index: int, col_index: int, rule: Dict[str, Any]):
        """Handle date_range rule validation"""
        start_date = rule.get('start')
        end_date = rule.get('end')

        # Validate date format in start/end
        if start_date:
            if not self._is_valid_date_string(start_date):
                self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                                   f"Invalid start date format '{start_date}'. Expected YYYY-MM-DD")

        if end_date:
            if not self._is_valid_date_string(end_date):
                self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                                   f"Invalid end date format '{end_date}'. Expected YYYY-MM-DD")

        # Validate date range logic
        if start_date and end_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                if start_dt > end_dt:
                    self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                                       f"Start date '{start_date}' is after end date '{end_date}'")
            except ValueError as e:
                self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                                   f"Date parsing error: {str(e)}")

    def _handle_time_range_rule(self, column: Dict[str, Any], table_name: str, col_name: str,
                                table_index: int, col_index: int, rule: Dict[str, Any]):
        """Handle time_range rule validation"""
        start_time = rule.get('start')
        end_time = rule.get('end')

        # Validate time format in start/end
        if start_time:
            if not self._is_valid_time_string(start_time):
                self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                                   f"Invalid start time format '{start_time}'. Expected HH:MM:SS")

        if end_time:
            if not self._is_valid_time_string(end_time):
                self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                                   f"Invalid end time format '{end_time}'. Expected HH:MM:SS")

    def _handle_timestamp_range_rule(self, column: Dict[str, Any], table_name: str, col_name: str,
                                     table_index: int, col_index: int, rule: Dict[str, Any]):
        """Handle timestamp_range rule validation"""
        start_timestamp = rule.get('start')
        end_timestamp = rule.get('end')

        # Validate timestamp format in start/end
        if start_timestamp:
            if not self._is_valid_timestamp_string(start_timestamp):
                self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                                   f"Invalid start timestamp format '{start_timestamp}'. Expected YYYY-MM-DD HH:MM:SS")

        if end_timestamp:
            if not self._is_valid_timestamp_string(end_timestamp):
                self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                                   f"Invalid end timestamp format '{end_timestamp}'. Expected YYYY-MM-DD HH:MM:SS")

        # Validate timestamp range logic
        if start_timestamp and end_timestamp:
            try:
                start_dt = datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S')
                end_dt = datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S')
                if start_dt > end_dt:
                    self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                                       f"Start timestamp '{start_timestamp}' is after end timestamp '{end_timestamp}'")
            except ValueError as e:
                self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                                   f"Timestamp parsing error: {str(e)}")

    def _is_valid_date_string(self, date_str: str) -> bool:
        """Check if string is valid YYYY-MM-DD format"""
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False

    def _is_valid_time_string(self, time_str: str) -> bool:
        """Check if string is valid HH:MM:SS format"""
        try:
            datetime.strptime(time_str, '%H:%M:%S')
            return True
        except ValueError:
            return False

    def _is_valid_timestamp_string(self, timestamp_str: str) -> bool:
        """Check if string is valid YYYY-MM-DD HH:MM:SS format"""
        try:
            datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            return True
        except ValueError:
            return False

    def _handle_range_rule_for_pk(self, column: Dict[str, Any], table_name: str, col_name: str,
                                  table_index: int, col_index: int, rule: Dict[str, Any], length_constraint):
        """Handle range rule for primary key"""
        self._handle_primary_key_with_rule(column, table_name, col_name, table_index, col_index, rule,
                                           length_constraint)

    def _handle_sequence_rule_for_pk(self, column: Dict[str, Any], table_name: str, col_name: str,
                                     table_index: int, col_index: int, rule: Dict[str, Any], length_constraint):
        """Handle sequence rule for primary key"""
        self._handle_primary_key_with_rule(column, table_name, col_name, table_index, col_index, rule,
                                           length_constraint)

    def _validate_basic_rule_structure(self, rule: Dict[str, Any], table_name: str, col_name: str):
        """Validate basic rule structure - optimized with early returns"""
        rule_type = rule.get('type')

        if rule_type == 'range':
            min_val, max_val = rule.get('min'), rule.get('max')
            if min_val is not None and max_val is not None and min_val > max_val:
                self.errors.append(
                    f"Table '{table_name}', Column '{col_name}': Range min ({min_val}) > max ({max_val})")

        elif rule_type == 'choice':
            value = rule.get('value')
            if value is None:
                self.errors.append(f"Table '{table_name}', Column '{col_name}': Choice rule must have 'value' field")
            elif not isinstance(value, list) or len(value) == 0:
                self.errors.append(f"Table '{table_name}', Column '{col_name}': Choice 'value' must be non-empty list")

    def _handle_choice_rule(self, column: Dict[str, Any], table_name: str, col_name: str,
                            table_index: int, col_index: int, rule: Dict[str, Any], length_constraint):
        """Handle choice rule validation and length correction"""
        choices = rule.get('value', [])
        if not choices:
            return

        # Calculate choice dimensions
        choice_info = self._analyze_choice_dimensions(rule, choices)

        # Validate and correct length constraints
        if length_constraint is not None:
            self._correct_choice_length_constraint(column, table_name, col_name, table_index, col_index,
                                                   choice_info, length_constraint)

    def _analyze_choice_dimensions(self, rule: Dict[str, Any], choices: List) -> Dict[str, int]:
        """Analyze choice value dimensions - optimized with list comprehension"""
        choice_lengths = [len(str(choice)) for choice in choices]
        prefix_len = len(rule.get('prefix', ''))
        suffix_len = len(rule.get('suffix', ''))
        base_len = prefix_len + suffix_len

        return {
            'min_length': min(choice_lengths) + base_len,
            'max_length': max(choice_lengths) + base_len,
            'choices': choices
        }

    def _correct_choice_length_constraint(self, column: Dict[str, Any], table_name: str, col_name: str,
                                          table_index: int, col_index: int, choice_info: Dict, length_constraint):
        """Correct length constraints that conflict with choice values"""
        min_len, max_len = choice_info['min_length'], choice_info['max_length']

        if isinstance(length_constraint, int):
            if min_len != max_len:
                self._apply_correction(table_index, col_index, 'length', length_constraint, None,
                                       'removed_conflicting_fixed_length')
                self.warnings.append(f"Table '{table_name}', Column '{col_name}': "
                                     f"Fixed length {length_constraint} removed (choices range {min_len}-{max_len})")
            elif length_constraint != min_len:
                self._apply_correction(table_index, col_index, 'length', length_constraint, min_len,
                                       'corrected_choice_length')
                self.warnings.append(f"Table '{table_name}', Column '{col_name}': "
                                     f"Length corrected from {length_constraint} to {min_len}")

        elif isinstance(length_constraint, dict):
            new_constraint = self._adjust_range_constraint(length_constraint, min_len, max_len, table_name, col_name)
            if new_constraint != length_constraint:
                self._apply_correction(table_index, col_index, 'length', length_constraint, new_constraint,
                                       'adjusted_choice_range')

    def _adjust_range_constraint(self, constraint: Dict, min_len: int, max_len: int,
                                 table_name: str, col_name: str) -> Dict:
        """Adjust range constraint to match choice dimensions"""
        new_constraint = {}

        constraint_min = constraint.get('min')
        if constraint_min is not None:
            if constraint_min > min_len:
                self.warnings.append(f"Table '{table_name}', Column '{col_name}': "
                                     f"Min length {constraint_min} > shortest choice {min_len}")
                new_constraint['min'] = min_len
            else:
                new_constraint['min'] = constraint_min

        constraint_max = constraint.get('max')
        if constraint_max is not None:
            if constraint_max < max_len:
                self.warnings.append(f"Table '{table_name}', Column '{col_name}': "
                                     f"Max length {constraint_max} < longest choice {max_len}")
                new_constraint['max'] = max_len
            else:
                new_constraint['max'] = constraint_max

        return new_constraint

    def _handle_primary_key_with_rule(self, column: Dict[str, Any], table_name: str, col_name: str,
                                      table_index: int, col_index: int, rule: Dict[str, Any], length_constraint):
        """Handle primary key length validation with prefix/suffix rules"""
        prefix_len = len(rule.get('prefix', ''))
        suffix_len = len(rule.get('suffix', ''))

        if prefix_len == 0 and suffix_len == 0:
            return

        length_requirements = self._calculate_pk_length_requirements(rule, prefix_len, suffix_len)
        self._validate_pk_length_constraint(column, table_name, col_name, table_index, col_index,
                                            length_constraint, length_requirements)

    def _calculate_pk_length_requirements(self, rule: Dict[str, Any], prefix_len: int, suffix_len: int) -> Dict:
        """Calculate length requirements for primary key with prefix/suffix - optimized"""
        rule_type = rule.get('type')

        if rule_type == 'range':
            core_min_len = len(str(rule.get('min', 1)))
            core_max_len = len(str(rule.get('max', sys.maxsize)))
        elif rule_type == 'sequence':
            start_val = rule.get('start', 1)
            step = rule.get('step', 1)
            estimated_max = start_val + (1000 * step)
            core_min_len = len(str(start_val))
            core_max_len = len(str(estimated_max))
        else:
            core_min_len, core_max_len = 1, 10

        return {
            'min_required': prefix_len + core_min_len + suffix_len,
            'max_possible': prefix_len + core_max_len + suffix_len
        }

    def _validate_pk_length_constraint(self, column: Dict[str, Any], table_name: str, col_name: str,
                                       table_index: int, col_index: int, length_constraint, requirements):
        """Validate primary key length constraint"""
        min_req, max_pos = requirements['min_required'], requirements['max_possible']

        if length_constraint is None:
            self._suggest_pk_length_constraint(table_name, col_name, min_req, max_pos)
            return

        if isinstance(length_constraint, int):
            if length_constraint < min_req:
                self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                                   f"PK length {length_constraint} < required {min_req}")
                self._apply_correction(table_index, col_index, 'length', length_constraint, min_req,
                                       'corrected_pk_min_length')
            elif length_constraint > max_pos:
                self.warnings.append(f"Table '{table_name}', Column '{col_name}': "
                                     f"PK length {length_constraint} > needed {max_pos}")
                self._apply_correction(table_index, col_index, 'length', length_constraint, max_pos,
                                       'optimized_pk_max_length')

    def _suggest_pk_length_constraint(self, table_name: str, col_name: str, min_req: int, max_pos: int):
        """Suggest appropriate length constraint for primary key"""
        if min_req == max_pos:
            self.suggestions.append(f"Table '{table_name}', Column '{col_name}': "
                                    f"Consider fixed length: {min_req} for PK with prefix/suffix")
        else:
            self.suggestions.append(f"Table '{table_name}', Column '{col_name}': "
                                    f"Consider length range: {{\"min\": {min_req}, \"max\": {max_pos}}}")

    def _handle_nullable_logic(self, column: Dict[str, Any], table_name: str, col_name: str,
                               table_index: int, col_index: int, constraint_info: Dict):
        """Handle nullable field validation and correction"""
        original_nullable = column.get('nullable')
        is_pk = constraint_info['is_primary_key']
        is_unique = constraint_info['is_unique']
        has_default = constraint_info['has_default']

        # Determine correct nullable value
        if is_pk:
            correct_nullable = False
            reason = "primary_key_constraint"
        elif is_unique and not has_default:
            correct_nullable = False
            reason = "unique_constraint_without_default"
        elif original_nullable is not None:
            correct_nullable = original_nullable
            reason = "user_specified"
        else:
            correct_nullable = True
            reason = "default_behavior"

        # Apply correction if needed
        if original_nullable is None:
            self._apply_correction(table_index, col_index, 'nullable', None, correct_nullable, reason)
        elif original_nullable != correct_nullable and is_pk:
            self.errors.append(f"Table '{table_name}', Column '{col_name}': PK cannot be nullable")
            self._apply_correction(table_index, col_index, 'nullable', original_nullable, False,
                                   "primary_key_override")

    def _validate_type_rule_consistency(self, column: Dict[str, Any], table_name: str, col_name: str,
                                        table_index: int, col_index: int):
        """Validate type-rule consistency with auto-correction"""
        col_type = column.get('type', '').lower()
        rule = column.get('rule')

        base_type, length, precision = self.parse_sql_type(col_type)
        print(base_type, length, precision)

        if not rule or not isinstance(rule, dict):
            return

        rule_type = rule.get('type', '').lower()
        type_compatibility = self._get_type_rule_compatibility()

        compatibility = type_compatibility.get(rule_type)
        if not compatibility:
            return

        compatible_types = set(compatibility['compatible_types'])  # Use set for faster lookups
        auto_convert = compatibility.get('auto_convert', False)

        if col_type not in compatible_types:
            if col_type in {'str', 'string', 'text'} and auto_convert:
                self._handle_string_type_conversion(column, table_name, col_name, table_index, col_index,
                                                    rule, rule_type)
            else:
                suggested_type = compatibility['compatible_types'][0] if compatibility['compatible_types'] else 'str'
                self.errors.append(f"Table '{table_name}', Column '{col_name}': "
                                   f"Type '{col_type}' incompatible with rule '{rule_type}'. "
                                   f"Auto-correcting to: '{suggested_type}'")
                self._apply_correction(table_index, col_index, 'type', col_type, suggested_type,
                                       f"critical_type_rule_mismatch_{rule_type}")

    def _get_type_rule_compatibility(self) -> Dict[str, Dict]:
        """Get type-rule compatibility matrix - cached for performance"""
        if self._type_compatibility_cache is None:
            self._type_compatibility_cache = {
                'range': {
                    'compatible_types': ['int', 'integer', 'float', 'double', 'decimal', 'number'],
                    'string_acceptable': True,
                    'preferred_type_for_string': 'int',
                    'auto_convert': True
                },
                'sequence': {
                    'compatible_types': ['int', 'integer'],
                    'string_acceptable': True,
                    'preferred_type_for_string': 'int',
                    'auto_convert': True
                },
                'choice': {
                    'compatible_types': ['str', 'string', 'text', 'int', 'integer', 'float', 'bool', 'boolean'],
                    'string_acceptable': True,
                    'preferred_type_for_string': None,
                    'auto_convert': True
                },
                'email': {
                    'compatible_types': ['str', 'string', 'text'],
                    'string_acceptable': False,
                    'auto_convert': False
                },
                'phone_number': {
                    'compatible_types': ['str', 'string', 'text'],
                    'string_acceptable': False,
                    'auto_convert': False
                },
                'date': {
                    'compatible_types': ['date', 'str', 'string'],
                    'string_acceptable': False,
                    'auto_convert': False
                },
                'date_range': {
                    'compatible_types': ['date', 'str', 'string'],
                    'string_acceptable': True,
                    'preferred_type_for_string': 'str',
                    'auto_convert': True
                },
                'time_range': {
                    'compatible_types': ['time', 'str', 'string'],
                    'string_acceptable': True,
                    'preferred_type_for_string': 'str',
                    'auto_convert': True
                },
                'timestamp_range': {
                    'compatible_types': ['timestamp', 'datetime', 'str', 'string'],
                    'string_acceptable': True,
                    'preferred_type_for_string': 'str',
                    'auto_convert': True
                }
            }
        return self._type_compatibility_cache

    def _handle_string_type_conversion(self, column: Dict[str, Any], table_name: str, col_name: str,
                                       table_index: int, col_index: int, rule: Dict, rule_type: str):
        """Handle string type conversion for numeric rules"""
        # Check for prefix/suffix that would justify string type
        if rule.get('prefix') or rule.get('suffix'):
            return

        suggested_type = self._determine_conversion_type(rule, rule_type)
        if suggested_type:
            self.warnings.append(f"Table '{table_name}', Column '{col_name}': "
                                 f"Auto-converting 'str' to '{suggested_type}' for {rule_type} rule")
            self._apply_correction(table_index, col_index, 'type', 'str', suggested_type,
                                   f"auto_converted_{rule_type}_to_{suggested_type}")

    def _determine_conversion_type(self, rule: Dict, rule_type: str) -> Optional[str]:
        """Determine appropriate conversion type based on rule - optimized"""
        if rule_type == 'range':
            min_val, max_val = rule.get('min'), rule.get('max')
            if isinstance(min_val, int) and isinstance(max_val, int):
                return 'int'
            elif isinstance(min_val, (int, float)) and isinstance(max_val, (int, float)):
                return 'float'

        elif rule_type == 'sequence':
            start, step = rule.get('start', 1), rule.get('step', 1)
            if isinstance(start, int) and isinstance(step, int):
                return 'int'

        elif rule_type == 'choice':
            choices = rule.get('value', [])
            if choices:
                if all(isinstance(choice, int) for choice in choices):
                    return 'int'
                elif all(isinstance(choice, (int, float)) for choice in choices):
                    return 'float'

        return None

    def _validate_foreign_keys(self, foreign_keys: List[Dict], table_name: str):
        """Validate foreign key constraints"""
        for i, fk in enumerate(foreign_keys):
            if not self._validate_fk_structure(fk, table_name, i):
                continue

            parent_table = fk['parent_table']
            parent_column = fk['parent_column']
            child_column = fk['child_column']

            if not self._validate_fk_references(parent_table, parent_column, child_column, table_name, i):
                continue

            self._validate_fk_type_compatibility(parent_table, parent_column, child_column, table_name, i)

    def _validate_fk_structure(self, fk: Dict, table_name: str, fk_index: int) -> bool:
        """Validate foreign key structure"""
        required_fields = {'parent_table', 'parent_column', 'child_column'}  # Use set for faster lookups
        missing_fields = required_fields - set(fk.keys())

        if missing_fields:
            for field in missing_fields:
                self.errors.append(f"Table '{table_name}': FK {fk_index + 1} missing '{field}'")
            return False
        return True

    def _validate_fk_references(self, parent_table: str, parent_column: str, child_column: str,
                                table_name: str, fk_index: int) -> bool:
        """Validate foreign key references exist"""
        # Check parent table exists
        if parent_table not in self.table_registry:
            self.errors.append(
                f"Table '{table_name}': FK {fk_index + 1} references non-existent table '{parent_table}'")
            return False

        # Check parent column exists
        parent_columns = self.table_registry[parent_table]['columns']
        if parent_column not in parent_columns:
            self.errors.append(f"Table '{table_name}': FK {fk_index + 1} references non-existent column "
                               f"'{parent_column}' in table '{parent_table}'")
            return False

        # Check child column exists
        current_table_columns = self.table_registry.get(table_name, {}).get('columns', {})
        if child_column not in current_table_columns:
            self.errors.append(
                f"Table '{table_name}': FK {fk_index + 1} references non-existent child column '{child_column}'")
            return False

        return True

    def _validate_fk_type_compatibility(self, parent_table: str, parent_column: str, child_column: str,
                                        table_name: str, fk_index: int):
        """Validate foreign key type and length compatibility"""
        parent_col = self.table_registry[parent_table]['columns'][parent_column]
        child_col = self.table_registry[table_name]['columns'][child_column]

        # Type compatibility
        parent_type = parent_col.get('type')
        child_type = child_col.get('type')

        if parent_type != child_type:
            self.errors.append(f"Table '{table_name}': FK {fk_index + 1} type mismatch. "
                               f"Parent '{parent_table}.{parent_column}' is '{parent_type}', "
                               f"child '{child_column}' is '{child_type}'")

        # Length compatibility
        self._validate_fk_length_compatibility(parent_col, child_col, parent_table, parent_column,
                                               child_column, table_name, fk_index)

    def _validate_fk_length_compatibility(self, parent_col: Dict, child_col: Dict, parent_table: str,
                                          parent_column: str, child_column: str, table_name: str, fk_index: int):
        """Validate foreign key length compatibility"""
        parent_length = parent_col.get('length')
        child_length = child_col.get('length')

        if parent_length is None or child_length is None:
            return

        # Both have length constraints
        if isinstance(parent_length, int) and isinstance(child_length, int):
            if child_length < parent_length:
                self.errors.append(f"Table '{table_name}': FK {fk_index + 1} length mismatch. "
                                   f"Child '{child_column}' length ({child_length}) < "
                                   f"parent '{parent_table}.{parent_column}' length ({parent_length})")

        elif isinstance(parent_length, dict) and isinstance(child_length, dict):
            parent_max = parent_length.get('max', float('inf'))
            child_max = child_length.get('max', float('inf'))
            if child_max < parent_max:
                self.warnings.append(f"Table '{table_name}': FK {fk_index + 1} potential length issue. "
                                     f"Child max ({child_max}) < parent max ({parent_max})")

    def _apply_correction(self, table_index: int, col_index: int, field: str, old_value: Any,
                          new_value: Any, reason: str):
        """Apply correction to the corrected schema - optimized with early validation"""
        if not self._is_valid_correction_path(table_index, col_index):
            return

        column = self.corrected_schema['tables'][table_index]['columns'][col_index]

        if new_value is None and field == 'length':
            column.pop('length', None)
        elif field == 'rule' and isinstance(new_value, dict):
            # For rule corrections, merge or replace appropriately
            column[field] = new_value
        else:
            column[field] = new_value

        # Initialize corrections list if it doesn't exist
        if 'corrections' not in column:
            column['corrections'] = []

        column['corrections'].append({
            'field': field,
            'old_value': old_value,
            'new_value': new_value,
            'reason': reason,
            'action': 'removed' if new_value is None else ('set_default' if old_value is None else 'corrected')
        })

    def _is_valid_correction_path(self, table_index: int, col_index: int) -> bool:
        """Check if correction path is valid - optimized with early returns"""
        if not self.corrected_schema or 'tables' not in self.corrected_schema:
            return False

        tables = self.corrected_schema['tables']
        if table_index >= len(tables):
            return False

        table = tables[table_index]
        if 'columns' not in table:
            return False

        return col_index < len(table['columns'])

    # Enhanced utility methods for better performance and functionality
    def get_validation_summary(self) -> Dict[str, Any]:
        """Get comprehensive validation summary"""
        return {
            'critical_errors': len(self.critical_errors),
            'errors': len(self.errors),
            'warnings': len(self.warnings),
            'suggestions': len(self.suggestions),
            'has_critical_issues': len(self.critical_errors) > 0,
            'validation_passed': len(self.critical_errors) == 0 and len(self.errors) == 0,
            'schema_corrected': self.corrected_schema is not None
        }

    def get_critical_errors(self) -> List[str]:
        """Get list of critical errors that prevent schema processing"""
        return self.critical_errors.copy()

    def get_format_validation_summary(self, corrected_schema: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Get summary of format validations for date/time rules"""
        schema = corrected_schema or self.corrected_schema
        if not schema:
            return []

        format_issues = []
        for table in schema.get('tables', []):
            table_name = table.get('table_name', 'Unknown')
            for col in table.get('columns', []):
                col_name = col.get('name', 'Unknown')
                rule = col.get('rule', {})

                if isinstance(rule, dict):
                    rule_type = rule.get('type', '')
                    format_field = rule.get('format')

                    if rule_type in ['date_range', 'time_range', 'timestamp_range'] and format_field:
                        format_issues.append({
                            'table': table_name,
                            'column': col_name,
                            'rule_type': rule_type,
                            'format': format_field,
                            'validated': True
                        })

        return format_issues

    def has_critical_issues(self) -> bool:
        """Check if there are critical issues that prevent processing"""
        return len(self.critical_errors) > 0

    def is_valid_schema(self) -> bool:
        """Check if schema is valid (no critical errors or regular errors)"""
        return len(self.critical_errors) == 0 and len(self.errors) == 0

    def get_table_statistics(self) -> Dict[str, Any]:
        """Get statistics about validated tables"""
        if not self.corrected_schema:
            return {}

        tables = self.corrected_schema.get('tables', [])
        total_columns = sum(len(table.get('columns', [])) for table in tables)

        pk_tables = 0
        fk_tables = 0
        date_columns = 0

        for table in tables:
            # Check for primary keys
            has_pk = any('PK' in self._get_constraints(col) for col in table.get('columns', []))
            if has_pk:
                pk_tables += 1

            # Check for foreign keys
            if table.get('foreign_keys'):
                fk_tables += 1

            # Count date/time columns
            for col in table.get('columns', []):
                rule = col.get('rule', {})
                if isinstance(rule, dict):
                    rule_type = rule.get('type', '')
                    if rule_type in ['date_range', 'time_range', 'timestamp_range']:
                        date_columns += 1

        return {
            'total_tables': len(tables),
            'total_columns': total_columns,
            'tables_with_pk': pk_tables,
            'tables_with_fk': fk_tables,
            'date_time_columns': date_columns,
            'avg_columns_per_table': total_columns / len(tables) if tables else 0
        }

    def get_type_conversion_summary(self) -> List[Dict[str, Any]]:
    """Get summary of type conversions performed"""
    if not self.corrected_schema:
        return []
    
    conversions = []
    for table in self.corrected_schema.get('tables', []):
        table_name = table.get('table_name', 'Unknown')
        for col in table.get('columns', []):
            col_name = col.get('name', 'Unknown')
            sql_type_info = col.get('sql_type_info')
            
            if sql_type_info:
                conversions.append({
                    'table': table_name,
                    'column': col_name,
                    'original_sql_type': sql_type_info['original_type'],
                    'base_type': sql_type_info['base_type'],
                    'converted_python_type': col.get('type'),
                    'length_from_sql': sql_type_info.get('length'),
                    'precision_from_sql': sql_type_info.get('precision')
                })
    
    return conversions

    def validate_email_format(self, email: str) -> bool:
        """Validate email format using pre-compiled regex - optimized"""
        return bool(self._email_pattern.match(email))

    def validate_phone_format(self, phone: str) -> bool:
        """Validate phone format using pre-compiled regex - optimized"""
        return bool(self._phone_pattern.match(phone))

    def cleanup(self):
        """Clean up resources and reset state"""
        self._reset_state()
        self.corrected_schema = None
        self._type_compatibility_cache = None
