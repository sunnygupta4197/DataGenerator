import json
from typing import Dict, Any
from datetime import datetime

from .choose_file_loader import ChoiceFileLoader
from .schema_validator import SchemaValidator


class JSONConfigReader:
    def __init__(self, file_name):
        self.file_name = file_name
        self.choice_loader = ChoiceFileLoader()

    def load_config(self):
        try:
            with open(self.file_name) as json_file:
                config = json.load(json_file)
                self._process_file_choices(config)

                try:
                    if not self._is_valid_schema_format(config):
                        print("🔄 Converting legacy config format to current schema format...")
                        json_converter = JSONTemplateConverter()
                        config = json_converter.convert(config)
                    return config
                except Exception as e:
                    print("🚨 CRITICAL: Failed to convert legacy config format")
                    exit(1)

        except FileNotFoundError:
            print(f"🚨 CRITICAL: Config file {self.file_name} not found.")
            exit(1)
        except json.JSONDecodeError as e:
            print(f'🚨 CRITICAL: Invalid JSON in config file: {e}')
            exit(1)
        except Exception as e:
            print(f"🚨 CRITICAL: Unexpected error loading config: {e}")
            exit(1)

    def _process_file_choices(self, config: Dict[str, Any]):
        """Process and resolve file-based choice rules"""
        if 'tables' not in config:
            return

        for table in config['tables']:
            if 'columns' not in table:
                continue

            for column in table['columns']:
                if 'rule' not in column:
                    continue

                rule = column['rule']

                # Handle different rule formats
                if isinstance(rule, dict) and rule.get('type') == 'choice_from_file':
                    try:
                        # Load choices from file
                        file_rule = self.choice_loader.load_choices_from_file(
                            file_path=rule['file_path'],
                            column=rule.get('column'),
                            weight_column=rule.get('weight_column')
                        )

                        # Replace the rule with loaded choices
                        column['rule'] = file_rule

                        print(
                            f"✅ Loaded {len(file_rule['value'])} choices for {table['table_name']}.{column['name']} from {rule['file_path']}")

                    except Exception as e:
                        print(f"❌ Error loading choices for {table['table_name']}.{column['name']}: {e}")
                        # Keep original rule as fallback
                        continue

    def _is_valid_schema_format(self, config):
        """Check if config is already in the correct schema format"""
        if not isinstance(config, dict):
            return False

        # Must have tables array
        if 'tables' not in config or not isinstance(config['tables'], list):
            return False

        # Check if tables have the expected structure
        for table in config['tables']:
            if not isinstance(table, dict):
                return False
            if 'table_name' not in table or 'columns' not in table:
                return False
            if not isinstance(table['columns'], list):
                return False

            # Check column structure
            for column in table['columns']:
                if not isinstance(column, dict):
                    return False
                if 'name' not in column or 'type' not in column:
                    return False

        return True

    def validate_schema(self, schema_dict):
        try:
            errors, warnings, suggestions, critical_errors, corrected_schema = validate_schema_file(
                schema_dict=schema_dict)
            return self._handle_validation_results(errors, warnings, suggestions, critical_errors, corrected_schema)
        except Exception as validation_error:
            print(f"🚨 CRITICAL: Schema validation error: {validation_error}")
            exit(1)

    def _handle_validation_results(self, errors, warnings, suggestions, critical_errors, corrected_schema):
        """Handle validation results and determine whether to continue or exit"""
        print("=== VALIDATION RESULTS ===")

        print(f"\nCritical Errors found: {len(critical_errors)}")
        for error in critical_errors:
            print(f"🔴 CRITICAL: {error}")

        print(f"\nNon-Critical Errors found: {len(errors)}")
        for error in errors:
            print(f"❌ {error}")

        print(f"\nWarnings found: {len(warnings)}")
        for warning in warnings:
            print(f"⚠️  {warning}")

        print(f"\nSuggestions found: {len(suggestions)}")
        for suggestion in suggestions:
            print(f"💡 {suggestion}")

        # Exit if critical errors found
        if critical_errors:
            print(f"\n🚨 SCRIPT TERMINATED: {len(critical_errors)} critical error(s) must be fixed before proceeding.")
            print("Critical errors indicate fundamental issues that would cause data corruption or system failure.")
            exit(1)

        # Continue if only non-critical errors, warnings, or suggestions
        if errors:
            print(
                f"\n⚠️  Continuing with {len(errors)} non-critical error(s). These may cause data quality issues but won't break the system.")

        # Show applied corrections
        self._show_applied_corrections(corrected_schema)

        return corrected_schema

    def _show_applied_corrections(self, corrected_schema):
        """Display any corrections that were applied to the schema"""
        corrections_found = False

        for table in corrected_schema.get('tables', []):
            for col in table.get('columns', []):
                if 'corrections' in col:
                    if not corrections_found:
                        print(f"\n🔧 Auto-corrections applied:")
                        corrections_found = True
                    print(f"   - {table['table_name']}.{col['name']}: {len(col['corrections'])} correction(s)")
                    for correction in col['corrections']:
                        action = correction.get('action', 'modified')
                        print(
                            f"     • {correction['field']}: {correction['old_value']} → {correction['new_value']} ({correction['reason']}) [{action}]")

        if not corrections_found:
            print(f"\n✅ No auto-corrections needed")


class JSONTemplateConverter:
    """Converts legacy JSON configurations to the current schema format"""

    def __init__(self):
        self.conversion_errors = []

    def convert(self, legacy_config):
        """Convert legacy config format to current schema format"""
        print('🔄 Converting legacy configuration format...')

        self.conversion_errors = []

        # Handle different legacy formats
        if self._is_single_table_format(legacy_config):
            return self._convert_single_table_format(legacy_config)
        elif self._is_old_multi_table_format(legacy_config):
            return self._convert_old_multi_table_format(legacy_config)
        else:
            print("❌ Unrecognized legacy format")
            return None

    def _is_single_table_format(self, config):
        """Check if this is a single table legacy format"""
        return (
                isinstance(config, dict) and
                'table_name' in config and
                'columns' in config and
                'tables' not in config
        )

    def _is_old_multi_table_format(self, config):
        """Check if this is an old multi-table format that needs conversion"""
        return (
                isinstance(config, dict) and
                'tables' in config and
                isinstance(config['tables'], list) and
                len(config['tables']) > 0 and
                # Check if any table is missing required fields or has old structure
                any(self._needs_table_conversion(table) for table in config['tables'])
        )

    def _needs_table_conversion(self, table):
        """Check if a table needs conversion from legacy format"""
        if not isinstance(table, dict):
            return True

        # Check for missing required fields
        if 'table_name' not in table or 'columns' not in table:
            return True

        # Check for old column format
        if isinstance(table.get('columns'), dict):  # Old format uses dict instead of list
            return True

        # Check if columns are missing required fields
        if isinstance(table.get('columns'), list):
            for col in table['columns']:
                if not isinstance(col, dict) or 'name' not in col or 'type' not in col:
                    return True

        return False

    def _convert_single_table_format(self, config):
        """Convert single table legacy format"""
        try:
            template = {
                "tables": [
                    {
                        "table_name": config.get("table_name", "unnamed_table"),
                        "columns": [],
                        "foreign_keys": config.get("foreign_keys", [])
                    }
                ],
                "locale": config.get("locale", "en_GB"),
                "rows": config.get("rows", config.get("row_count", 100)),
                "output_format": config.get("output_format", "csv")
            }

            # Convert columns
            columns_data = config.get("columns", {})
            if isinstance(columns_data, dict):
                # Old format: columns as dictionary
                for col_name, col_info in columns_data.items():
                    converted_column = self._convert_legacy_column(col_name, col_info, config)
                    template["tables"][0]["columns"].append(converted_column)
            elif isinstance(columns_data, list):
                # Already in list format, just validate and clean
                for col in columns_data:
                    if isinstance(col, dict) and 'name' in col:
                        cleaned_column = self._clean_column_definition(col)
                        template["tables"][0]["columns"].append(cleaned_column)

            return template

        except Exception as e:
            print(f"❌ Error converting single table format: {e}")
            return None

    def _convert_old_multi_table_format(self, config):
        """Convert old multi-table format"""
        try:
            template = {
                "tables": [],
                "locale": config.get("locale", "en_GB"),
                "rows": config.get("rows", config.get("row_count", 100)),
                "output_format": config.get("output_format", "csv")
            }

            for table in config.get("tables", []):
                converted_table = self._convert_legacy_table(table)
                if converted_table:
                    template["tables"].append(converted_table)

            return template

        except Exception as e:
            print(f"❌ Error converting multi-table format: {e}")
            return None

    def _convert_legacy_table(self, table):
        """Convert a legacy table definition"""
        if not isinstance(table, dict):
            return None

        converted_table = {
            "table_name": table.get("table_name", "unnamed_table"),
            "columns": [],
            "foreign_keys": table.get("foreign_keys", [])
        }

        # Convert columns
        columns_data = table.get("columns", [])
        if isinstance(columns_data, dict):
            # Old format: columns as dictionary
            for col_name, col_info in columns_data.items():
                converted_column = self._convert_legacy_column(col_name, col_info, table)
                converted_table["columns"].append(converted_column)
        elif isinstance(columns_data, list):
            # List format, clean each column
            for col in columns_data:
                if isinstance(col, dict):
                    cleaned_column = self._clean_column_definition(col)
                    converted_table["columns"].append(cleaned_column)

        return converted_table

    def _convert_legacy_column(self, col_name, col_info, context):
        """Convert legacy column definition"""
        # Basic column structure
        converted_column = {
            "name": col_name,
            "type": self._normalize_data_type(col_info.get("type", "str"))
        }

        # Handle constraints from context
        primary_keys = context.get("primary_keys", [])
        required_columns = context.get("required_columns", [])
        unique_constraints = context.get("unique_constraints", [])

        constraints = []
        if col_name in primary_keys:
            constraints.append("PK")
        if col_name in unique_constraints:
            constraints.append("unique")

        if constraints:
            converted_column["constraints"] = constraints

        # Handle nullable - only set if explicitly provided
        if "nullable" in col_info:
            converted_column["nullable"] = bool(col_info["nullable"])

        # Convert rules from legacy format
        rules = context.get("rules", {})
        rule = self._extract_legacy_rule(col_name, col_info, rules)
        if rule:
            converted_column["rule"] = rule

        # Handle other properties
        if "default" in col_info:
            converted_column["default"] = col_info["default"]

        # Handle length constraints
        length_constraints = context.get("length_constraints", {})
        if col_name in length_constraints:
            converted_column["length"] = self._convert_length_constraint(length_constraints[col_name])

        # Handle sensitivity
        sensitivity = context.get("sensitivity", {})
        if col_name in sensitivity:
            converted_column["sensitivity"] = sensitivity[col_name]

        return converted_column

    def _clean_column_definition(self, col):
        """Clean and normalize a column definition"""
        cleaned = {
            "name": col.get("name", "unnamed_column"),
            "type": self._normalize_data_type(col.get("type", "str"))
        }

        # Copy over valid fields
        valid_fields = ["nullable", "constraints", "constraint", "rule", "default", "length", "sensitivity",
                        "conditional_rules"]
        for field in valid_fields:
            if field in col and col[field] is not None:
                cleaned[field] = col[field]

        return cleaned

    def _normalize_data_type(self, data_type):
        """Normalize data type names"""
        if not isinstance(data_type, str):
            return "str"

        type_mapping = {
            "string": "str",
            "text": "str",
            "varchar": "str",
            "char": "str",
            "boolean": "bool",
            "integer": "int",
            "number": "int",
            "decimal": "float",
            "double": "float",
            "timestamp": "datetime",
            "guid": "uuid"
        }

        normalized = data_type.lower().strip()
        return type_mapping.get(normalized, normalized)

    def _extract_legacy_rule(self, col_name, col_info, rules_context):
        """Extract rule from legacy format"""
        # Check for direct rule in column info
        if "rule" in col_info:
            return col_info["rule"]

        # Check in rules context
        if isinstance(rules_context, dict):
            # Check validations
            validations = rules_context.get("validations", {})
            if col_name in validations:
                return self._convert_validation_to_rule(validations[col_name])

            # Check distributions
            distributions = rules_context.get("distributions", {})
            if col_name in distributions:
                return self._convert_distribution_to_rule(distributions[col_name])

            # Check default values for date rules
            default_values = rules_context.get("default_values", {})
            if col_name in default_values:
                default_val = default_values[col_name]
                if isinstance(default_val, str) and self._is_date_string(default_val):
                    return {"type": "date_range", "start": default_val}

        # Generate rule based on column name patterns
        return self._generate_rule_from_name(col_name)

    def _convert_validation_to_rule(self, validation):
        """Convert legacy validation to rule"""
        if isinstance(validation, dict):
            if "regex" in validation:
                return {"type": "regex", "pattern": validation["regex"]}

            if "min_value" in validation or "max_value" in validation:
                rule = {"type": "range"}
                if "min_value" in validation:
                    rule["min"] = validation["min_value"]
                if "max_value" in validation:
                    rule["max"] = validation["max_value"]
                return rule

        return None

    def _convert_distribution_to_rule(self, distribution):
        """Convert legacy distribution to rule"""
        if isinstance(distribution, dict):
            if "values" in distribution:
                rule = {"type": "choice", "value": distribution["values"]}
                if "probabilities" in distribution:
                    rule["probabilities"] = dict(zip(distribution["values"], distribution["probabilities"]))
                return rule

        return None

    def _generate_rule_from_name(self, col_name):
        """Generate appropriate rule based on column name"""
        col_lower = col_name.lower()

        if "email" in col_lower:
            return {"type": "email"}
        elif any(name_part in col_lower for name_part in ["name", "first_name", "last_name"]):
            return "name" if "name" in col_lower else "first_name"
        elif "phone" in col_lower:
            return {"type": "phone_number"}
        elif any(date_part in col_lower for date_part in ["date", "birth"]):
            return {"type": "date_range", "start": "1950-01-01"}
        elif "registration" in col_lower:
            return {"type": "date_range", "start": "2020-01-01"}
        elif "age" in col_lower:
            return {"type": "range", "min": 18, "max": 90}
        elif "income" in col_lower or "salary" in col_lower:
            return {"type": "range", "min": 1000, "max": 1000000}
        elif "gender" in col_lower:
            return {"type": "choice", "value": ["Male", "Female", "Other", "Not Specified"]}
        elif col_lower.endswith("_id") or col_lower == "id":
            return {"type": "range", "min": 1, "max": 999999}
        elif "uuid" in col_lower or "guid" in col_lower:
            return "uuid"
        elif "status" in col_lower:
            return {"type": "choice", "value": ["ACTIVE", "INACTIVE", "PENDING"]}

        return None

    def _convert_length_constraint(self, length_constraint):
        """Convert length constraint to proper format"""
        if isinstance(length_constraint, int):
            return length_constraint
        elif isinstance(length_constraint, dict):
            result = {}
            if "min" in length_constraint:
                result["min"] = length_constraint["min"]
            if "max" in length_constraint:
                result["max"] = length_constraint["max"]
            return result if result else None
        elif isinstance(length_constraint, str):
            # Handle "min-max" format
            if "-" in length_constraint:
                parts = length_constraint.split("-")
                if len(parts) == 2:
                    try:
                        return {
                            "min": int(parts[0].strip()),
                            "max": int(parts[1].strip())
                        }
                    except ValueError:
                        pass
            else:
                try:
                    return int(length_constraint)
                except ValueError:
                    pass
        return None

    def _is_date_string(self, date_str):
        """Check if string is a valid date"""
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return True
        except ValueError:
            return False


def validate_schema_file(file_path: str = None, schema_dict: Dict = None):
    """Main function to validate schema from file or dictionary"""
    validator = SchemaValidator()

    if file_path:
        try:
            with open(file_path, 'r') as f:
                schema = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            return [f"Error reading schema file: {str(e)}"], [], [], {}
    elif schema_dict:
        schema = schema_dict
    else:
        return ["No schema provided"], [], [], {}

    errors, warnings, suggestions, critical_error, corrected_schema = validator.validate_schema(schema)
    return errors, warnings, suggestions, critical_error, corrected_schema


# Test the enhanced validator with your specific issues
if __name__ == "__main__":
    # Enhanced test case with string types that should be auto-converted
    test_auto_conversion = {
        "tables": [
            {
                "table_name": "customer",
                "columns": [
                    {
                        "name": "customer_id",
                        "type": "str",  # Will be auto-converted to int
                        "constraint": ["PK"],
                        "nullable": False,
                        "rule": {
                            "type": "range",
                            "min": 1000,
                            "max": 999999
                        },
                        "length": 6
                    },
                    {
                        "name": "postal_code",
                        "type": "str",  # Will be auto-converted to int
                        "rule": {
                            "type": "range",
                            "min": 10000,
                            "max": 99999
                        }
                    },
                    {
                        "name": "order_sequence",
                        "type": "str",  # Will be auto-converted to int
                        "rule": {
                            "type": "sequence",
                            "start": 1,
                            "step": 1
                        }
                    },
                    {
                        "name": "priority_level",
                        "type": "str",  # Will be auto-converted to int
                        "rule": {
                            "type": "choice",
                            "value": [1, 2, 3, 4, 5]  # All numeric choices
                        }
                    },
                    {
                        "name": "formatted_id",
                        "type": "str",  # Will NOT be converted (has prefix)
                        "rule": {
                            "prefix": "CUST-",
                            "type": "range",
                            "min": 1000,
                            "max": 9999
                        }
                    },
                    {
                        "name": "status",
                        "type": "str",  # Will NOT be converted (string choices)
                        "rule": {
                            "type": "choice",
                            "value": ["ACTIVE", "INACTIVE", "PENDING"]
                        }
                    }
                ]
            }
        ],
        "locale": "en_GB",
        "rows": 1000,
        "output_format": "csv"
    }

    print("TESTING AUTO TYPE CONVERSION WITH PRESERVATION")
    print("=" * 80)

    errors, warnings, suggestions, corrected_schema = validate_schema_file(schema_dict=test_auto_conversion)

    # Create validator instance to access the new method
    validator = SchemaValidator()
    validator.corrected_schema = corrected_schema

    print(f"\nErrors found: {len(errors)}")
    for error in errors:
        print(f"❌ {error}")

    print(f"\nWarnings found: {len(warnings)}")
    for warning in warnings:
        print(f"⚠️  {warning}")

    print(f"\nSuggestions found: {len(suggestions)}")
    for suggestion in suggestions:
        print(f"💡 {suggestion}")

    # Show corrections using enhanced method
    validator._show_applied_corrections(corrected_schema)

    # Show conversion summary
    conversions = validator.get_type_conversion_summary(corrected_schema)
    if conversions:
        print(f"\n📊 TYPE CONVERSION SUMMARY:")
        print(f"{'Table.Column':<25} {'Original':<10} {'New':<10} {'Rule Type':<15} {'Reason':<30}")
        print("-" * 95)
        for conv in conversions:
            table_col = f"{conv['table']}.{conv['column']}"
            print(
                f"{table_col:<25} {conv['original_type']:<10} {conv['new_type']:<10} {conv['rule_type']:<15} {conv['reason']:<30}")

    print(f"\n{'=' * 80}")
    print("✅ AUTO-CONVERSION FEATURES:")
    print("• String types with numeric range rules → auto-converted to int/float")
    print("• String types with numeric sequence rules → auto-converted to int")
    print("• String types with all-numeric choice rules → auto-converted to int/float")
    print("• Original types preserved in corrections array")
    print("• Prefix/suffix rules keep string types (formatting needed)")
    print("• Mixed-type choices keep string types (appropriate)")
    print(f"{'=' * 80}")

    # Test case: Choice rule with conflicting length (should remove length)
    test_choice_config = {
        "tables": [
            {
                "table_name": "customer",
                "columns": [
                    {
                        "name": "customer_code",
                        "type": "str",
                        "nullable": False,
                        "rule": {
                            "type": "choice",
                            "value": ["PREMIUM", "GOLD", "SILVER", "BRONZE"]  # 4-7 chars
                        },
                        "length": 5  # Conflicts with some choices
                    },
                    {
                        "name": "status",
                        "type": "str",
                        "nullable": False,
                        "rule": {
                            "prefix": "STS-",
                            "type": "choice",
                            "value": ["ACTIVE", "INACTIVE", "PENDING"]  # 6-8 chars + 4 prefix = 10-12
                        },
                        "length": 15  # Too large
                    }
                ]
            }
        ],
        "locale": "en_GB",
        "rows": 100,
        "output_format": "csv"
    }

    # Test case: Primary key with prefix/suffix length issues
    test_pk_config = {
        "tables": [
            {
                "table_name": "customer",
                "columns": [
                    {
                        "name": "customer_id",
                        "type": "str",
                        "constraint": ["PK"],
                        "nullable": False,
                        "rule": {
                            "prefix": "CUST-",
                            "type": "range",
                            "min": 1000,
                            "max": 999999
                        },
                        "length": 8  # Too small: CUST- (5) + 999999 (6) = 11 minimum
                    },
                    {
                        "name": "product_id",
                        "type": "str",
                        "constraint": ["PK"],
                        "nullable": False,
                        "rule": {
                            "prefix": "PRD-",
                            "suffix": "-V1",
                            "type": "uuid"
                        },
                        "length": 50  # Too large: PRD- (4) + UUID (36) + -V1 (3) = 43
                    }
                ]
            }
        ],
        "locale": "en_GB",
        "rows": 100,
        "output_format": "csv"
    }

    # Test case: Foreign key with length mismatch
    test_fk_config = {
        "tables": [
            {
                "table_name": "customer",
                "columns": [
                    {
                        "name": "customer_id",
                        "type": "str",
                        "constraint": ["PK"],
                        "nullable": False,
                        "rule": {
                            "prefix": "CUST-",
                            "type": "range",
                            "min": 1000,
                            "max": 999999
                        },
                        "length": 11
                    }
                ]
            },
            {
                "table_name": "order",
                "columns": [
                    {
                        "name": "order_id",
                        "type": "uuid",
                        "constraint": ["PK"],
                        "nullable": False,
                        "rule": "uuid",
                        "length": 36
                    },
                    {
                        "name": "customer_id",
                        "type": "str",
                        "nullable": False,
                        "length": 6  # TOO SMALL! Should be 11
                    }
                ],
                "foreign_keys": [
                    {
                        "parent_table": "customer",
                        "parent_column": "customer_id",
                        "child_column": "customer_id",
                        "one_to_one": False
                    }
                ]
            }
        ],
        "locale": "en_GB",
        "rows": 1000,
        "output_format": "csv"
    }

    test_cases = [
        ("Choice Rule Length Conflicts", test_choice_config),
        ("Primary Key Prefix/Suffix Length Issues", test_pk_config),
        ("Foreign Key Length Mismatch", test_fk_config)
    ]

    for test_name, test_config in test_cases:
        print(f"\n{'=' * 80}")
        print(f"TESTING: {test_name}")
        print(f"{'=' * 80}")

        errors, warnings, suggestions, corrected_schema = validate_schema_file(schema_dict=test_config)

        print(f"\nErrors found: {len(errors)}")
        for error in errors:
            error_type = "🔴 CRITICAL" if any(keyword in error for keyword in ["type mismatch", "length mismatch",
                                                                              "will cause data truncation"]) else "❌"
            print(f"{error_type} {error}")

        print(f"\nWarnings found: {len(warnings)}")
        for warning in warnings:
            print(f"⚠️  {warning}")

        print(f"\nSuggestions found: {len(suggestions)}")
        for suggestion in suggestions:
            print(f"💡 {suggestion}")

        # Show corrections applied
        corrections_found = False
        for table in corrected_schema.get('tables', []):
            for col in table.get('columns', []):
                if 'corrections' in col:
                    if not corrections_found:
                        print(f"\n🔧 Auto-corrections applied:")
                        corrections_found = True
                    print(f"   - {table['table_name']}.{col['name']}: {len(col['corrections'])} correction(s)")
                    for correction in col['corrections']:
                        action = correction.get('action', 'modified')
                        print(
                            f"     • {correction['field']}: {correction['old_value']} → {correction['new_value']} ({correction['reason']}) [{action}]")

        if not corrections_found:
            print(f"\n✅ No auto-corrections needed")

        print(f"\n{'=' * 50} END TEST {'=' * 50}")

    print(f"\n{'=' * 80}")
    print("SUMMARY OF ENHANCEMENTS:")
    print("✅ Choice rules with conflicting length constraints are automatically fixed")
    print("✅ Primary key length constraints are validated against prefix/suffix rules")
    print("✅ Foreign key type and length mismatches are detected as critical errors")
    print("✅ Auto-corrections preserve data integrity while optimizing constraints")
    print("✅ Automatic type conversion from str to int/float for numeric rules")
    print("✅ Original types preserved in corrections array for full audit trail")
    print(f"{'=' * 80}")