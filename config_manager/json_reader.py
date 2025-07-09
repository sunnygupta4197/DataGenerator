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
                if not self._is_valid_schema_format(config):
                    print("Config schema is not correct")
                    exit(1)
                return config

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
    validator._show_applied_corrections()

    # Show conversion summary
    conversions = validator.get_type_conversion_summary()
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