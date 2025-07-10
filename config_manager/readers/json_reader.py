import json
from typing import Dict, Any
from datetime import datetime

from config_manager.choose_file_loader import ChoiceFileLoader
from config_manager.schema_validator import SchemaValidator


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