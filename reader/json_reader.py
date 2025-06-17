import json
import re
from datetime import datetime


class JSONConfigReader:
    def __init__(self, file_name, db_file=None):
        self.file_name = file_name

    def load_config(self):
        try:
            with open(self.file_name) as json_file:
                config = json.load(json_file)
                json_converter = JSONTemplateConverter()
                is_valid, errors = json_converter.validate_template(config)
                if not is_valid:
                    config = json_converter.convert(config)
                    is_valid, errors = json_converter.validate_template(config)
                if is_valid:
                    # self.create_table(config)
                    return config
                elif errors:
                    raise ValueError(errors)
                return None
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file {self.file_name} not found.")
        except json.decoder.JSONDecodeError as e:
            raise json.decoder.JSONDecodeError(f'Invalid JSON in config file: {e}')


class JSONTemplateConverter:
    def __init__(self):
        self.conversion_error = []

    def convert(self, json_config):
        print('Conversion of config file is required')

        self.conversion_error = []

        template = {
            "tables": [
                {
                    "table_name": json_config.get("table_name", ""),
                    "columns": [],
                    "foreign_keys": [],
                    "locale": json_config.get("locale"),
                    "rows": json_config.get("rows")
                }
            ]
        }

        columns = json_config.get("columns", {})
        primary_keys = json_config.get("primary_keys", [])
        required_columns = json_config.get("required_columns", [])
        rules = json_config.get("rules", {})
        validations = rules.get("validations", {})
        distributions = rules.get("distributions", {})
        default_values = rules.get("default_values", {})
        formatting = rules.get("formatting", {})
        sensitivity = rules.get("sensitivity", {})
        attributes = rules.get("attributes", {})
        dependencies = rules.get("dependencies", {})
        unique_constraints = rules.get("unique_constraints", [])
        null_ratio = rules.get("null_ratio", {})

        for column_name, column_info in columns.items():
            converted_column = self._convert_column(column_name, column_info, primary_keys, required_columns, rules, attributes, dependencies, unique_constraints, null_ratio, validations, formatting, sensitivity, distributions, default_values)
            template["tables"][0]["columns"].append(converted_column)

        foreign_keys = json_config.get("foreign_keys", [])
        for foreign_key in foreign_keys:
            converted_fk = self._convert_foreign_key(foreign_key)
            template["tables"][0]["foreign_keys"].append(converted_fk)

        return template

    def _convert_column(self, column_name, column_info, primary_keys, required_columns, rules, attributes, dependencies, unique_constraints, null_ratio, validations, formatting, sensitivity, distributions, default_values):
        converted_column = {
            "name": column_name,
            "type": self._convert_data_type(column_info.get("type", "str")),
            "nullable": column_info.get("nullable", column_name not in required_columns and column_name not in primary_keys),
        }

        constraints = []
        if column_name in primary_keys:
            constraints.append("PK")
        if constraints:
            converted_column["constraints"] = constraints

        rule = self._generate_rule(column_name, column_info, default_values, attributes, null_ratio, validations, distributions, formatting)
        if rule:
            converted_column["rule"] = rule

        conditional_rules = self._generate_conditional_rules(column_name, dependencies.get(column_name, {}))
        if conditional_rules:
            converted_column["conditional_rules"] = conditional_rules

        if column_name in null_ratio and null_ratio[column_name] > 0:
            converted_column["nullable"] = True

        if column_name in default_values:
            converted_column["default"] = default_values[column_name]

        if column_name in sensitivity:
            converted_column["sensitivity"] = sensitivity[column_name]

        return converted_column

    def _convert_data_type(self, data_type):
        type_mapping = {
            "string": "str",
            "boolean": "bool",
        }
        return type_mapping.get(data_type.lower(), data_type)

    def _generate_rule(self, column_name, column_info, default_values, attributes, null_ratio, validations, distributions, formatting):
        if column_name in validations:
            validation_info = validations[column_name]

            if isinstance(validation_info, dict) and "regex" in validation_info:
                return {"type": column_name, "regex": validation_info["regex"]}

            if isinstance(validation_info, dict):
                if "min_value" in validation_info or "max_value" in validation_info:
                    rule = {"type": "range"}
                    if "min_value" in validation_info:
                        rule["min"] = validation_info["min_value"]
                    if "max_value" in validation_info:
                        rule["max"] = validation_info["max_value"]
                    return rule

        if column_name in distributions:
            distribution_info = distributions[column_name]
            if isinstance(distribution_info, dict) and "values" in distribution_info:
                rule = {"type": "choice", "value": distribution_info["values"]}
                if "probabilities" in distribution_info:
                    rule["probabilities"] = dict(zip(distribution_info["values"], distribution_info["probabilities"]))
                return rule

            if isinstance(distribution_info, str):
                if distribution_info == "lognormal":
                    return {"type": "lognormal"}
                elif distribution_info == "normal":
                    return {"type": "normal"}
                elif distribution_info == "uniform":
                    return {"type": "uniform"}

            if column_name in default_values:
                default_value = default_values[column_name]
                if isinstance(default_value, str) and self._is_date_string(default_value):
                    return {"type": "date_range", "start": default_value}

            if column_name in formatting:
                format_pattern = formatting[column_name]
                if "YYYY-MM-DD" in format_pattern or "date" in format_pattern.lower():
                    return {"type": "date_range", "start": "2020-01-01"}

        if "email" in column_name.lower():
            return "email"
        elif "name" in column_name.lower():
            return "name"
        elif "phone" in column_name.lower():
            return "phone"
        elif "date" in column_name.lower() or "birth" in column_name.lower():
            return {"type": "date_range", "start": "1950-01-01"}
        elif "registration" in column_name.lower():
            return {"type": "date_range", "start": "2020-01-01"}
        elif "age" in column_name.lower():
            return {"type": "range", "min": 18, "max": 90}
        elif "income" in column_name.lower():
            return {"type": "range", "min": 1000, "max": 10000000}
        elif "gender" == column_name.lower():
            return "gender"
        elif column_name.lower() in ["status"]:
            return "word"
        elif "id" in column_name.lower() and column_name.lower().endswith("id"):
            return {"type": "range", "min": 0, "max": 100000}
        return column_name.lower()

    def _generate_conditional_rules(self, column_name, dependencies):
        conditional_rules = []
        if isinstance(dependencies, dict):
            dependencies = [dependencies]

        for dependency in dependencies:
            depends_on = dependency.get("depends_on", "")
            dep_rules = dependency.get("rules", {})
            for condition, rule_text in dep_rules.items():
                if depends_on.lower() in condition.lower() or column_name.lower() in rule_text.lower() or any(keyword in rule_text.lower() for keyword in [column_name.lower()]):
                    when_conditions = self._parse_rule_from_key(condition, depends_on)
                    if when_conditions:
                        conditional_rule = {
                            "when": when_conditions,
                            "then": {"rule": self._parse_rule_from_text(rule_text)}
                        }
                        conditional_rules.append(conditional_rule)
        return conditional_rules if conditional_rules else None

    def _parse_rule_from_key(self, condition, depends_on):
        conditions = []
        if "if" in condition and "then" in condition:
            condition_part = condition.split("then")[0].replace("if", "").strip()
            if ">=" in condition_part and "<=" in condition_part:
                if "and" in condition_part:
                    parts = condition_part.split("and")
                    if len(parts) == 2:
                        first_part = parts[0].strip()
                        if ">=" in first_part:
                            ge_parts = first_part.split(">=")
                            if len(ge_parts) == 2:
                                col = ge_parts[0].strip()
                                min_val = ge_parts[1].strip()
                                second_part = parts[1].strip()
                                if "<=" in second_part:
                                    le_parts = second_part.split("<=")
                                    if len(le_parts) == 2:
                                        max_val = le_parts[1].strip()
                                        try:
                                            min_val = int(min_val)
                                            max_val = int(max_val)
                                            conditions.append({
                                                "column": col,
                                                "min": min_val,
                                                "max": max_val,
                                                "operator": "range"
                                            })
                                        except ValueError:
                                            pass
            elif "<" in condition_part:
                parts = condition_part.split("<")
                if len(parts) == 2:
                    col = parts[0].strip()
                    val = parts[1].strip()
                    try:
                        value = int(val)
                        conditions.append({
                            "column": col,
                            "value": value,
                            "operator": "less_than"
                        })
                    except ValueError:
                        pass
            elif ">" in condition_part:
                parts = condition_part.split(">")
                if len(parts) == 2:
                    col = parts[0].strip()
                    val = parts[1].strip()
                    try:
                        value = int(val)
                        conditions.append({
                            "column": col,
                            "value": value,
                            "operator": "greater_than"
                        })
                    except ValueError:
                        pass
            elif "=" in condition_part:
                parts = condition_part.split("=")
                if len(parts) == 2:
                    col = parts[0].strip()
                    val = parts[1].strip()
                    try:
                        value = int(val)
                        conditions.append({
                            "column": col,
                            "value": value,
                            "operator": "equals"
                        })
                    except ValueError:
                        conditions.append({
                            "column": col,
                            "value": val.strip('"\''),
                        })
        return conditions

    def _parse_rule_from_text(self, rule_text):
        if "<" in rule_text and ">" in rule_text:
            numbers = re.findall(r"\d+", rule_text)
            if len(numbers) >= 2:
                return {"type": "range", "min": int(numbers[0]), "max": int(numbers[1])}
        elif "<" in rule_text:
            numbers = re.findall(r"\d+", rule_text)
            if numbers:
                return {"type": "range", "max": int(numbers[0])}
        elif ">" in rule_text:
            numbers = re.findall(r"\d+", rule_text)
            if numbers:
                return {"type": "range", "min": int(numbers[0])}
        elif "-" in rule_text:
            min_val = re.findall(r"\d+", rule_text.split("-")[0])
            max_val = re.findall(r"\d+", rule_text.split("-")[1])
            if min_val and max_val:
                return {"type": "range", "min": int(min_val[0]), "max": int(max_val[0])}
        return "default"

    def _convert_foreign_key(self, foreign_key):
        references = foreign_key.get("references", {})
        return {
            "parent_table": references.get("table", ""),
            "parent_column": references.get("column", ""),
            "child_column": references.get("column", "")
        }

    def _is_date_string(self, date_str):
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    def validate_template(self, json_config):
        errors = []
        if "tables" not in json_config:
            errors.append("Tables section must be defined")
            return False, errors

        if not isinstance(json_config["tables"], list):
            errors.append("Tables section must be a list")
            return False, errors

        for i, table in enumerate(json_config["tables"]):
            table_errors = self._validate_table(table, i)
            errors.extend(table_errors)

        return len(errors) == 0, errors

    def _validate_table(self, table, table_index):
        errors = []
        table_prefix = f"Table {table_index}"

        required_fields = ["table_name", "columns"]
        for field in required_fields:
            if field not in table:
                errors.append(f"{table_prefix} must contain {field}")

        if "table_name" in table and not isinstance(table["table_name"], str):
            errors.append(f"{table_prefix} must contain {table['table_name']} and should be a string")

        if "columns" in table:
            if not isinstance(table["columns"], list):
                errors.append(f"{table_prefix} must contain {table['columns']} and should be a list")
            else:
                for j, column in enumerate(table["columns"]):
                    column_error = self._validate_columns(column, table_index, j)
                    errors.extend(column_error)

        if "foreign_keys" in table:
            if not isinstance(table["foreign_keys"], list):
                errors.append(f"{table_prefix} must contain {table['foreign_keys']} and should be a list")
            else:
                for k, fk, in enumerate(table["foreign_keys"]):
                    fk_errors = self._validate_foreign_key(fk, table_index, k)
                    errors.extend(fk_errors)
        return errors

    def _validate_columns(self, column, table_index, column_index):
        errors = []
        column_prefix = f"Table {table_index}, Column {column_index}"

        required_fields = ["name", "type", "nullable"]
        for field in required_fields:
            if field not in column:
                errors.append(f"Table {column_prefix} must contain {field}")

        if "name" in column and not isinstance(column["name"], str):
            errors.append(f"{column_prefix} must contain {column["name"]} and should be a string")

        if "type" in column and not isinstance(column["type"], str):
            errors.append(f"{column_prefix} must contain {column['type']} and should be a string")

        if "nullable" in column and not isinstance(column["nullable"], bool):
            errors.append(f"{column_prefix} must contain {column['nullable']} and should be a boolean")

        if "constraints" in column and not isinstance(column["constraints"], list):
            errors.append(f"{column_prefix} must contain {column['constraints']} and should be a list")

        return errors

    def _validate_foreign_key(self, foreign_key, table_index, foreign_index):
        errors = []
        fk_prefix = f"Table {table_index}, Foreign Key {foreign_index}"

        required_fields = ["parent_table", "parent_column", "child_column"]
        for field in required_fields:
            if field not in foreign_key:
                errors.append(f"{fk_prefix} must contain {field}")
            elif not isinstance(foreign_key[field], str):
                errors.append(f"{fk_prefix} must contain {field} and should be a string")
        return errors