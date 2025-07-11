import pandas as pd
import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from abc import ABC, abstractmethod
from enum import Enum


class Column:
    """Represents a database column with its properties and rules."""

    def __init__(self, name: str, data_type: str):
        self.name = name
        self.data_type = data_type
        self.nullable = False
        self.constraints = []
        self.rule = None
        self.length = None
        self.default = None
        self.sensitivity = None
        self.null_percentage = None

    def set_nullable(self, nullable: bool):
        """Set the nullable property."""
        self.nullable = nullable
        return self

    def add_constraint(self, constraint: str):
        """Add a constraint like 'PK'."""
        if constraint not in self.constraints:
            self.constraints.append(constraint)
        return self

    def set_rule(self, rule: Union[str, Dict]):
        """Set the rule for data generation."""
        self.rule = rule
        return self

    def set_length(self, length: int):
        """Set the length constraint."""
        self.length = length
        return self

    def set_default(self, default: Any):
        """Set the default value."""
        self.default = default
        return self

    def set_sensitivity(self, sensitivity: str):
        """Set the sensitivity level (e.g., 'PII')."""
        self.sensitivity = sensitivity
        return self

    def set_null_percentage(self, percentage: int):
        """Set the null percentage for nullable columns."""
        self.null_percentage = percentage
        return self

    def to_dict(self) -> Dict:
        """Convert column to dictionary representation."""
        result = {
            "name": self.name,
            "type": self.data_type
        }

        if self.nullable:
            result["nullable"] = True

        if self.constraints:
            result["constraint"] = self.constraints

        if self.rule:
            result["rule"] = self.rule

        if self.length:
            result["length"] = self.length

        if self.default:
            result["default"] = self.default

        if self.sensitivity:
            result["sensitivity"] = self.sensitivity

        if self.null_percentage:
            result["null_percentage"] = self.null_percentage

        return result


class ForeignKey:
    """Represents a foreign key relationship."""

    def __init__(self, parent_table: str, parent_column: str, child_column: str,
                 relationship_type: str, nullable: bool = False):
        self.parent_table = parent_table
        self.parent_column = parent_column
        self.child_column = child_column
        self.relationship_type = relationship_type
        self.nullable = nullable

    def to_dict(self) -> Dict:
        """Convert foreign key to dictionary representation."""
        return {
            "parent_table": self.parent_table,
            "parent_column": self.parent_column,
            "child_column": self.child_column,
            "relationship_type": self.relationship_type,
            "nullable": self.nullable
        }


class Table:
    """Represents a database table with columns and foreign keys."""

    def __init__(self, name: str, file_name: str = None):
        self.name = name
        self.file_name = file_name
        self.columns: List[Column] = []
        self.foreign_keys: List[ForeignKey] = []

    def add_column(self, column: Column):
        """Add a column to the table."""
        self.columns.append(column)
        return self

    def add_foreign_key(self, foreign_key: ForeignKey):
        """Add a foreign key relationship."""
        self.foreign_keys.append(foreign_key)
        return self

    def get_column(self, name: str) -> Optional[Column]:
        """Get a column by name."""
        for column in self.columns:
            if column.name == name:
                return column
        return None

    def to_dict(self) -> Dict:
        """Convert table to dictionary representation."""
        result = {
            "table_name": self.name,
            "columns": [col.to_dict() for col in self.columns]
        }

        if self.file_name:
            result["file_name"] = self.file_name

        if self.foreign_keys:
            result["foreign_keys"] = [fk.to_dict() for fk in self.foreign_keys]

        return result


class OutputConfig:
    """Configuration for output generation."""

    def __init__(self, format_type: str = "csv", rows: int = 20000, locale: str = "en_US"):
        self.format_type = format_type
        self.rows = rows
        self.locale = locale
        self.enable_fixed_width = True
        self.directory = "./output/{timestamp}"
        self.filename_template = "{table_name}"
        self.alignment = "left"
        self.numeric_alignment = "left"
        self.default_column_width = 30

    def to_dict(self) -> Dict:
        """Convert output config to dictionary."""
        return {
            "format": self.format_type,
            "enable_fixed_width": self.enable_fixed_width,
            "directory": self.directory,
            "filename_template": self.filename_template,
            "alignment": self.alignment,
            "numeric_alignment": self.numeric_alignment,
            "default_column_width": self.default_column_width
        }


class Schema:
    """Represents the complete database schema."""

    def __init__(self, locale: str = "en_US", rows: int = 20000):
        self.tables: List[Table] = []
        self.output_config = OutputConfig(rows=rows, locale=locale)

    def add_table(self, table: Table):
        """Add a table to the schema."""
        self.tables.append(table)
        return self

    def get_table(self, name: str) -> Optional[Table]:
        """Get a table by name."""
        for table in self.tables:
            if table.name == name:
                return table
        return None

    def to_dict(self) -> Dict:
        """Convert schema to dictionary representation."""
        return {
            "tables": [table.to_dict() for table in self.tables],
            "locale": self.output_config.locale,
            "rows": self.output_config.rows,
            "output": self.output_config.to_dict()
        }


class RuleBuilder:
    """Builds rules for data generation based on CSV/Excel data."""

    @staticmethod
    def build_rule(row: pd.Series) -> Optional[Union[str, Dict]]:
        """Build a rule from CSV/Excel row data."""
        # Direct rule specification
        if pd.notna(row.get("Rule")):
            return str(row["Rule"])

        # Range rule
        if pd.notna(row.get("Minimum_Value")) and pd.notna(row.get("Maximum_Value")):
            rule = {
                "type": "range",
                "min": row["Minimum_Value"],
                "max": row["Maximum_Value"]
            }
            return RuleBuilder._add_prefix_suffix(rule, row)

        # Choice rule with probabilities
        if pd.notna(row.get("Choice_Values")):
            choices = [choice.strip() for choice in str(row["Choice_Values"]).split(',')]
            rule = {
                "type": "choice",
                "value": choices
            }

            # Add probabilities if present
            if pd.notna(row.get("Choice_Probabilities")):
                try:
                    probs = eval(row["Choice_Probabilities"])  # Handle dict string
                    if isinstance(probs, dict):
                        rule["probabilities"] = probs
                except:
                    pass

            return RuleBuilder._add_prefix_suffix(rule, row)

        # Date range rule
        if pd.notna(row.get("Start_Date")) or pd.notna(row.get("End_Date")):
            rule = {"type": "date_range"}

            if pd.notna(row.get("Start_Date")):
                rule["start"] = str(row["Start_Date"])
            if pd.notna(row.get("End_Date")):
                rule["end"] = str(row["End_Date"])
            if pd.notna(row.get("Date_Format")):
                rule["format"] = str(row["Date_Format"])

            return RuleBuilder._add_prefix_suffix(rule, row)

        # Timestamp range rule
        if pd.notna(row.get("Datetimestamp_Format")):
            rule = {"type": "timestamp_range"}

            if pd.notna(row.get("Start_Date")):
                rule["start"] = str(row["Start_Date"])
            if pd.notna(row.get("Datetimestamp_Format")):
                rule["format"] = str(row["Datetimestamp_Format"])

            return RuleBuilder._add_prefix_suffix(rule, row)

        # Regex rule
        if pd.notna(row.get("Regex_Pattern")):
            rule = {
                "type": "regex",
                "regex": str(row["Regex_Pattern"])
            }
            return RuleBuilder._add_prefix_suffix(rule, row)

        # Only prefix/suffix
        if pd.notna(row.get("Value_Prefix")) or pd.notna(row.get("Value_Suffix")):
            rule = {}
            return RuleBuilder._add_prefix_suffix(rule, row)

        return None

    @staticmethod
    def _add_prefix_suffix(rule: Dict, row: pd.Series) -> Dict:
        """Add prefix and suffix to rule if specified."""
        if pd.notna(row.get("Value_Prefix")):
            rule["prefix"] = str(row["Value_Prefix"])
        if pd.notna(row.get("Value_Suffix")):
            rule["suffix"] = str(row["Value_Suffix"])
        return rule


class FileFormat(Enum):
    """Supported file formats."""
    CSV = "csv"
    EXCEL_SINGLE_SHEET = "excel_single"
    EXCEL_MULTIPLE_SHEETS = "excel_multiple"


class DataProcessor(ABC):
    """Abstract base class for data processors."""

    @abstractmethod
    def process_data(self, data: Any) -> Schema:
        """Process data and return schema."""
        pass

    def _create_column(self, row: pd.Series) -> Column:
        """Create a column from row data."""
        column = Column(row["column_name"], row["column_data_type"])

        # Set nullable
        if row.get("nullable") == True or str(row.get("nullable", "")).upper() == "TRUE":
            column.set_nullable(True)

        # Add primary key constraint
        if row.get("pk") == "Yes":
            column.add_constraint("PK")

        # Set rule
        rule = RuleBuilder.build_rule(row)
        if rule:
            column.set_rule(rule)

        # Set length
        if pd.notna(row.get("length")):
            column.set_length(int(row["length"]))

        # Set default
        if pd.notna(row.get("default_value")):
            column.set_default(str(row["default_value"]))

        # Set sensitivity
        if pd.notna(row.get("sensitivity")):
            column.set_sensitivity(str(row["sensitivity"]))

        # Set null percentage
        if pd.notna(row.get("null_percentage")):
            column.set_null_percentage(int(row["null_percentage"]))

        return column

    def _create_foreign_key(self, row: pd.Series) -> ForeignKey:
        """Create a foreign key from row data."""
        nullable = row.get("nullable") == True or str(row.get("nullable", "")).upper() == "TRUE"

        return ForeignKey(
            parent_table=row["parent_table_name"],
            parent_column=row["parent_column_name"],
            child_column=row["column_name"],
            relationship_type=row["fk_relationship_type"],
            nullable=nullable
        )


class CSVProcessor(DataProcessor):
    """Processes CSV data with database and table name columns."""

    def __init__(self, locale: str = "en_US", rows: int = 20000):
        self.locale = locale
        self.rows = rows

    def process_data(self, df: pd.DataFrame) -> Schema:
        """Process CSV data and build schema."""
        schema = Schema(self.locale, self.rows)

        # Group by table name
        table_groups = df.groupby('table_name')

        for table_name, group in table_groups:
            table = Table(table_name)

            # Process each column
            for _, row in group.iterrows():
                column = self._create_column(row)
                table.add_column(column)

                # Handle foreign keys
                if row.get("fk") == "Yes":
                    foreign_key = self._create_foreign_key(row)
                    table.add_foreign_key(foreign_key)

            schema.add_table(table)

        return schema


class ExcelMultiSheetProcessor(DataProcessor):
    """Processes Excel data with multiple sheets (each sheet = table)."""

    def __init__(self, locale: str = "en_US", rows: int = 20000):
        self.locale = locale
        self.rows = rows

    def process_data(self, excel_data: Dict[str, pd.DataFrame]) -> Schema:
        """Process Excel multi-sheet data and build schema."""
        schema = Schema(self.locale, self.rows)

        for sheet_name, df in excel_data.items():
            # Skip sheets that don't look like table definitions
            if not self._is_table_definition_sheet(df):
                continue

            table = Table(sheet_name)

            # Process each column
            for _, row in df.iterrows():
                column = self._create_column(row)
                table.add_column(column)

                # Handle foreign keys
                if row.get("fk") == "Yes":
                    foreign_key = self._create_foreign_key(row)
                    table.add_foreign_key(foreign_key)

            schema.add_table(table)

        return schema

    def _is_table_definition_sheet(self, df: pd.DataFrame) -> bool:
        """Check if sheet contains table definition data."""
        required_columns = ["column_name", "column_data_type"]
        return all(col in df.columns for col in required_columns)


class FileToJSONConverter:
    """Main converter class that handles CSV/Excel to JSON conversion."""

    def __init__(self, locale: str = "en_US", rows: int = 20000):
        self.locale = locale
        self.rows = rows
        self.schema = None

    def detect_file_format(self, file_path: str) -> FileFormat:
        """Detect the file format and structure."""
        base_file_name, file_ext = os.path.splitext(file_path)
        if file_ext == '.csv':
            return FileFormat.CSV
        elif file_ext in ['.xlsx', '.xls']:
            # Check if it's single sheet or multiple sheets
            excel_file = pd.ExcelFile(file_path)
            if len(excel_file.sheet_names) == 1:
                # Check if single sheet has Table_Name column
                df = pd.read_excel(file_path, sheet_name=excel_file.sheet_names[0])
                if 'Table_Name' in df.columns:
                    return FileFormat.EXCEL_SINGLE_SHEET
                else:
                    return FileFormat.EXCEL_MULTIPLE_SHEETS
            else:
                return FileFormat.EXCEL_MULTIPLE_SHEETS
        else:
            raise ValueError(f"Unsupported file format: {file_path}")

    def _convert_columns_slug(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = df.columns = [col_name.strip().lower().replace(' ', '_') for col_name in df.columns]
        return df

    def load_data(self, file_path: str) -> Any:
        """Load data from file based on format."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        file_format = self.detect_file_format(file_path)

        if file_format == FileFormat.CSV:
            df = pd.read_csv(file_path)
            df = self._convert_columns_slug(df)
            return df.where(pd.notnull(df), None)

        elif file_format == FileFormat.EXCEL_SINGLE_SHEET:
            df = pd.read_excel(file_path)
            df = self._convert_columns_slug(df)
            return df.where(pd.notnull(df), None)

        elif file_format == FileFormat.EXCEL_MULTIPLE_SHEETS:
            excel_file = pd.ExcelFile(file_path)
            data = {}
            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                df = self._convert_columns_slug(df)
                data[sheet_name] = df.where(pd.notnull(df), None)
            return data
        return None

    def process_data(self, data: Any, file_format: FileFormat):
        """Process data based on format."""
        if file_format in [FileFormat.CSV, FileFormat.EXCEL_SINGLE_SHEET]:
            processor = CSVProcessor(self.locale, self.rows)
            self.schema = processor.process_data(data)
        elif file_format == FileFormat.EXCEL_MULTIPLE_SHEETS:
            processor = ExcelMultiSheetProcessor(self.locale, self.rows)
            self.schema = processor.process_data(data)

    def convert(self, file_path: str, output_file_path: str = None) -> Dict:
        """Convert file to JSON format."""
        # Detect format and load data
        file_format = self.detect_file_format(file_path)
        data = self.load_data(file_path)

        # Process data
        self.process_data(data, file_format)

        # Generate JSON
        result = self.schema.to_dict()

        # Save to file if specified
        if output_file_path:
            self.save_json(result, output_file_path)

        return result

    def save_json(self, data: Dict, output_file_path: str):
        """Save JSON data to file."""
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print(f"JSON file saved to: {output_file_path}")

    def print_summary(self):
        """Print conversion summary."""
        if not self.schema:
            print("No schema available. Run convert() first.")
            return

        print("\n" + "=" * 50)
        print("CONVERSION SUMMARY")
        print("=" * 50)
        print(f"Number of tables: {len(self.schema.tables)}")

        for table in self.schema.tables:
            print(f"\n📊 Table: {table.name}")
            print(f"   └─ Columns: {len(table.columns)}")

            # Show column details
            for col in table.columns:
                constraints = f" [{', '.join(col.constraints)}]" if col.constraints else ""
                nullable = " (nullable)" if col.nullable else ""
                print(f"      • {col.name}: {col.data_type}{constraints}{nullable}")

            if table.foreign_keys:
                print(f"   └─ Foreign Keys: {len(table.foreign_keys)}")
                for fk in table.foreign_keys:
                    print(f"      • {fk.child_column} -> {fk.parent_table}.{fk.parent_column}")

        print(f"\n🔧 Generation Settings:")
        print(f"   • Rows to generate: {self.schema.output_config.rows:,}")
        print(f"   • Locale: {self.schema.output_config.locale}")
        print(f"   • Output format: {self.schema.output_config.format_type}")
        print("=" * 50)


def main():
    """Main function to demonstrate usage."""

    # Example file paths - update these
    files_to_convert = [
        ("examples/sample_format.csv", "examples/output_from_csv.json"),
        ("examples/single_sheet.xlsx", "examples/output_from_single_excel.json"),
        ("examples/multiple_sheets.xlsx", "examples/output_from_multi_excel.json")
    ]

    for input_file, output_file in files_to_convert:
        print(f"\n{'=' * 60}")
        print(f"Converting: {input_file}")
        print(f"{'=' * 60}")

        try:
            # Create converter
            converter = FileToJSONConverter(locale="en_US", rows=25000)

            # Auto-detect format and convert
            result = converter.convert(input_file, output_file)

            # Print detailed summary
            converter.print_summary()

        except FileNotFoundError:
            print(f"⚠️  File not found: {input_file}")
        except Exception as e:
            print(f"❌ Error converting {input_file}: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()