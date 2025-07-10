import pandas as pd
import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Any, Union


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


class RuleBuilder:
    """Builds rules for data generation based on CSV data."""
    
    @staticmethod
    def build_rule(row: pd.Series) -> Optional[Union[str, Dict]]:
        """Build a rule from CSV row data."""
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
            
        # Choice rule
        if pd.notna(row.get("Choice_Values")):
            choices = [choice.strip() for choice in str(row["Choice_Values"]).split(',')]
            rule = {
                "type": "choice",
                "value": choices
            }
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
            if pd.notna(row.get("Date_Format")):
                rule["format"] = str(row["Date_Format"])
                
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


class CSVToJSONConverter:
    """Main converter class that handles CSV to JSON conversion."""
    
    def __init__(self, locale: str = "en_US", rows: int = 20000):
        self.locale = locale
        self.rows = rows
        self.schema = Schema(locale, rows)
        
    def load_csv(self, csv_file_path: str) -> pd.DataFrame:
        """Load and preprocess CSV file."""
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"CSV file not found: {csv_file_path}")
            
        df = pd.read_csv(csv_file_path)
        df.columns = [col_name.strip() for col_name in df.columns]
        df = df.where(pd.notnull(df), None)
        
        return df
        
    def process_csv_data(self, df: pd.DataFrame):
        """Process CSV data and build schema."""
        # Group by table name
        table_groups = df.groupby('Table_Name')
        
        for table_name, group in table_groups:
            table = Table(table_name)
            
            # Process each column
            for _, row in group.iterrows():
                column = self._create_column(row)
                table.add_column(column)
                
                # Handle foreign keys
                if row.get("FK") == "Yes":
                    foreign_key = self._create_foreign_key(row)
                    table.add_foreign_key(foreign_key)
                    
            self.schema.add_table(table)
            
    def _create_column(self, row: pd.Series) -> Column:
        """Create a column from CSV row data."""
        column = Column(row["Column_Name"], row["Column_Data_rype"])
        
        # Set nullable
        if row.get("Nullable") == True or str(row.get("Nullable", "")).upper() == "TRUE":
            column.set_nullable(True)
            
        # Add primary key constraint
        if row.get("PK") == "Yes":
            column.add_constraint("PK")
            
        # Set rule
        rule = RuleBuilder.build_rule(row)
        if rule:
            column.set_rule(rule)
            
        # Set length
        if pd.notna(row.get("Length")):
            column.set_length(int(row["Length"]))
            
        return column
        
    def _create_foreign_key(self, row: pd.Series) -> ForeignKey:
        """Create a foreign key from CSV row data."""
        nullable = row.get("Nullable") == True or str(row.get("Nullable", "")).upper() == "TRUE"
        
        return ForeignKey(
            parent_table=row["Parent_Table_Name"],
            parent_column=row["Parent_Column_Name"],
            child_column=row["Column_Name"],
            relationship_type=row["FK_Relationship_Type"],
            nullable=nullable
        )
        
    def convert(self, csv_file_path: str, output_file_path: str = None) -> Dict:
        """Convert CSV to JSON format."""
        # Load and process CSV
        df = self.load_csv(csv_file_path)
        self.process_csv_data(df)
        
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
        print("\nConversion Summary:")
        print(f"- Number of tables: {len(self.schema.tables)}")
        
        for table in self.schema.tables:
            print(f"  - {table.name}: {len(table.columns)} columns")
            if table.foreign_keys:
                print(f"    - Foreign keys: {len(table.foreign_keys)}")
                
        print(f"\nGeneration settings:")
        print(f"- Rows to generate: {self.schema.output_config.rows}")
        print(f"- Locale: {self.schema.output_config.locale}")
        print(f"- Output format: {self.schema.output_config.format_type}")


def main():
    """Main function to demonstrate usage."""
    # Configuration
    csv_file = "examples/sample_format.csv"
    output_file = "examples/converted_output.json"
    
    try:
        # Create converter
        converter = CSVToJSONConverter(locale="en_US", rows=20000)
        
        # Convert CSV to JSON
        result = converter.convert(csv_file, output_file)
        
        # Print summary
        converter.print_summary()
        
        # Optional: Print first table structure
        if result["tables"]:
            print(f"\nFirst table structure:")
            print(json.dumps(result["tables"][0], indent=2))
            
    except Exception as e:
        print(f"Error during conversion: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
