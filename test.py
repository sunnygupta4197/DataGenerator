import pandas as pd
import json
import os
from datetime import datetime

def convert_csv_to_json(csv_file_path, output_file_path=None, rows=20000):
    """
    Convert CSV file to the specified JSON format for data generation.
    
    Args:
        csv_file_path (str): Path to the input CSV file
        output_file_path (str): Path for the output JSON file (optional)
        rows (int): Number of rows to generate (default: 20000)
    
    Returns:
        dict: The converted JSON structure
    """
    
    # Load the CSV file
    df = pd.read_csv(csv_file_path)
    
    # Clean column names
    df.columns = [col_name.strip() for col_name in df.columns]
    
    # Replace NaN with None for JSON compatibility
    df = df.where(pd.notnull(df), None)
    
    # Initialize the final JSON structure
    final_json = {
        "tables": [],
        "locale": "en_US",
        "rows": rows,
        "output": {
            "format": "csv",
            "enable_fixed_width": True,
            "directory": "./output/{timestamp}",
            "filename_template": "{table_name}",
            "alignment": "left",
            "numeric_alignment": "left",
            "default_column_width": 30
        }
    }
    
    # Group by Table_Name
    table_groups = {}
    
    for _, row in df.iterrows():
        table_name = row["Table_Name"]
        
        if table_name not in table_groups:
            table_groups[table_name] = {
                "columns": [],
                "foreign_keys": []
            }
        
        # Build column information
        column = {
            "name": row["Column_Name"],
            "type": row["Column_Data_rype"]
        }
        
        # Add nullable if TRUE
        if row["Nullable"] == True or str(row["Nullable"]).upper() == "TRUE":
            column["nullable"] = True
        
        # Add constraints (PK)
        if row["PK"] == "Yes":
            column["constraint"] = ["PK"]
        
        # Build rule object
        rule = None
        
        # Handle different rule types
        if row["Rule"] and pd.notna(row["Rule"]):
            rule = row["Rule"]
        elif pd.notna(row["Minimum_Value"]) and pd.notna(row["Maximum_Value"]):
            rule = {
                "type": "range",
                "min": row["Minimum_Value"],
                "max": row["Maximum_Value"]
            }
        elif pd.notna(row["Choice_Values"]):
            choices = [choice.strip() for choice in str(row["Choice_Values"]).split(',')]
            rule = {
                "type": "choice",
                "value": choices
            }
        elif pd.notna(row["Start_Date"]) or pd.notna(row["End_Date"]):
            rule = {
                "type": "date_range"
            }
            if pd.notna(row["Start_Date"]):
                rule["start"] = str(row["Start_Date"])
            if pd.notna(row["End_Date"]):
                rule["end"] = str(row["End_Date"])
            if pd.notna(row["Date_Format"]):
                rule["format"] = str(row["Date_Format"])
        
        # Add rule to column if exists
        if rule:
            column["rule"] = rule
        
        # Add length if specified
        if pd.notna(row["Length"]):
            column["length"] = int(row["Length"])
        
        # Add prefix/suffix if specified
        if pd.notna(row["Value_Prefix"]) or pd.notna(row["Value_Suffix"]):
            if not rule:
                column["rule"] = {}
            elif isinstance(column["rule"], str):
                column["rule"] = {"type": column["rule"]}
            
            if pd.notna(row["Value_Prefix"]):
                column["rule"]["prefix"] = str(row["Value_Prefix"])
            if pd.notna(row["Value_Suffix"]):
                column["rule"]["suffix"] = str(row["Value_Suffix"])
        
        table_groups[table_name]["columns"].append(column)
        
        # Handle foreign keys
        if row["FK"] == "Yes":
            foreign_key = {
                "parent_table": row["Parent_Table_Name"],
                "parent_column": row["Parent_Column_Name"],
                "child_column": row["Column_Name"],
                "relationship_type": row["FK_Relationship_Type"],
                "nullable": row["Nullable"] == True or str(row["Nullable"]).upper() == "TRUE"
            }
            
            table_groups[table_name]["foreign_keys"].append(foreign_key)
    
    # Convert grouped data to final format
    for table_name, table_data in table_groups.items():
        table_entry = {
            "table_name": table_name,
            "columns": table_data["columns"]
        }
        
        if table_data["foreign_keys"]:
            table_entry["foreign_keys"] = table_data["foreign_keys"]
        
        final_json["tables"].append(table_entry)
    
    # Save to file if output path is provided
    if output_file_path:
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(final_json, f, indent=2, ensure_ascii=False)
        print(f"JSON file saved to: {output_file_path}")
    
    return final_json

def main():
    """
    Main function to demonstrate usage
    """
    # Example usage
    csv_file = "examples/sample_format.csv"  # Update this path
    output_file = "examples/converted_output.json"  # Update this path
    
    # Check if CSV file exists
    if not os.path.exists(csv_file):
        print(f"CSV file not found: {csv_file}")
        print("Please update the csv_file path in the script.")
        return
    
    try:
        # Convert CSV to JSON
        result = convert_csv_to_json(csv_file, output_file)
        
        # Print summary
        print("\nConversion Summary:")
        print(f"- Number of tables: {len(result['tables'])}")
        for table in result['tables']:
            print(f"  - {table['table_name']}: {len(table['columns'])} columns")
            if 'foreign_keys' in table:
                print(f"    - Foreign keys: {len(table['foreign_keys'])}")
        
        print(f"\nGeneration settings:")
        print(f"- Rows to generate: {result['rows']}")
        print(f"- Locale: {result['locale']}")
        print(f"- Output format: {result['output']['format']}")
        
    except Exception as e:
        print(f"Error during conversion: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
