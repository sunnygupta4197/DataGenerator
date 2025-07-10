import pandas as pd
import json

# Load the CSV file
df = pd.read_csv("examples/sample_format.csv")
df.columns = [col_name.lower().strip().replace(' ', '_') for col_name in df.columns]

# Replace NaN with None for JSON compatibility
df = df.where(pd.notnull(df), None)

# Initialize the final JSON structure
final_json = {
    "locale": "en_US",
    "output_format": "csv",
    "row_count": 100,
    "tables": []
}

# Group by Database_Name and Table_Name
grouped = df.groupby(['database_name', 'table_name'])

for (db_name, table_name), group in grouped:
    table_entry = {
        "columns": [],
        "foreign_keys": []
    }

    for _, row in group.iterrows():
        print(row)
        column_info = {
            "name": row["column_name"],
            "type": row["column_data_rype"],
            "nullable": row["nullable"],
            "primary_key": row["pk"],
            "length": row["length"],
            "min_value": row["minimum_value"],
            "max_value": row["maximum_value"],
            "choice_values": row["choice_values"],
            "start_date": row["start_date"],
            "end_date": row["end_date"],
            "date_format": row["date_format"],
            "datetimestamp_format": row["datetimestamp_format"],
            "value_prefix": row["value_prefix"],
            "value_suffix": row["value_suffix"]
        }
        rule_info = {
            "rule": row["rule"]
        }

        column_info["rule"] = rule_info
        table_entry["columns"].append(column_info)

        # Add foreign key info if FK is present
        if row["fk"] == "Yes":
            fk_info = {
                "column": row["column_name"],
                "references": {
                    "table": row["parent_table_name"],
                    "column": row["parent_column_name"]
                },
                "relationship_type": row["fk_relationship_type"]
            }
            table_entry["foreign_keys"].append(fk_info)

    print(table_entry)
    final_json["tables"].append({"table_name": table_name, **table_entry})

# Save the final JSON to a file
with open("examples/converted_output.json", "w") as f:
    json.dump(final_json, f, indent=4)

print("Conversion completed. JSON saved to 'converted_output.json'.")
