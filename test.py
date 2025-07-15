import pandas as pd
import pyarrow.parquet as pq

file = 'output/20250715_083241/order_header.parquet'
# Method 1: Using pandas
df = pd.read_parquet(file)
print("Data:")
print(df.to_string())
print("\nData types:")
print(df.dtypes)
print("\nShape:", df.shape)

# Method 2: Using pyarrow (more detailed schema info)
table = pq.read_table(file)
print("\nPyArrow Schema:")
print(table.schema)
print("\nData:")
print(table.to_pydict())

# Method 3: Just read schema without loading data
parquet_file = pq.ParquetFile(file)
print("\nSchema only:")
print(parquet_file.schema)
print("\nMetadata:")
print(parquet_file.metadata)