import pandas as pd

excel_config = './examples/sample_format.csv'

df = pd.read_csv(excel_config)
df.columns = [x.lower() for x in df.columns]

print(df)

print(df.to_dict())