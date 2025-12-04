import pandas as pd
df = pd.read_parquet('parquet_data/sensor_20251204_213830.parquet')
print(df.head())
