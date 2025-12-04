import json
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime
import os

TOPIC = 'sensor-events'
KAFKA_BROKER = '100.111.0.0:9092' # use your id here this is replcaed with dumy
OUTPUT_DIR = 'parquet_data'
os.makedirs(OUTPUT_DIR, exist_ok=True)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v)
)

buffer = []
for message in consumer:
    buffer.append(message.value)
    if len(buffer) >= 100:  # batch size
        df = pd.DataFrame(buffer)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(OUTPUT_DIR, f'sensor_{timestamp}.parquet')
        df.to_parquet(file_path, index=False)
        print(f"Written {len(buffer)} events to {file_path}")
        buffer = []
