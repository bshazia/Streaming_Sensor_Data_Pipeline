import requests
import json
from kafka import KafkaProducer

KAFKA_BROKER = '192.168.1.4:9092'

TOPIC = 'sensor-events'
STREAM_URL = 'http://10.0.0.2:30080/stream'  # your stream URL from Streamlit app

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    key_serializer=lambda k: k.encode(),
    value_serializer=lambda v: json.dumps(v).encode()
)

with requests.get(STREAM_URL, stream=True) as response:
    for line in response.iter_lines():
        if line:
            event = json.loads(line)
           # print(event) 

            for sensor_name, value in event['values'].items():
                sensor_event = {
                    'seq': event['seq'],
                    'ts': event['ts'],
                    'pattern': event['pattern'],
                    'sensor': sensor_name,
                    'value': value
                }
                key = sensor_name  #  partitioning by sensor
                producer.send(TOPIC, key=key, value=sensor_event)
                print(f"Sent: {sensor_event}")
                
                