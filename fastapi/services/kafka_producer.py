from kafka import KafkaProducer
import json
import os

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data-dev")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def produce_to_kafka(data: dict):
    producer.send(KAFKA_TOPIC, data)
    producer.flush()
