from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic_name = "test-topic"

for i in range(5):
    message = {
        "event_id": i,
        "event_type": "demo",
        "message": f"hello kafka {i}"
    }

    producer.send(topic_name, message)
    print("sent:", message)
    time.sleep(1)

producer.flush()
producer.close()
