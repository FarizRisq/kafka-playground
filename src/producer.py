from confluent_kafka import Producer
import json
import time

conf = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(conf)

topic = "test-topic"

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

for i in range(5):
    data = {
        "event_id": i,
        "message": f"hello kafka {i}"
    }

    producer.produce(
        topic,
        value=json.dumps(data),
        callback=delivery_report
    )

    producer.poll(0)
    time.sleep(1)

producer.flush()
