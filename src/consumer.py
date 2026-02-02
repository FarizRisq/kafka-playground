from confluent_kafka import Consumer

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "demo-group",
    "auto.offset.reset": "earliest"
    }

consumer = Consumer(conf)
consumer.subscribe(["test-topic"])

print("Waiting for messages....j")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Error:", msg.error())
        continue
    print("received:", msg.value().decode("utf-8"))
