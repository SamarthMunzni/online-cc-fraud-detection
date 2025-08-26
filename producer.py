from confluent_kafka import Producer
import pandas as pd
import json, time

# Kafka configuration
conf = {"bootstrap.servers": "localhost:9092"}
producer = Producer(conf)

# Load dataset
df = pd.read_csv("creditcard.csv")

print("✅ Transaction producer started. Press CTRL+C to stop.")

try:
    while True:
        transaction = df.sample(1).to_dict(orient="records")[0]
        producer.produce("transactions", key="transaction", value=json.dumps(transaction))
        producer.flush()
        print("Produced:", transaction)
        time.sleep(1)  # send 1 transaction per second

except KeyboardInterrupt:
    print("\n⏹️ Stopping producer (CTRL+C pressed)...")

finally:
    # Flush any remaining messages
    producer.flush()
    print("✅ Producer closed cleanly.")
