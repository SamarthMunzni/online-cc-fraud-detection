from confluent_kafka import Consumer
import joblib, json, pandas as pd

# Load model
model = joblib.load("../model/fraud_model.pkl")

# Kafka consumer configuration
conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "fraud-consumer",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(conf)
consumer.subscribe(["transactions"])

print("✅ Fraud detection consumer started. Press CTRL+C to stop.")

try:
    while True:
        msg = consumer.poll(1.0)  # poll with timeout (non-blocking)
        if msg is None:
            continue
        if msg.error():
            print(f"⚠️ Consumer error: {msg.error()}")
            continue

        transaction = json.loads(msg.value().decode("utf-8"))
        df = pd.DataFrame([transaction])
        prob = model.predict_proba(df)[0][1]

        print(f"Transaction Fraud Probability: {prob:.4f}")
        if prob > 0.8:
            print("🚨 ALERT: Possible Fraud Detected!")

except KeyboardInterrupt:
    print("\n⏹️ Stopping consumer (CTRL+C pressed)...")

finally:
    consumer.close()
    print("✅ Consumer closed cleanly.")
