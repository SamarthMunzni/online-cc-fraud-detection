from fastapi import FastAPI
import joblib, pandas as pd

app = FastAPI()
model = joblib.load("../model/fraud_model.pkl")

@app.post("/predict")
def predict(data: dict):
    df = pd.DataFrame([data])
    prob = model.predict_proba(df)[0][1]
    return {"fraud_probability": float(prob), "fraud": bool(prob > 0.8)}