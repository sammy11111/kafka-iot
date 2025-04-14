import os
import sys
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from datetime import datetime, timezone
from libs.env_loader import PROJECT_ROOT # do not remove

# initialize FastAPI
app = FastAPI()

# Setup MongoDB connection
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "iot") # If MONGO_DB is not found, use iot
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]


@app.get("/health")
def health_check():
    return {"status": "ok"}

# ---------------------------------------------------------------------------------
#    Perform data aggregation using pandas.
#    
#   This function reads raw senseBox data from the iot data collection, 
#    extracts sensor measurements from each document, aggregates the sensor values 
#    (mean per minute) for each sensor type, and writes the results to the 
#    'aggregated_data' collection.
# ---------------------------------------------------------------------------------
def aggregate_data():
    raw_data = list(db["sensor_data"].find())
    if not raw_data:
        return {"message": "No data to aggregate"}

    # Build a list of flat records from the raw sensor data
    records = []
    for doc in raw_data:
        # Parse timestamp from the "timestamp" field (new schema) or fallback to "createdAt" (old schema)
        try:
            timestamp = pd.to_datetime(doc.get("timestamp") or doc.get("createdAt"))
        except Exception as e:
            continue  # Skip record if timestamp conversion fails

        # Get location data - might be nested or flat depending on schema version
        location = doc.get("location", {})
        if isinstance(location, dict):
            lat = location.get("lat")
            lon = location.get("lon")
        else:
            # Fallback for old schema
            lat = doc.get("lat")
            lon = doc.get("lon")

        # Try getting sensor ID from either schema format
        sensor_id = doc.get("sensor_id") or doc.get("sensorId")

        # Handle value conversion properly for both schema versions
        try:
            value = float(doc.get("value", 0))
        except (TypeError, ValueError):
            continue

        # Box ID could be in either format
        box_id = doc.get("box_id") or doc.get("boxId")
        box_name = doc.get("box_name") or doc.get("boxName")

        # Get sensor type - could be in either format
        sensor_type = doc.get("sensor_type") or doc.get("sensorType")

        record = {
            "timestamp": timestamp,
            "boxId": box_id,
            "boxName": box_name,
            "exposure": doc.get("exposure"),
            "lat": lat,
            "lon": lon,
            "sensorId": sensor_id,
            "sensorType": sensor_type,
            "phenomenon": doc.get("phenomenon"),
            "value": value,
            "unit": doc.get("unit"),
        }
        records.append(record)

    if not records:
        return {"message": "No valid sensor measurements found for aggregation"}

    # Convert list of records to DataFrame
    df = pd.DataFrame(records)

    # Ensure timestamp is datetime and set as index for resampling
    if "timestamp" not in df.columns:
        return {"error": "No 'timestamp' field found in sensor measurements"}
    df.set_index("timestamp", inplace=True)

    # Perform aggregation: compute mean value per minute for each sensor type
    # Group by sensorType and resample per minute
    agg_df = (
        df.groupby("sensorType")
        .resample("1T")["value"]
        .mean()
        .reset_index()  # reset the index back, timestamp is no longer index
    )

    # Convert aggregated DataFrame back to dictionary records
    aggregated_records = agg_df.to_dict("records")

    # Clean up previous aggregated data and insert new aggregation
    db["aggregated_data"].drop()
    if aggregated_records:
        db["aggregated_data"].insert_many(aggregated_records)

    return {"message": "Aggregation complete", "aggregated_count": len(aggregated_records)}

# ---------------------------------------------------------------------------------
# Endpoint to Trigger Aggregation
# This endpoint aggregates raw senseBox data and updates the aggregated_data collection.
# When a client sends a POST request to /aggregate, this function calls aggregate_data().
# ---------------------------------------------------------------------------------
@app.post("/aggregate")
def run_aggregation():
    result = aggregate_data()
    return result

# ---------------------------------------------------------------------------------
# Allows clients (Grafana dashboards) to query the aggregated sensor data.
# Optional query parameters:
#      - start: ISO formatted start datetime string.
#      - end: ISO formatted end datetime string.
# Returns the aggregated data records within the specified time range.
# ---------------------------------------------------------------------------------
@app.get("/query")
def query_aggregated_data(start: str = None, end: str = None):

    query = {}
    if start or end:
        time_filter = {}
        if start:
            try:
                start_dt = datetime.fromisoformat(start)
                time_filter["$gte"] = start_dt
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid start datetime format")
        if end:
            try:
                end_dt = datetime.fromisoformat(end)
                time_filter["$lte"] = end_dt
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid end datetime format")
        query["timestamp"] = time_filter

    data = list(db["aggregated_data"].find(query, {"_id": 0}))
    return {"data": data}

# ---------------------------------------------------------------------------------
# When called, this endpoint generates a simple report on data aggregation and storage.
# The report includes:
#      - Count of raw senseBox documents in iot data
#      - Count of aggregated records in 'aggregated_data'
#      - Aggregation status
#      - Timestamp of report generation
# ---------------------------------------------------------------------------------
@app.get("/report")
def generate_report():
    raw_count = db["sensor_data"].count_documents({})
    aggregated_count = db["aggregated_data"].count_documents({})

    report = {
        "raw_data_count": raw_count,
        "aggregated_data_count": aggregated_count,
        "aggregation_status": "Complete" if aggregated_count > 0 else "Not run",
        "report_generated_at": datetime.now(timezone.utc).isoformat()
    }
    return {"report": report}