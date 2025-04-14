import os
import sys
import asyncio
import pandas as pd
from libs.env_loader import PROJECT_ROOT # do not remove
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from tracing import setup_tracer

# Setup MongoDB connection
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "iot")  # If MONGO_DB is not found, use iot
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]


# Background aggregation task
async def periodic_aggregation():
    print("⏰ Periodic aggregation task started")
    while True:
        try:
            result = aggregate_data()
            if isinstance(result, dict):
                if "operations_performed" in result:
                    print(f"✅ Aggregation completed: {result['operations_performed']} operations performed")
                elif "aggregated_count" in result:
                    print(f"✅ Aggregation completed: {result['aggregated_count']} records processed")
                else:
                    print(f"✅ Aggregation completed: {result}")
            else:
                print(f"✅ Aggregation completed: {result}")
        except Exception as e:
            print(f"❌ Aggregation error: {str(e)}")

        print(f"💤 Sleeping for 10 seconds...")
        await asyncio.sleep(10)


# Lifespan context manager to handle startup and shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: start the background task
    print("🚀 Starting automatic aggregation (every 10 seconds)...")
    task = asyncio.create_task(periodic_aggregation())

    yield  # This is where FastAPI runs

    # Shutdown: cancel the background task
    print("🛑 Shutting down aggregation task...")
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("✅ Aggregation task stopped cleanly")


# Initialize FastAPI with lifespan
app = FastAPI(lifespan=lifespan)

# Attach tracing to this service
setup_tracer(app)


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
    print(f"Starting aggregation process...")

    # Count raw data before processing
    raw_count = db["sensor_data"].count_documents({})
    print(f"Found {raw_count} documents in sensor_data collection")

    # Count existing aggregated data
    agg_count_before = db["aggregated_data"].count_documents({})
    print(f"Current aggregated_data collection has {agg_count_before} documents")

    raw_data = list(db["sensor_data"].find())
    if not raw_data:
        print("No data found to aggregate")
        return {"operations_performed": 0, "new_records": 0, "updated_records": 0, "net_change": 0, "total_records": 0}

    # Create a unique compound index if it doesn't exist
    db["aggregated_data"].create_index([("timestamp", 1), ("sensorType", 1)], unique=True)

    records = []
    skipped_records = 0

    for doc in raw_data:
        try:
            # Try to extract timestamp
            timestamp_field = doc.get("timestamp") or doc.get("createdAt")
            if not timestamp_field:
                skipped_records += 1
                continue

            timestamp = pd.to_datetime(timestamp_field)

            # Get location data
            location = doc.get("location", {})
            if isinstance(location, dict):
                lat = location.get("lat")
                lon = location.get("lon")
            else:
                lat = doc.get("lat")
                lon = doc.get("lon")

            # Get sensor ID
            sensor_id = doc.get("sensor_id") or doc.get("sensorId")
            if not sensor_id:
                skipped_records += 1
                continue

            # Handle value conversion
            try:
                value = float(doc.get("value", 0))
            except (TypeError, ValueError):
                skipped_records += 1
                continue

            # Box ID could be in either format
            box_id = doc.get("box_id") or doc.get("boxId")
            box_name = doc.get("box_name") or doc.get("boxName")

            # Get sensor type
            sensor_type = doc.get("sensor_type") or doc.get("sensorType")
            if not sensor_type:
                # Use a default type if not found
                sensor_type = "unknown"

            # Get unit
            unit = doc.get("unit")

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
                "unit": unit,
            }
            records.append(record)
        except Exception as e:
            print(f"Error processing record: {str(e)}")
            skipped_records += 1
            continue

    print(f"Processed {len(raw_data)} records: {len(records)} valid, {skipped_records} skipped")

    if not records:
        print("No valid records to aggregate")
        return {"operations_performed": 0, "new_records": 0, "updated_records": 0, "net_change": 0,
                "total_records": agg_count_before}

    # Convert to DataFrame and aggregate
    df = pd.DataFrame(records)
    print(f"Created DataFrame with {len(df)} rows and columns: {', '.join(df.columns)}")

    df.set_index("timestamp", inplace=True)

    # Group by sensorType and resample per minute
    print("Performing aggregation...")

    # Group by sensorType and unit, then resample
    agg_df = (
        df.groupby(["sensorType", "unit"])
        .resample("1T")["value"]
        .mean()
        .reset_index()
    )

    # Remove rows with NaN values
    agg_df = agg_df.dropna(subset=["value"])

    print(f"Aggregated to {len(agg_df)} rows after removing NaN values")

    # Convert back to dictionaries
    aggregated_records = agg_df.to_dict("records")


    # ----------------------------------------------------------------------------------------
    # Aggregates raw senseBox data and updates the aggregated_data collection.
    # When a client sends a POST request to /aggregate, this function calls aggregate_data().
    # ----------------------------------------------------------------------------------------

    # Improved upsert tracking
    operations_count = 0
    upserted_ids = []
    modified_ids = []

    for record in aggregated_records:
        # Create a unique ID for logging
        record_id = f"{record['sensorType']}:{record['timestamp'].isoformat()}"

        try:
            result = db["aggregated_data"].update_one(
                {"timestamp": record["timestamp"], "sensorType": record["sensorType"]},
                {"$set": record},
                upsert=True
            )

            if result.upserted_id is not None:
                operations_count += 1
                upserted_ids.append(record_id)
            elif result.modified_count > 0:
                operations_count += 1
                modified_ids.append(record_id)
            # No else - record existed and was unchanged
        except Exception as e:
            print(f"Error upserting record {record_id}: {str(e)}")

    # Count aggregated data after operation
    agg_count_after = db["aggregated_data"].count_documents({})
    net_change = agg_count_after - agg_count_before

    print(f"Aggregation summary:")
    print(f"- Processed {len(aggregated_records)} aggregated records")
    print(f"- New records inserted: {len(upserted_ids)}")
    print(f"- Existing records updated: {len(modified_ids)}")
    print(f"- Net change in collection: {net_change} records")
    print(f"- Collection now has {agg_count_after} records")

    return {
        "operations_performed": operations_count,
        "new_records": len(upserted_ids),
        "updated_records": len(modified_ids),
        "net_change": net_change,
        "total_records": agg_count_after
    }


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
        "report_generated_at": datetime.now(timezone.utc).isoformat(),
        "aggregation_frequency": "Every 10 seconds",
        "database_name": MONGO_DB,
        "collections": {
            "raw_data": "sensor_data",
            "aggregated_data": "aggregated_data"
        }
    }
    return {"report": report}