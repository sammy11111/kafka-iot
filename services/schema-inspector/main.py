import sys
import os
from tracing import setup_tracer
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "libs")))

from fastapi import FastAPI, Query, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from kafka import KafkaConsumer
import os
import json
import csv
import io

# ----------------------------------------
# Load environment variables from root .env
# ----------------------------------------
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.env"))
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path=dotenv_path)

# ----------------------------------------
# Add shared libraries to sys.path
# ----------------------------------------
libs_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../libs"))
if libs_path not in sys.path:
    sys.path.append(libs_path)

# ----------------------
# FastAPI App with Health Check
# ----------------------
app = FastAPI()
templates = Jinja2Templates(directory="templates")

# Attach tracing to this service
setup_tracer(app)

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/")
def render_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
ERROR_TOPIC = "iot.errors.raw-data"


def fetch_errors(limit: int):
    consumer = KafkaConsumer(
        ERROR_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=5000,
        group_id=None,
    )
    messages = []
    for message in consumer:
        msg = message.value
        # Enrich message with validation error note if available
        if isinstance(msg, dict):
            if "_error" not in msg and "reason" in msg:
                msg["_error"] = msg.get("reason", "Schema validation failed")
        messages.append(msg)
        if len(messages) >= limit:
            break
    return messages[::-1]

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/schema-errors-html", response_class=HTMLResponse)
def schema_errors_html(limit: int = 10):
    messages = fetch_errors(limit)
    html = "".join(
        [
            f"<div class='error-item'><strong>⚠ Error:</strong> {msg.get('_error', 'Unknown')}<br><pre>{json.dumps(msg, indent=2)}</pre></div>"
            for msg in messages
        ]
    )
    return HTMLResponse(content=html)


@app.get("/schema-errors.json", response_class=JSONResponse)
def schema_errors_json(limit: int = 50):
    return JSONResponse(content={"errors": fetch_errors(limit)})


@app.get("/schema-errors.csv")
def schema_errors_csv(limit: int = 50):
    messages = fetch_errors(limit)
    output = io.StringIO()
    if messages:
        fields = sorted(
            set().union(*[msg.keys() for msg in messages if isinstance(msg, dict)])
        )
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for msg in messages:
            writer.writerow({k: msg.get(k, "") for k in fields})
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=schema_errors.csv"},
    )

