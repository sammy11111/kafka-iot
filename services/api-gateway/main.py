import os
import sys
from dotenv import load_dotenv

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

from fastapi import FastAPI
from tracing import setup_tracer
from opentelemetry.instrumentation.requests import RequestsInstrumentor

app = FastAPI()

# Attach tracing to this service
setup_tracer(app)

@app.on_event("startup")
def setup_requests_instrumentation():
    RequestsInstrumentor().instrument()

@app.get("/health")
def health_check():
    return {"status": "ok"}
