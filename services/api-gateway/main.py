from libs.env_loader import PROJECT_ROOT # do not remove
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
