# libs/tracing.py

import os
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.b3 import B3MultiFormat
from opentelemetry.instrumentation.requests import RequestsInstrumentor

import logging

def setup_tracer(app, service_name_env_var="OTEL_SERVICE_NAME"):
    # Optional logging
    logging.basicConfig(level=logging.INFO)

    service_name = os.getenv(service_name_env_var, "unknown-service")
    trace.set_tracer_provider(
        TracerProvider(
            resource=Resource.create({SERVICE_NAME: service_name})
        )
    )

    jaeger_exporter = JaegerExporter(
        collector_endpoint="http://jaeger:14268/api/traces"
    )

    span_processor = BatchSpanProcessor(jaeger_exporter)
    trace.get_tracer_provider().add_span_processor(span_processor)

    # Use B3 propagation headers for cross-service tracing
    set_global_textmap(B3MultiFormat())

    # Automatically instrument FastAPI
    FastAPIInstrumentor().instrument_app(app)

    RequestsInstrumentor().instrument()
