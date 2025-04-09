import os
import sys
import time
import json
import shutil
import threading
import logging
import platform
import pkg_resources
import glob
import psutil
import subprocess
import asyncio
from kafka import KafkaProducer, KafkaAdminClient, KafkaConsumer
from kafka.errors import NoBrokersAvailable
from kafka.admin import NewTopic
from kafka.structs import TopicPartition
from fastapi.responses import JSONResponse

# ----------------------------------------
# Always honor PYTHONPATH if set
# ----------------------------------------
pythonpath = os.environ.get("PYTHONPATH")
if pythonpath and pythonpath not in sys.path:
    sys.path.append(pythonpath)
    print(f"🔧 Added {pythonpath} to sys.path")

print(f"💡 PYTHONPATH: {os.environ.get('PYTHONPATH')}")
print(f"📁 sys.path = {sys.path}")

PROCESS_START_TIME = time.time()

print(f"🐍 Current working directory: {os.getcwd()}")
print(f"📦 diagnostic module path: {__file__}")

# ----------------------------------------
# Init diagnostics (hooked in main.py)
# ----------------------------------------
def init_diagnostics(app, service_name: str):
    print("📝 Writing diagnostic snapshot to /app/logs/diagnostic_snapshot.json")
    print(f"📣 init_diagnostics called for: {service_name}")
    logging.info(f"🔍 Initializing diagnostics for {service_name}")
    print("🚰 Running store_diagnostics_snapshot manually...")
    store_diagnostics_snapshot()
    print("✅ Snapshot write complete!")
    asyncio.create_task(schedule_diagnostics_refresh())

# ----------------------------------------
# Startup checks
# ----------------------------------------
def startup_diagnostics():
    logging.info("🚦 Running startup diagnostics...")
    try:
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVER", "kafka:29092")
        client = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        if client.bootstrap_connected():
            logging.info("✅ Kafka bootstrap is reachable")
        else:
            logging.warning("⚠️ Kafka bootstrap is NOT connected")
        client.close()
    except Exception as e:
        logging.error(f"❌ Kafka check failed: {str(e)}")

# ----------------------------------------
# Diagnostics helpers
# ----------------------------------------
def get_env_info():
    return dict(os.environ)

def get_python_info():
    return {
        "version": platform.python_version(),
        "executable": sys.executable,
        "platform": platform.platform(),
        "implementation": platform.python_implementation(),
    }

def get_important_packages():
    important_pkgs = ["fastapi", "uvicorn", "kafka-python", "requests", "psutil"]
    return {
        pkg: pkg_resources.get_distribution(pkg).version
        for pkg in important_pkgs
    }

def get_system_info():
    proc = psutil.Process(os.getpid())
    mem_mb = round(proc.memory_info().rss / 1024 / 1024, 2)
    disk = shutil.disk_usage("/app")
    return {
        "memory_usage_mb": mem_mb,
        "disk_usage_percent": round((disk.used / disk.total) * 100, 2),
        "active_thread_count": threading.active_count(),
        "uptime_seconds": round(time.time() - PROCESS_START_TIME, 2),
        "cpu_percent": psutil.cpu_percent(interval=1),
        "cpu_cores": psutil.cpu_count(logical=False),
        "cpu_threads": psutil.cpu_count(logical=True),
        "load_average": os.getloadavg() if hasattr(os, "getloadavg") else "Unavailable",
    }

def get_filesystem_info():
    dirs = ["/app", "/app/data", "/app/logs"]
    return {d: os.path.exists(d) for d in dirs}

def get_recent_logs():
    try:
        log_file = sorted(glob.glob("/app/logs/*.log"))[-1]
        with open(log_file, "r") as f:
            return f.readlines()[-20:]
    except Exception as e:
        return [f"No recent log file found or error: {str(e)}"]

def count_log_errors():
    try:
        log_file = sorted(glob.glob("/app/logs/*.log"))[-1]
        with open(log_file, "r") as f:
            return sum(1 for line in f if "ERROR" in line.upper())
    except Exception as e:
        return f"Unavailable: {str(e)}"

def get_kafka_status():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVER", "kafka:29092")
    status = False
    topics = []
    topics_info = {}
    lag_info = {}
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        status = producer.bootstrap_connected()
        producer.close()

        admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="diagnostic")
        topics = sorted(admin.list_topics())
        admin.close()

        consumer = KafkaConsumer(bootstrap_servers=bootstrap, group_id="diagnostic-checker")
        for topic in topics:
            partitions = consumer.partitions_for_topic(topic)
            topics_info[topic] = {"partitions": list(partitions) if partitions else []}

            if partitions:
                tps = [TopicPartition(topic, p) for p in partitions]
                consumer.assign(tps)
                end_offsets = consumer.end_offsets(tps)
                for tp in tps:
                    committed = consumer.committed(tp)
                    lag = (end_offsets.get(tp, 0) - committed) if committed else 0
                    lag_info[f"{tp.topic}-partition-{tp.partition}"] = lag
        consumer.close()

    except Exception as e:
        topics = [f"Error: {str(e)}"]

    return status, topics, topics_info, lag_info

def get_docker_info():
    docker_path = shutil.which("docker")
    if not docker_path:
        return [f"Docker CLI not found in PATH"], "Unavailable"

    try:
        output = subprocess.check_output([
            "docker", "ps", "--format",
            "{{.ID}} | {{.Image}} | {{.Names}} | {{.Status}} | {{.Ports}}"
        ], text=True)
        lines = output.strip().split("\n")
        return lines, len(lines)
    except Exception as e:
        return [f"Docker info unavailable: {str(e)}"], "Unavailable"

# ----------------------------------------
# Persist diagnostics to file for reference
# ----------------------------------------
def store_diagnostics_snapshot():
    kafka_status, kafka_topics, kafka_info, kafka_lags = get_kafka_status()
    docker_data, container_count = get_docker_info()
    diagnostics_data = {
        "env": get_env_info(),
        "python": get_python_info(),
        "important_packages": get_important_packages(),
        "system": get_system_info(),
        "filesystem": get_filesystem_info(),
        "recent_logs": get_recent_logs(),
        "log_error_count": count_log_errors(),
        "kafka": {
            "status": kafka_status,
            "topics": kafka_topics,
            "topic_details": kafka_info,
            "consumer_lag": kafka_lags
        },
        "docker_containers": docker_data,
        "docker_container_count": container_count
    }
    try:
        os.makedirs("/app/logs", exist_ok=True)
        with open("/app/logs/diagnostic_snapshot.json", "w") as f:
            json.dump(diagnostics_data, f, indent=2)
        logging.info("🔗 Diagnostic snapshot saved to /app/logs/diagnostic_snapshot.json")
    except Exception as e:
        print(f"❌ Failed to write diagnostics: {str(e)}")
        logging.warning(f"Could not write diagnostic snapshot: {str(e)}")

# ----------------------------------------
# Schedule recurring snapshots every 60s
# ----------------------------------------
async def schedule_diagnostics_refresh():
    while True:
        await asyncio.sleep(60)
        logging.info("🕒 Auto-refreshing diagnostic snapshot...")
        store_diagnostics_snapshot()

# ----------------------------------------
# Debug endpoint
# ----------------------------------------
def debug_env():
    kafka_status, kafka_topics, kafka_info, kafka_lags = get_kafka_status()
    docker_data, container_count = get_docker_info()
    return JSONResponse(content={
        "env": get_env_info(),
        "python": get_python_info(),
        "important_packages": get_important_packages(),
        "system": get_system_info(),
        "filesystem": get_filesystem_info(),
        "recent_logs": get_recent_logs(),
        "log_error_count": count_log_errors(),
        "kafka_bootstrap_connected": kafka_status,
        "kafka_topics": kafka_topics,
        "kafka_topics_info": kafka_info,
        "kafka_consumer_lag": kafka_lags,
        "docker_containers": docker_data,
        "docker_container_count": container_count
    })
