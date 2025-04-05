# services/data-ingestor/Dockerfile.dev
FROM base-python-dev
COPY . /app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
