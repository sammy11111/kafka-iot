FROM base-python-dev

WORKDIR /app

COPY . /app

ENV PYTHONPATH=/app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8004"]