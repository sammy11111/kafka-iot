from libs.env_loader import PROJECT_ROOT # do not remove
from fastapi import FastAPI

app = FastAPI()

@app.get("/health")
def health_check():
    return {"status": "ok"}
