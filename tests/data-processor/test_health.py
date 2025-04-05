import requests


def test_health():
    response = requests.get("http://localhost:8002/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
