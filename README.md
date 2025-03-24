# Kafka IoT Project - Setup Instructions

## 🔧 Setup Using Conda (Recommended for All Platforms)

1. **Install Miniconda** (if not already installed):
   - [Download Miniconda](https://docs.conda.io/en/latest/miniconda.html)

2. **Create the environment**:
   ```bash
   conda env create -f environment.yml
   ```

3. **Activate the environment**:
   ```bash
   conda activate kafka-iot-env
   ```

4. **Start the project using Docker**:
   ```bash
   docker-compose up --build
   ```

---

## 💡 Notes on Cross-Platform Compatibility

- This setup is tested on **macOS, Windows (with WSL2), and Linux**.
- For **Windows users**:
  - Use **Git Bash** or **WSL2** for best compatibility.
  - Avoid using native CMD or PowerShell for Docker volume mounts.
- Docker Desktop must be installed and running.

---

## 🔁 Optional: Using pip Instead of Conda

For developers who prefer pip:
```bash
python3 -m venv venv
source venv/bin/activate   # Or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

> Note: You are responsible for ensuring compatibility of all dependencies manually.

---

## 🐳 Docker Quick Start

```bash
docker-compose up --build
```

This will start:
- Zookeeper
- Kafka broker
- MongoDB
- FastAPI (with auto-reload)
- Kafka UI
- Grafana
- Prometheus

---

Happy coding!
