FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run as non-root
RUN useradd -m simulator && chown -R simulator /app
USER simulator

ENTRYPOINT ["python", "iot_simulator.py"]
