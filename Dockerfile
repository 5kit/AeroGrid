FROM python:3.12-slim

WORKDIR /AeroGrid

# Install curl for optional container healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

COPY sensor_client.py .
COPY lambda/ ./lambda/

RUN pip install --no-cache-dir boto3

# -u ensures Python stdout/stderr streams are unbuffered for live container logs
CMD ["python", "-u", "sensor_client.py"]
