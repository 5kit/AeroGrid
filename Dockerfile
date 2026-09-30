FROM python:3.12-slim

WORKDIR /AeroGrid

COPY sensor_client.py .
COPY lambda/ ./lambda/

RUN pip install --no-cache-dir boto3

CMD ["python", "-u", "sensor_client.py"]
