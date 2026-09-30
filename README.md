# AeroGrid

A local AWS-style telemetry pipeline for simulating and analysing wind turbine sensor data.

The project started as a CSV-based telemetry analysis task and was extended into a small event-driven cloud architecture using **Docker and LocalStack**. It simulates turbine telemetry, stores raw batches in S3, processes them with Lambda, and stores analysed results in DynamoDB.

## Architecture

```text
Turbine Simulator
       │
       │ 10-minute telemetry batch
       ▼
     S3
       │
       │ ObjectCreated
       ▼
    Lambda
       │
       ├───────────────┐
       ▼               ▼
  telemetry        anomalies
  DynamoDB          DynamoDB
```

LocalStack provides the AWS services locally, so the entire pipeline can be developed and tested without deploying to AWS.

## What it does

The simulator generates telemetry for eight turbines:

* Temperature
* Vibration
* RPM
* Timestamp

Each batch contains 10 simulated minutes of readings and is uploaded to an S3 bucket as a CSV file.

When a CSV is uploaded:

1. S3 triggers the Lambda function.
2. Lambda downloads the CSV.
3. Readings are grouped by turbine.
4. Batch-level statistics are calculated.
5. Results are written to DynamoDB.
6. Anomalous turbine batches are also written to a separate `anomalies` table.

### Anomaly detection

The current thresholds are:

```text
Average temperature > 85°C
Maximum vibration > 15 mm/s
```

The simulator intentionally produces two anomalous turbines:

* **T-04** — elevated temperature
* **T-07** — elevated vibration

The values are kept close to the thresholds rather than generating extreme failures.

## AWS services

| Service    | Purpose                                       |
| ---------- | --------------------------------------------- |
| S3         | Stores raw telemetry CSV batches              |
| Lambda     | Processes uploaded telemetry                  |
| DynamoDB   | Stores processed telemetry and anomalies      |
| LocalStack | Runs the AWS services locally                 |
| Docker     | Runs the simulator in an isolated environment |

The same architecture could later be deployed to AWS using S3, Lambda, and DynamoDB directly.

## Project structure

```text
AeroGrid/
├── sensor_client.py
├── Dockerfile
├── lambda/
│   └── handler.py
├── lambda.zip
└── README.md
```

`lambda.zip` is generated automatically and is ignored by Git.

## Running locally

### Requirements

* Docker
* LocalStack
* Python 3.10+ if running the simulator directly

### 1. Start LocalStack

Start LocalStack with S3, Lambda and DynamoDB available.

The local AWS endpoint is:

```text
http://localhost:4566
```

The LocalStack Web Application can be accessed at:

[https://app.localstack.cloud](https://app.localstack.cloud?utm_source=chatgpt.com)

From the Web Application you can inspect the local AWS resources created by AeroGrid, including S3 buckets, Lambda functions and DynamoDB tables.

### 2. Build the simulator

```bash
docker build -t turbine-simulator .
```

### 3. Run the simulator

The simulator needs to be on the same Docker network as LocalStack.

For example:

```bash
docker run --rm --network localstack-net -e AWS_ENDPOINT_URL=http://localstack-aws:4566 turbine-simulator
```

Replace `localstack` with the actual LocalStack container/service name if different.


You should then see output similar to:

```text
========================================
        AeroGrid Telemetry Simulator
========================================
LocalStack API:   http://localstack-aws:4566
LocalStack UI:    https://app.localstack.cloud
S3 bucket:        sensor-data
========================================

Starting 10-minute telemetry batches...

Generating telemetry batch starting 2026-09-30 01:12:34
Uploaded 10-minute telemetry batch ->
s3://sensor-data/polls/batch_20260930_011234_123456.csv

Records uploaded: 80
```

The simulator currently waits **6 seconds** between batches so the pipeline can be tested quickly. Each generated batch represents 10 simulated minutes of telemetry.

## DynamoDB schema

### `telemetry`

Stores one summary record per turbine per batch.

```text
Partition key: turbine_id
Sort key:      batch_start
```

Example attributes:

```text
turbine_id
batch_start
batch_end
avg_temp_c
max_temp_c
avg_vibration_mm_s
max_vibration_mm_s
avg_rpm
temperature_anomaly
vibration_anomaly
```

### `anomalies`

Contains only batches where an anomaly was detected.

```text
Partition key: turbine_id
Sort key:      batch_start
```

This keeps the complete raw telemetry in S3 while DynamoDB contains queryable summaries.

## Why S3 + DynamoDB?

S3 is used as the raw telemetry store because the original CSV batches are retained in their entirety.

DynamoDB is used for processed summaries because applications can query individual turbines and time ranges without repeatedly reading and processing the raw CSV files.

For example:

```text
S3
└── Complete raw telemetry

DynamoDB
└── T-04
    ├── batch 1 summary
    ├── batch 2 summary
    └── batch 3 summary
```

## Technologies

* Python
* AWS SDK for Python (`boto3`)
* Amazon S3
* AWS Lambda
* Amazon DynamoDB
* Docker
* LocalStack
* CSV processing

## Future improvements

Possible extensions include:

* Replace the simulated CSV batches with MQTT telemetry
* Add AWS IoT Core to the architecture
* Introduce SQS between ingestion and processing
* Add more sophisticated anomaly detection
* Add a dashboard for turbine health
* Store raw telemetry with partitioned S3 keys
* Deploy the architecture to AWS
* Add automated tests for the Lambda processing logic
