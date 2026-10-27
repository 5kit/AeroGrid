# AeroGrid

A local AWS-style telemetry pipeline for simulating and analysing wind turbine sensor data.

The project started as a CSV-based telemetry analysis task and was extended into a small event-driven cloud architecture using **Docker and LocalStack**. It simulates turbine telemetry, streams raw CSV batches to S3, processes them asynchronously with Lambda, and writes aggregate metrics and anomalies to DynamoDB.

## Architecture

```text
  ┌─────────────────────────────────┐
  │  Turbine Simulator Container    │
  │  (sensor_client.py)             │
  └────────────────┬────────────────┘
                   │ 10-min telemetry batch (CSV)
                   ▼
  ┌─────────────────────────────────┐
  │  Amazon S3                      │
  │  s3://sensor-data/polls/        │
  └────────────────┬────────────────┘
                   │ s3:ObjectCreated:* Event
                   ▼
  ┌─────────────────────────────────┐
  │  AWS Lambda                     │
  │  (Streaming DictReader + P95/P99)│
  └───────┬─────────────────┬───────┘
          │                 │ BatchWriter
          ▼                 ▼
  ┌───────────────┐   ┌───────────────┐
  │   telemetry   │   │   anomalies   │
  │   DynamoDB    │   │   DynamoDB    │
  └───────────────┘   └───────────────┘

```

LocalStack provides AWS services locally so the entire pipeline runs end-to-end without cloud costs or AWS credentials.

---

## What It Does

The simulator generates streaming telemetry across eight turbines (`T-01` through `T-09`):

* Temperature (°C)
* Vibration (mm/s)
* RPM
* Timestamp

Each batch contains 10 simulated minutes of readings (100 sample intervals across all turbines) and is uploaded to an S3 bucket as a CSV file.

### Pipeline Execution Flow

1. **Upload Event:** S3 receives a new batch and triggers an `s3:ObjectCreated:*` event to Lambda.
2. **Streamed Ingestion:** Lambda reads the file line-by-line (`response["Body"].iter_lines()`) directly from the HTTP stream to avoid loading large payloads into memory.
3. **Statistical Analysis:** Readings are grouped by turbine to calculate average, maximum, and percentile metrics (95th percentile temperature and 99th percentile vibration using linear interpolation).
4. **Batch Writes:** Aggregated summaries are pushed to DynamoDB via `boto3.batch_writer()` to reduce API call overhead.
5. **Anomaly Storage:** Turbine summaries violating threshold criteria are also saved to a separate `anomalies` table.

---

## Anomaly Detection & Metrics

Anomalies are flagged using 95th and 99th percentile thresholds:

| Metric | Threshold Criteria | Target Turbine Behavior |
| --- | --- | --- |
| **P95 Temperature** | `> 85.0°C` | **T-04** (fluctuates around 85.1°C – 88.0°C) |
| **P99 Vibration** | `> 15.0 mm/s` | **T-07** (fluctuates around 15.1 mm/s – 17.0 mm/s) |

Values drift within narrow ranges near the thresholds to simulate realistic hardware degradation rather than artificial sudden spikes.

---

## AWS Services & Infrastructure

| Service / Tool | Purpose | Implementation Details |
| --- | --- | --- |
| **Amazon S3** | Raw telemetry storage | Bucket `sensor-data` receiving raw CSV files |
| **AWS Lambda** | Stream processing | Python 3.10 event handler with S3 body generator |
| **Amazon DynamoDB** | Database storage | Composite key tables `telemetry` & `anomalies` |
| **LocalStack** | Local AWS emulator | Simulates S3, Lambda, DynamoDB, and IAM locally |
| **Docker Compose** | Multi-container setup | Manages container execution, networking, and health checks |

---

## Project Structure

```text
AeroGrid/
├── docker-compose.yml     # LocalStack & simulator container orchestration
├── Dockerfile             # Simulator container definition
├── sensor_client.py       # Infrastructure setup, wait_for_localstack, & generator loop
├── verify_db.py           # CLI script to inspect DynamoDB tables directly
├── export_csv.py          # Script for exporting DynamoDB data back to ISO CSV files
└── lambda/
    └── handler.py         # Memory-efficient Lambda processor using batch_writer

```

---

## Running Locally

### Prerequisites

* [Docker Desktop](https://www.docker.com/products/docker-desktop/) with Docker Compose.
* Python 3.10+ (optional, for local verification scripts).

### 1. Launch Stack with Docker Compose

Start LocalStack and the containerized simulator:

```bash
docker-compose up --build

```

`sensor_client.py` uses built-in health checking (`wait_for_localstack()`) to wait until LocalStack services are ready before initializing S3 buckets, DynamoDB tables, and Lambda triggers.

### 2. Verify Ingested Records

In a separate terminal, scan the processed DynamoDB summary items:

```bash
python verify_db.py

```

### 3. Export Summary Data to CSV

Export table records into sorted ISO 8601 CSV files:

```bash
python export_csv.py

```

Generates `telemetry.csv` and `anomalies.csv` with calculated percentiles (`p95_temp_c`, `p99_vibration_mm_s`).

---

## DynamoDB Data Schema

Both `telemetry` and `anomalies` tables share a composite primary key layout:

* **Partition Key (`HASH`):** `turbine_id` (String)
* **Sort Key (`RANGE`):** `batch_start` (String)

### Example Record JSON

```json
{
  "turbine_id": "T-04",
  "batch_start": "10/27/2026 13:00:00",
  "batch_end": "10/27/2026 13:10:00",
  "avg_temp_c": 86.42,
  "max_temp_c": 87.90,
  "p95_temp_c": 87.55,
  "avg_vibration_mm_s": 5.12,
  "max_vibration_mm_s": 7.80,
  "p99_vibration_mm_s": 7.65,
  "avg_rpm": 14.80,
  "temperature_anomaly": true,
  "vibration_anomaly": false
}

```

---

## Performance Optimizations

* **Streamed Ingestion:** Decodes S3 object lines on the fly via `iter_lines()` to maintain minimal memory usage inside Lambda regardless of CSV file size.
* **Bulk DynamoDB Writes:** Uses `boto3.batch_writer()` to buffer up to 25 items per request, reducing API latency and network overhead.

---

## Future Improvements

* Replace simulated CSV polling with MQTT streaming.
* Add AWS IoT Core and SQS between S3 ingestion and Lambda processing.
* Add partitioned S3 storage paths (`year=YYYY/month=MM/day=DD/`).
* Build a web visualization dashboard for monitoring turbine health.
* Add `pytest` and `moto` unit test suites for handler testing.
