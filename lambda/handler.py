import json
import os
import boto3
import csv
import math
from decimal import Decimal
from datetime import datetime

# LocalStack injects AWS_ENDPOINT_URL automatically inside the Lambda container.
ENDPOINT_URL = os.getenv(
    "AWS_ENDPOINT_URL",
    "http://localhost:4566"
)

# ============================================================
# AWS clients
# ============================================================

s3_client = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    region_name="eu-west-2",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url=ENDPOINT_URL,
    region_name="eu-west-2",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

# DynamoDB tables
telemetry_table = dynamodb.Table("telemetry")
anomalies_table = dynamodb.Table("anomalies")


# ============================================================
# Percentile calculation
# ============================================================

def calculate_percentile(values, percentile):
    """
    Calculate a percentile using linear interpolation.

    percentile must be between 0 and 100.
    """
    if not values:
        raise ValueError("Cannot calculate percentile of empty data")

    if not 0 <= percentile <= 100:
        raise ValueError("Percentile must be between 0 and 100")

    sorted_values = sorted(values)

    position = (len(sorted_values) - 1) * percentile / 100
    lower_index = math.floor(position)
    upper_index = math.ceil(position)

    if lower_index == upper_index:
        return sorted_values[lower_index]

    fraction = position - lower_index

    return (
        sorted_values[lower_index]
        + fraction * (
            sorted_values[upper_index]
            - sorted_values[lower_index]
        )
    )


# ============================================================
# Lambda handler
# ============================================================

def lambda_handler(event, context):
    print("--- Lambda Triggered by S3 Upload ---")

    total_readings = 0
    total_turbine_summaries = 0
    total_anomalies = 0

    # Use batch_writer contexts to batch put_item calls (up to 25 items per batch)
    with telemetry_table.batch_writer() as telemetry_batch, \
         anomalies_table.batch_writer() as anomalies_batch:

        for record in event.get("Records", []):
            bucket_name = record["s3"]["bucket"]["name"]
            file_key = record["s3"]["object"]["key"]

            print(f"Processing: s3://{bucket_name}/{file_key}")

            # Stream CSV directly from the S3 payload stream without reading full body into memory
            response = s3_client.get_object(
                Bucket=bucket_name,
                Key=file_key
            )

            lines_stream = (
                line.decode("utf-8")
                for line in response["Body"].iter_lines()
            )
            reader = csv.DictReader(lines_stream)

            # Group readings by turbine
            turbine_data = {}

            for row in reader:
                turbine_id = row["turbine_id"]
                timestamp = row["timestamp"]

                temperature = float(row["temperature_c"])
                vibration = float(row["vibration_mm_s"])
                rpm = float(row["rpm"])

                # Reject non-finite sensor values
                if not all(
                    math.isfinite(value)
                    for value in (temperature, vibration, rpm)
                ):
                    raise ValueError(
                        f"Invalid sensor value for turbine {turbine_id}"
                    )

                total_readings += 1

                if turbine_id not in turbine_data:
                    turbine_data[turbine_id] = {
                        "timestamps": [],
                        "temperatures": [],
                        "vibrations": [],
                        "rpms": []
                    }

                turbine_data[turbine_id]["timestamps"].append(timestamp)
                turbine_data[turbine_id]["temperatures"].append(temperature)
                turbine_data[turbine_id]["vibrations"].append(vibration)
                turbine_data[turbine_id]["rpms"].append(rpm)

            # Analyse each turbine and write to DynamoDB via batch_writer
            print("\n=== TURBINE ANALYSIS ===")

            for turbine_id, data in turbine_data.items():
                temperatures = data["temperatures"]
                vibrations = data["vibrations"]
                rpms = data["rpms"]

                avg_temp = sum(temperatures) / len(temperatures)
                max_temp = max(temperatures)

                avg_vibration = sum(vibrations) / len(vibrations)
                max_vibration = max(vibrations)

                avg_rpm = sum(rpms) / len(rpms)

                # Calculate percentile statistics
                p95_temp = calculate_percentile(temperatures, 95)
                p99_vibration = calculate_percentile(vibrations, 99)

                # Parse timestamps chronologically
                timestamps = [
                    datetime.strptime(
                        ts,
                        "%m/%d/%Y %H:%M:%S"
                    )
                    for ts in data["timestamps"]
                ]

                batch_start = min(timestamps).strftime(
                    "%m/%d/%Y %H:%M:%S"
                )
                batch_end = max(timestamps).strftime(
                    "%m/%d/%Y %H:%M:%S"
                )

                # Anomaly detection using percentiles
                temp_fail = p95_temp > 85.0
                vibration_fail = p99_vibration > 15.0

                anomaly_detected = temp_fail or vibration_fail

                total_turbine_summaries += 1

                # Shared summary fields
                summary = {
                    "turbine_id": turbine_id,
                    "batch_start": batch_start,
                    "batch_end": batch_end,
                    "avg_temp_c": Decimal(str(round(avg_temp, 2))),
                    "max_temp_c": Decimal(str(round(max_temp, 2))),
                    "p95_temp_c": Decimal(str(round(p95_temp, 2))),
                    "avg_vibration_mm_s": Decimal(
                        str(round(avg_vibration, 2))
                    ),
                    "max_vibration_mm_s": Decimal(
                        str(round(max_vibration, 2))
                    ),
                    "p99_vibration_mm_s": Decimal(
                        str(round(p99_vibration, 2))
                    ),
                    "avg_rpm": Decimal(str(round(avg_rpm, 2))),
                    "temperature_anomaly": temp_fail,
                    "vibration_anomaly": vibration_fail
                }

                # Queue item to telemetry batch writer
                telemetry_batch.put_item(Item=summary)

                print(
                    f"{turbine_id}: "
                    f"avg temp = {avg_temp:.2f}°C | "
                    f"P95 temp = {p95_temp:.2f}°C | "
                    f"max vibration = {max_vibration:.2f} mm/s | "
                    f"P99 vibration = {p99_vibration:.2f} mm/s | "
                    f"avg RPM = {avg_rpm:.2f}"
                )

                # Queue item to anomaly batch writer if triggered
                if anomaly_detected:
                    total_anomalies += 1

                    print(
                        f"ANOMALY DETECTED: {turbine_id} | "
                        f"Temperature anomaly: {temp_fail} | "
                        f"Vibration anomaly: {vibration_fail}"
                    )

                    anomalies_batch.put_item(Item=summary.copy())

    # Context managers exit here, flushing remaining batched DynamoDB writes

    # ========================================================
    # Invocation summary
    # ========================================================

    print("\n=== PROCESSING SUMMARY ===")
    print(f"Readings processed: {total_readings}")
    print(f"Turbine summaries: {total_turbine_summaries}")
    print(f"Anomalies detected: {total_anomalies}")
    print(f"Telemetry records written: {total_turbine_summaries}")
    print(f"Anomaly records written: {total_anomalies}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Telemetry processed successfully",
            "readings_processed": total_readings,
            "turbine_summaries": total_turbine_summaries,
            "anomalies_detected": total_anomalies
        })
    }
