import json
import os
import boto3
import csv
import math
import hashlib
from decimal import Decimal
from datetime import datetime

ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")

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

telemetry_table = dynamodb.Table("telemetry")
anomalies_table = dynamodb.Table("anomalies")


def calculate_percentile(values, percentile):
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
    return sorted_values[lower_index] + fraction * (
        sorted_values[upper_index] - sorted_values[lower_index]
    )


def lambda_handler(event, context):
    print("--- Lambda Triggered by S3 Upload ---")

    total_readings = 0
    total_turbine_summaries = 0
    total_anomalies = 0

    with telemetry_table.batch_writer() as telemetry_batch, \
         anomalies_table.batch_writer() as anomalies_batch:

        for record in event.get("Records", []):
            bucket_name = record["s3"]["bucket"]["name"]
            file_key = record["s3"]["object"]["key"]

            print(f"Processing: s3://{bucket_name}/{file_key}")

            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)

            lines_stream = (
                line.decode("utf-8") for line in response["Body"].iter_lines()
            )
            reader = csv.DictReader(lines_stream)

            turbine_data = {}

            for row in reader:
                turbine_id = row["turbine_id"]
                timestamp = row["timestamp"]

                try:
                    temperature = float(row["temperature_c"])
                    vibration = float(row["vibration_mm_s"])
                    rpm = float(row["rpm"])
                except (ValueError, KeyError) as err:
                    print(f"Skipping malformed row: {row} ({err})")
                    continue

                if not all(
                    math.isfinite(val) for val in (temperature, vibration, rpm)
                ):
                    continue

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

                p95_temp = calculate_percentile(temperatures, 95)
                p99_vibration = calculate_percentile(vibrations, 99)

                # Robust ISO 8601 Timestamp Parsing
                parsed_ts = []
                for ts in data["timestamps"]:
                    ts_clean = ts.replace("Z", "+00:00")
                    try:
                        parsed_ts.append(datetime.fromisoformat(ts_clean))
                    except ValueError:
                        # Fallback parsing if format varies
                        parsed_ts.append(datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f"))

                batch_start = min(parsed_ts).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                batch_end = max(parsed_ts).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

                temp_fail = p95_temp > 85.0
                vibration_fail = p99_vibration > 15.0
                anomaly_detected = temp_fail or vibration_fail

                total_turbine_summaries += 1

                # Hashed batch ID for idempotency tracking
                batch_id = hashlib.sha256(
                    f"{file_key}:{turbine_id}:{batch_start}".encode("utf-8")
                ).hexdigest()

                summary = {
                    "turbine_id": turbine_id,
                    "batch_start": batch_start,
                    "batch_end": batch_end,
                    "batch_id": batch_id,
                    "avg_temp_c": Decimal(str(round(avg_temp, 2))),
                    "max_temp_c": Decimal(str(round(max_temp, 2))),
                    "p95_temp_c": Decimal(str(round(p95_temp, 2))),
                    "avg_vibration_mm_s": Decimal(str(round(avg_vibration, 2))),
                    "max_vibration_mm_s": Decimal(str(round(max_vibration, 2))),
                    "p99_vibration_mm_s": Decimal(str(round(p99_vibration, 2))),
                    "avg_rpm": Decimal(str(round(avg_rpm, 2))),
                    "temperature_anomaly": temp_fail,
                    "vibration_anomaly": vibration_fail
                }

                telemetry_batch.put_item(Item=summary)

                if anomaly_detected:
                    total_anomalies += 1
                    anomalies_batch.put_item(Item=summary.copy())

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Telemetry processed successfully",
            "readings_processed": total_readings,
            "turbine_summaries": total_turbine_summaries,
            "anomalies_detected": total_anomalies
        })
    }