import json
import os
import boto3
import csv
import io
from decimal import Decimal

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

# DynamoDB Tables
telemetry_table = dynamodb.Table("telemetry")
anomalies_table = dynamodb.Table("anomalies")


# ============================================================
# Lambda handler
# ============================================================

def lambda_handler(event, context):
    print("--- Lambda Triggered by S3 Upload ---")

    for record in event.get("Records", []):
        bucket_name = record["s3"]["bucket"]["name"]
        file_key = record["s3"]["object"]["key"]

        print(f"Processing: s3://{bucket_name}/{file_key}")

        # Download CSV from S3
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=file_key
        )

        file_content = response["Body"].read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(file_content))

        # Group readings by turbine
        turbine_data = {}

        for row in reader:
            turbine_id = row["turbine_id"]
            timestamp = row["timestamp"]
            temperature = float(row["temperature_c"])
            vibration = float(row["vibration_mm_s"])
            rpm = float(row["rpm"])

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

        # Analyse each turbine and write to DynamoDB
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

            batch_start = min(data["timestamps"])
            batch_end = max(data["timestamps"])

            temp_fail = avg_temp > 85.0
            vibration_fail = max_vibration > 15.0
            anomaly_detected = temp_fail or vibration_fail

            # Store normalised turbine summary (converting floats to Decimal via string)
            telemetry_table.put_item(
                Item={
                    "turbine_id": turbine_id,
                    "batch_start": batch_start,
                    "batch_end": batch_end,
                    "avg_temp_c": Decimal(str(round(avg_temp, 2))),
                    "max_temp_c": Decimal(str(round(max_temp, 2))),
                    "avg_vibration_mm_s": Decimal(str(round(avg_vibration, 2))),
                    "max_vibration_mm_s": Decimal(str(round(max_vibration, 2))),
                    "avg_rpm": Decimal(str(round(avg_rpm, 2))),
                    "temperature_anomaly": temp_fail,
                    "vibration_anomaly": vibration_fail
                }
            )

            print(
                f"{turbine_id}: avg temp = {avg_temp:.2f}°C | "
                f"max vibration = {max_vibration:.2f} mm/s | "
                f"avg RPM = {avg_rpm:.2f}"
            )

            # Store anomaly separately if triggered
            if anomaly_detected:
                print(f"ANOMALY DETECTED: {turbine_id}")
                anomalies_table.put_item(
                    Item={
                        "turbine_id": turbine_id,
                        "batch_start": batch_start,
                        "batch_end": batch_end,
                        "avg_temp_c": Decimal(str(round(avg_temp, 2))),
                        "max_temp_c": Decimal(str(round(max_temp, 2))),
                        "avg_vibration_mm_s": Decimal(str(round(avg_vibration, 2))),
                        "max_vibration_mm_s": Decimal(str(round(max_vibration, 2))),
                        "avg_rpm": Decimal(str(round(avg_rpm, 2))),
                        "temperature_anomaly": temp_fail,
                        "vibration_anomaly": vibration_fail
                    }
                )

    return {
        "statusCode": 200,
        "body": json.dumps("Telemetry processed successfully")
    }
