import os
import boto3
import csv
from datetime import datetime

# Read ENDPOINT_URL from environment variable to remain flexible across container setups
ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")

dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url=ENDPOINT_URL,
    region_name="eu-west-2",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)


def format_timestamp(timestamp):
    """Convert M/D/YYYY HH:MM:SS to ISO 8601."""
    if not timestamp:
        return ""
    dt = datetime.strptime(
        timestamp,
        "%m/%d/%Y %H:%M:%S"
    )
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def export_table(table_name, output_file):
    table = dynamodb.Table(table_name)

    # Scan the entire table with pagination support
    items = []
    response = table.scan()
    items.extend(response.get("Items", []))

    while "LastEvaluatedKey" in response:
        response = table.scan(
            ExclusiveStartKey=response["LastEvaluatedKey"]
        )
        items.extend(response.get("Items", []))

    if not items:
        print(f"{table_name}: no data found")
        return

    # Convert timestamps to ISO 8601
    for item in items:
        if "batch_start" in item:
            item["batch_start"] = format_timestamp(item["batch_start"])
        if "batch_end" in item:
            item["batch_end"] = format_timestamp(item["batch_end"])

    # Sort chronologically by batch start, then by turbine ID
    items.sort(
        key=lambda x: (
            x.get("batch_start", ""),
            x.get("turbine_id", "")
        )
    )

    # Fields list updated to include percentile stats written by the handler
    fields = [
        "turbine_id",
        "batch_start",
        "batch_end",
        "avg_temp_c",
        "max_temp_c",
        "p95_temp_c",
        "avg_vibration_mm_s",
        "max_vibration_mm_s",
        "p99_vibration_mm_s",
        "avg_rpm",
        "temperature_anomaly",
        "vibration_anomaly"
    ]

    # Write CSV using extrasaction='ignore' to safely ignore unexpected attributes
    with open(
        output_file,
        "w",
        newline="",
        encoding="utf-8"
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fields,
            extrasaction="ignore"
        )

        writer.writeheader()
        writer.writerows(items)

    print(
        f"{table_name}: exported "
        f"{len(items)} records -> {output_file}"
    )


if __name__ == "__main__":
    export_table("telemetry", "telemetry.csv")
    export_table("anomalies", "anomalies.csv")
