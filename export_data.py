import os
import boto3
import csv

ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")

dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url=ENDPOINT_URL,
    region_name="eu-west-2",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)


def export_table(table_name, output_file):
    table = dynamodb.Table(table_name)

    # Scan entire table with full pagination support
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

    # Sort chronologically by ISO 8601 batch start, then by turbine ID
    items.sort(
        key=lambda x: (
            x.get("batch_start", ""),
            x.get("turbine_id", "")
        )
    )

    # Fields list including percentiles and batch_id
    fields = [
        "turbine_id",
        "batch_start",
        "batch_end",
        "batch_id",
        "avg_temp_c",
        "max_temp_c",
        "p95_temp_c",
        "avg_vibration_mm_s",
        "max_vibration_mm_s",
        "p99_vibration_mm_s",
        "avg_rpm",
        "temperature_anomaly",
        "vibration_anomaly",
    ]

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
