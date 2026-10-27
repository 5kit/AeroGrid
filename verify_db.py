import os
import boto3

ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")

dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url=ENDPOINT_URL,
    region_name="eu-west-2",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

telemetry_table = dynamodb.Table("telemetry")
anomalies_table = dynamodb.Table("anomalies")


def fetch_and_display_table(table, table_name):
    print(f"\n========================================")
    print(f"      DynamoDB Table: {table_name}")
    print(f"========================================")

    # Full scan with pagination
    items = []
    response = table.scan()
    items.extend(response.get("Items", []))

    while "LastEvaluatedKey" in response:
        response = table.scan(
            ExclusiveStartKey=response["LastEvaluatedKey"]
        )
        items.extend(response.get("Items", []))

    print(f"Total items found: {len(items)}\n")

    for idx, item in enumerate(items, 1):
        print(f"--- Record #{idx} ---")
        print(f"Turbine ID          : {item.get('turbine_id')}")
        print(f"Batch Start         : {item.get('batch_start')}")
        print(f"Batch End           : {item.get('batch_end')}")
        print(f"Batch ID (Hash)     : {item.get('batch_id')}")
        print(f"Avg Temperature     : {item.get('avg_temp_c')} °C")
        print(f"Max Temperature     : {item.get('max_temp_c')} °C")
        print(f"P95 Temperature     : {item.get('p95_temp_c')} °C")
        print(f"Avg Vibration       : {item.get('avg_vibration_mm_s')} mm/s")
        print(f"Max Vibration       : {item.get('max_vibration_mm_s')} mm/s")
        print(f"P99 Vibration       : {item.get('p99_vibration_mm_s')} mm/s")
        print(f"Avg RPM             : {item.get('avg_rpm')}")
        print(f"Temperature Anomaly : {item.get('temperature_anomaly')}")
        print(f"Vibration Anomaly   : {item.get('vibration_anomaly')}")
        print("-" * 40)


if __name__ == "__main__":
    fetch_and_display_table(telemetry_table, "telemetry")
    fetch_and_display_table(anomalies_table, "anomalies")
