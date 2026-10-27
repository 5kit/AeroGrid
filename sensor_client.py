import csv
from datetime import datetime, timedelta
import io
import os
import random
import time
from datetime import timezone
import zipfile
import boto3
from botocore.exceptions import EndpointConnectionError, ClientError

ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")

aws_config = {
    "endpoint_url": ENDPOINT_URL,
    "region_name": "eu-west-2",
    "aws_access_key_id": "test",
    "aws_secret_access_key": "test",
}


def wait_for_localstack(max_retries=30, delay_seconds=2):
    print(f"Connecting to LocalStack at {ENDPOINT_URL}...")
    s3_check = boto3.client("s3", **aws_config)
    for attempt in range(1, max_retries + 1):
        try:
            s3_check.list_buckets()
            print("LocalStack is ready!\n")
            return True
        except (EndpointConnectionError, ClientError) as err:
            print(f"[{attempt}/{max_retries}] Waiting for LocalStack... ({err})")
            time.sleep(delay_seconds)
    raise RuntimeError("Could not connect to LocalStack.")


wait_for_localstack()

# Package Lambda
base_dir = os.path.dirname(os.path.abspath(__file__))
handler_path = os.path.join(base_dir, "lambda", "handler.py")
zip_path = os.path.join(base_dir, "lambda.zip")

print(f"Packaging {handler_path} into lambda.zip...")
with zipfile.ZipFile(zip_path, "w") as zipf:
    zipf.write(handler_path, arcname="handler.py")

s3 = boto3.client("s3", **aws_config)
lambda_client = boto3.client("lambda", **aws_config)
dynamodb_client = boto3.client("dynamodb", **aws_config)

bucket_name = "sensor-data"

try:
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
    )
except Exception:
    pass

tables = ["telemetry", "anomalies"]
for table_name in tables:
    try:
        dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "turbine_id", "KeyType": "HASH"},
                {"AttributeName": "batch_start", "KeyType": "RANGE"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "turbine_id", "AttributeType": "S"},
                {"AttributeName": "batch_start", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST"
        )
    except Exception:
        pass

with open(zip_path, "rb") as f:
    zipped_code = f.read()

function_name = "process-sensor-data"

try:
    lambda_client.delete_function(FunctionName=function_name)
except Exception:
    pass

lambda_response = lambda_client.create_function(
    FunctionName=function_name,
    Runtime="python3.10",
    Role="arn:aws:iam::000000000000:role/lambda-role",
    Handler="handler.lambda_handler",
    Code={"ZipFile": zipped_code},
)
lambda_arn = lambda_response["FunctionArn"]

try:
    lambda_client.add_permission(
        FunctionName=function_name,
        StatementId="s3-invoke-permission",
        Action="lambda:InvokeFunction",
        Principal="s3.amazonaws.com",
        SourceArn=f"arn:aws:s3:::{bucket_name}",
        SourceAccount="000000000000"
    )
except Exception:
    pass

time.sleep(2)

s3.put_bucket_notification_configuration(
    Bucket=bucket_name,
    NotificationConfiguration={
        "LambdaFunctionConfigurations": [
            {
                "LambdaFunctionArn": lambda_arn,
                "Events": ["s3:ObjectCreated:*"]
            }
        ]
    }
)

turbine_ids = ["T-01", "T-02", "T-04", "T-05", "T-06", "T-07", "T-08", "T-09"]
turbine_states = {
    tid: {"temperature_c": 60.0, "vibration_mm_s": 5.0, "rpm": 15.0}
    for tid in turbine_ids
}

SAMPLES_PER_BATCH = 100
BATCH_DURATION_MINUTES = 10

print("\nStarting telemetry generation...")

try:
    while True:
        batch_start = datetime.now(timezone.utc)
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)

        writer.writerow(["timestamp", "turbine_id", "temperature_c", "vibration_mm_s", "rpm"])

        for sample_index in range(SAMPLES_PER_BATCH):
            seconds_offset = (BATCH_DURATION_MINUTES * 60 * sample_index) / SAMPLES_PER_BATCH
            simulated_time = batch_start + timedelta(seconds=seconds_offset)

            # ISO 8601 string with millisecond precision
            timestamp_str = simulated_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

            for turbine_id in turbine_ids:
                state = turbine_states[turbine_id]

                # T-02: Single isolated temperature noise spike at sample index 50
                if turbine_id == "T-02" and sample_index == 50:
                    temp = 96.0
                elif turbine_id == "T-04":  # Sustained Overheating
                    temp = round(random.uniform(85.5, 88.0), 1)
                else:
                    temp = round(random.uniform(55.0, 65.0), 1)

                # T-07: Sustained Excessive Vibration
                if turbine_id == "T-07":
                    vib = round(random.uniform(15.2, 16.8), 1)
                else:
                    vib = round(random.uniform(3.0, 7.0), 1)

                rpm = round(random.uniform(12.0, 18.0), 1)

                writer.writerow([timestamp_str, turbine_id, temp, vib, rpm])

        file_key = f"polls/batch_{batch_start.strftime('%Y%m%d_%H%M%S_%f')}.csv"
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())

        print(f"Uploaded: s3://{bucket_name}/{file_key} ({SAMPLES_PER_BATCH * len(turbine_ids)} rows)")
        time.sleep(10)

except KeyboardInterrupt:
    print("\nTelemetry simulation stopped.")
