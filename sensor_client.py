import csv
from datetime import datetime, timedelta
import io
import os
import random
import time
import zipfile
import boto3
from botocore.exceptions import EndpointConnectionError, ClientError


# ============================================================
# AWS Environment & Configuration
# ============================================================

ENDPOINT_URL = os.getenv(
    "AWS_ENDPOINT_URL",
    "http://localhost:4566"
)

aws_config = {
    "endpoint_url": ENDPOINT_URL,
    "region_name": "eu-west-2",
    "aws_access_key_id": "test",
    "aws_secret_access_key": "test",
}


# ============================================================
# Wait for LocalStack Readiness
# ============================================================

def wait_for_localstack(max_retries=30, delay_seconds=2):
    """
    Poll LocalStack using boto3 until the service endpoint is reachable.
    """
    print(f"Connecting to LocalStack at {ENDPOINT_URL}...")
    s3_check = boto3.client("s3", **aws_config)

    for attempt in range(1, max_retries + 1):
        try:
            s3_check.list_buckets()
            print("LocalStack is up and ready!\n")
            return True
        except (EndpointConnectionError, ClientError) as err:
            print(f"[{attempt}/{max_retries}] Waiting for LocalStack to boot... ({err})")
            time.sleep(delay_seconds)

    raise RuntimeError(f"Could not connect to LocalStack at {ENDPOINT_URL} after {max_retries} attempts.")


# Run the readiness check before proceeding
wait_for_localstack()


# ============================================================
# Package Lambda
# ============================================================

base_dir = os.path.dirname(os.path.abspath(__file__))
handler_path = os.path.join(base_dir, "lambda", "handler.py")
zip_path = os.path.join(base_dir, "lambda.zip")

print(f"Packaging {handler_path} into lambda.zip...")

with zipfile.ZipFile(zip_path, "w") as zipf:
    zipf.write(handler_path, arcname="handler.py")

with zipfile.ZipFile(zip_path, "r") as zipf:
    print(f"Contents of lambda.zip: {zipf.namelist()}")


# ============================================================
# Initialize AWS clients
# ============================================================

s3 = boto3.client("s3", **aws_config)
lambda_client = boto3.client("lambda", **aws_config)
dynamodb_client = boto3.client("dynamodb", **aws_config)


# ============================================================
# Initialize S3 bucket
# ============================================================

bucket_name = "sensor-data"

try:
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            "LocationConstraint": "eu-west-2"
        }
    )
    print(f"S3 bucket '{bucket_name}' created.")
except s3.exceptions.BucketAlreadyOwnedByYou:
    print(f"S3 bucket '{bucket_name}' already exists.")
except s3.exceptions.BucketAlreadyExists:
    print(f"S3 bucket '{bucket_name}' already exists.")


# ============================================================
# Initialize DynamoDB tables
# ============================================================

tables_to_create = ["telemetry", "anomalies"]
for table_name in tables_to_create:
    try:
        dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "turbine_id", "KeyType": "HASH"},   # Partition Key
                {"AttributeName": "batch_start", "KeyType": "RANGE"}   # Sort Key
            ],
            AttributeDefinitions=[
                {"AttributeName": "turbine_id", "AttributeType": "S"},
                {"AttributeName": "batch_start", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST"
        )
        print(f"DynamoDB table '{table_name}' created successfully.")
    except dynamodb_client.exceptions.ResourceInUseException:
        print(f"DynamoDB table '{table_name}' already exists.")


# ============================================================
# Package and deploy Lambda
# ============================================================

with open(zip_path, "rb") as f:
    zipped_code = f.read()

function_name = "process-sensor-data"

# Delete previous version so that code is updated dynamically
try:
    lambda_client.delete_function(FunctionName=function_name)
    print(f"Deleted existing Lambda '{function_name}'.")
except Exception:
    pass

lambda_response = lambda_client.create_function(
    FunctionName=function_name,
    Runtime="python3.10",
    Role="arn:aws:iam::000000000000:role/lambda-role",
    Handler="handler.lambda_handler",
    Code={
        "ZipFile": zipped_code
    },
)

lambda_arn = lambda_response["FunctionArn"]
print(f"Lambda function '{function_name}' deployed successfully!")
print(f"Lambda ARN: {lambda_arn}")


# ============================================================
# Allow S3 to invoke Lambda
# ============================================================

try:
    lambda_client.add_permission(
        FunctionName=function_name,
        StatementId="s3-invoke-permission",
        Action="lambda:InvokeFunction",
        Principal="s3.amazonaws.com",
        SourceArn=f"arn:aws:s3:::{bucket_name}",
        SourceAccount="000000000000"
    )
    print("Lambda permission granted to S3!")
except lambda_client.exceptions.ResourceConflictException:
    print("Lambda permission already exists.")
except Exception as e:
    print(f"Notice on add_permission: {e}")

# Pause briefly for LocalStack event-bus propagation
time.sleep(2)


# ============================================================
# Configure S3 -> Lambda event
# ============================================================

s3.put_bucket_notification_configuration(
    Bucket=bucket_name,
    NotificationConfiguration={
        "LambdaFunctionConfigurations": [
            {
                "LambdaFunctionArn": lambda_arn,
                "Events": [
                    "s3:ObjectCreated:*"
                ]
            }
        ]
    }
)

print("S3 bucket notification successfully configured!")

print("\n========================================")
print("        AeroGrid Telemetry Simulator")
print("========================================")
print("LocalStack API:  ", ENDPOINT_URL)
print("S3 bucket:       ", bucket_name)
print("========================================\n")


# ============================================================
# Turbine simulation setup
# ============================================================

turbine_ids = [
    "T-01",
    "T-02",
    "T-04",
    "T-05",
    "T-06",
    "T-07",
    "T-08",
    "T-09"
]

turbine_states = {}

for turbine_id in turbine_ids:
    if turbine_id == "T-04":
        initial_temperature = random.uniform(85.5, 87.0)
        initial_vibration = random.uniform(3, 8)
    elif turbine_id == "T-07":
        initial_temperature = random.uniform(50, 70)
        initial_vibration = random.uniform(15.2, 16.2)
    else:
        initial_temperature = random.uniform(50, 70)
        initial_vibration = random.uniform(3, 8)

    turbine_states[turbine_id] = {
        "temperature_c": round(initial_temperature, 1),
        "vibration_mm_s": round(initial_vibration, 1),
        "rpm": round(random.uniform(10, 20), 1)
    }

print("Starting 10-minute telemetry batches...")
print("Press Ctrl+C to stop.\n")


# ============================================================
# Generate and upload batches
# ============================================================

SAMPLES_PER_BATCH = 100
BATCH_DURATION_MINUTES = 10

try:
    while True:
        batch_start = datetime.now()
        print(f"Generating telemetry batch starting {batch_start.strftime('%Y-%m-%d %H:%M:%S')}")

        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)

        writer.writerow([
            "timestamp",
            "turbine_id",
            "temperature_c",
            "vibration_mm_s",
            "rpm"
        ])

        # ----------------------------------------------------
        # Generate simulated minutes of telemetry
        # ----------------------------------------------------
        for sample_index in range(SAMPLES_PER_BATCH):
            seconds_offset = (BATCH_DURATION_MINUTES * 60 * sample_index) / SAMPLES_PER_BATCH
            simulated_time = batch_start + timedelta(seconds=seconds_offset)

            timestamp_str = (
                f"{simulated_time.month:02d}/"
                f"{simulated_time.day:02d}/"
                f"{simulated_time.year} "
                f"{simulated_time.strftime('%H:%M:%S')}"
            )

            for turbine_id in turbine_ids:
                state = turbine_states[turbine_id]

                # Temperature simulation step
                if turbine_id == "T-04":
                    state["temperature_c"] = round(
                        state["temperature_c"] + random.uniform(-0.6, 0.6), 1
                    )
                    state["temperature_c"] = max(85.1, min(88.0, state["temperature_c"]))
                else:
                    state["temperature_c"] = round(
                        state["temperature_c"] + random.uniform(-1.5, 1.5), 1
                    )
                    state["temperature_c"] = max(40.0, min(80.0, state["temperature_c"]))

                # Vibration simulation step
                if turbine_id == "T-07":
                    state["vibration_mm_s"] = round(
                        state["vibration_mm_s"] + random.uniform(-0.3, 0.3), 1
                    )
                    state["vibration_mm_s"] = max(15.1, min(17.0, state["vibration_mm_s"]))
                else:
                    state["vibration_mm_s"] = round(
                        max(0.0, state["vibration_mm_s"] + random.uniform(-0.4, 0.4)), 1
                    )
                    state["vibration_mm_s"] = min(12.0, state["vibration_mm_s"])

                # RPM simulation step
                state["rpm"] = round(
                    max(0.0, state["rpm"] + random.uniform(-1.5, 1.5)), 1
                )

                writer.writerow([
                    timestamp_str,
                    turbine_id,
                    state["temperature_c"],
                    state["vibration_mm_s"],
                    state["rpm"]
                ])

        # ----------------------------------------------------
        # Upload batch to S3
        # ----------------------------------------------------
        file_key = f"polls/batch_{batch_start.strftime('%Y%m%d_%H%M%S_%f')}.csv"

        s3.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=csv_buffer.getvalue()
        )

        total_records_sent = SAMPLES_PER_BATCH * len(turbine_ids)
        print(f"Uploaded telemetry batch -> s3://{bucket_name}/{file_key}")
        print(f"Records uploaded: {total_records_sent}")

        print("Waiting 10 seconds for next batch trigger...\n")
        time.sleep(10)

except KeyboardInterrupt:
    print("\nTelemetry simulation stopped.")
