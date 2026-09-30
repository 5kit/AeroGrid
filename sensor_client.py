import csv
from datetime import datetime, timedelta
import io
import random
import time
import zipfile
import boto3
import os


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

s3 = boto3.client("s3", **aws_config)

lambda_client = boto3.client("lambda", **aws_config)

dynamodb = boto3.client("dynamodb", **aws_config)


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


# ============================================================
# Initialize DynamoDB table
# ============================================================

# Create DynamoDB tables with a composite primary key (turbine_id + batch_end)
dynamodb_client = boto3.client(
    "dynamodb",
    endpoint_url=ENDPOINT_URL,
    region_name="eu-west-2",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

tables_to_create = ["telemetry", "anomalies"]
for table_name in tables_to_create:
    try:
        dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "turbine_id", "KeyType": "HASH"},  # Partition Key
                {"AttributeName": "batch_start", "KeyType": "RANGE"}   # Sort Key (keeps history!)
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

with open("lambda.zip", "rb") as f:
    zipped_code = f.read()

function_name = "process-sensor-data"

# Delete previous version so that the code is always updated
try:
    lambda_client.delete_function(
        FunctionName=function_name
    )

    print(f"Deleted existing Lambda '{function_name}'.")

except Exception:
    pass


lambda_client.create_function(
    FunctionName=function_name,
    Runtime="python3.10",
    Role="arn:aws:iam::000000000000:role/lambda-role",
    Handler="handler.lambda_handler",
    Code={
        "ZipFile": zipped_code
    },
)

print(f"Lambda function '{function_name}' deployed successfully!")


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
    )

    print("Lambda permission granted to S3!")

except Exception:
    print("Lambda permission already exists.")


time.sleep(1)


# ============================================================
# Configure S3 → Lambda event
# ============================================================

s3.put_bucket_notification_configuration(
    Bucket=bucket_name,
    NotificationConfiguration={
        "LambdaFunctionConfigurations": [
            {
                "LambdaFunctionArn":
                    f"arn:aws:lambda:eu-west-2:000000000000:function:{function_name}",

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
print("LocalStack UI:   https://app.localstack.cloud")
print("S3 bucket:        sensor-data")
print("========================================\n")


# ============================================================
# Turbine simulation
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


# Maintain state so values drift rather than jumping randomly
turbine_states = {}

for turbine_id in turbine_ids:

    if turbine_id == "T-04":
        # Slightly elevated temperature.
        # The average should remain just above 85°C.
        initial_temperature = random.uniform(85.5, 87.0)
        initial_vibration = random.uniform(3, 8)

    elif turbine_id == "T-07":
        # Slightly elevated vibration.
        # Occasional readings should cross 15 mm/s.
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


print("\nStarting 10-minute telemetry batches...")
print("Press Ctrl+C to stop.\n")


# ============================================================
# Generate and upload batches
# ============================================================

try:

    while True:

        batch_start = datetime.now()

        print(
            f"Generating telemetry batch starting "
            f"{batch_start.strftime('%Y-%m-%d %H:%M:%S')}"
        )

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
        # Generate 10 simulated minutes of telemetry
        # ----------------------------------------------------

        simulated_time = batch_start

        for minute in range(10):

            timestamp_str = (
                f"{simulated_time.month}/"
                f"{simulated_time.day}/"
                f"{simulated_time.year} "
                f"{simulated_time.strftime('%H:%M:%S')}"
            )

            # Every turbine reports once during each simulated minute
            for turbine_id in turbine_ids:

                state = turbine_states[turbine_id]

                # --------------------------------------------
                # Temperature
                # --------------------------------------------

                if turbine_id == "T-04":

                    # Small fluctuations around the threshold.
                    # This should keep the batch average just
                    # above 85°C without looking artificial.
                    state["temperature_c"] = round(
                        state["temperature_c"]
                        + random.uniform(-0.6, 0.6),
                        1
                    )

                    state["temperature_c"] = max(
                        85.1,
                        min(88.0, state["temperature_c"])
                    )

                else:

                    state["temperature_c"] = round(
                        state["temperature_c"]
                        + random.uniform(-1.5, 1.5),
                        1
                    )

                    state["temperature_c"] = max(
                        40,
                        min(80, state["temperature_c"])
                    )


                # --------------------------------------------
                # Vibration
                # --------------------------------------------

                if turbine_id == "T-07":

                    # Keep vibration close to the threshold.
                    # The maximum should exceed 15 mm/s,
                    # but only slightly.
                    state["vibration_mm_s"] = round(
                        state["vibration_mm_s"]
                        + random.uniform(-0.3, 0.3),
                        1
                    )

                    state["vibration_mm_s"] = max(
                        15.1,
                        min(17.0, state["vibration_mm_s"])
                    )

                else:

                    state["vibration_mm_s"] = round(
                        max(
                            0,
                            state["vibration_mm_s"]
                            + random.uniform(-0.4, 0.4)
                        ),
                        1
                    )

                    state["vibration_mm_s"] = min(
                        12,
                        state["vibration_mm_s"]
                    )


                # --------------------------------------------
                # RPM
                # --------------------------------------------

                state["rpm"] = round(
                    max(
                        0,
                        state["rpm"]
                        + random.uniform(-1.5, 1.5)
                    ),
                    1
                )


                writer.writerow([
                    timestamp_str,
                    turbine_id,
                    state["temperature_c"],
                    state["vibration_mm_s"],
                    state["rpm"]
                ])


            # Move simulated clock forward one minute
            simulated_time += timedelta(minutes=1)


        # ----------------------------------------------------
        # Upload the completed 10-minute batch to S3
        # ----------------------------------------------------

        file_key = (
            f"polls/"
            f"batch_{batch_start.strftime('%Y%m%d_%H%M%S_%f')}.csv"
        )

        s3.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=csv_buffer.getvalue()
        )

        print(
            f"Uploaded 10-minute telemetry batch -> "
            f"s3://{bucket_name}/{file_key}"
        )

        print(
            f"Records uploaded: "
            f"{10 * len(turbine_ids)}"
        )


        # ----------------------------------------------------
        # Wait before next batch
        # ----------------------------------------------------

        print("Waiting 10 minutes for next batch...\n")

        time.sleep(10)


except KeyboardInterrupt:

    print("\nTelemetry simulation stopped.")
