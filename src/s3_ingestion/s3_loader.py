import boto3  # type: ignore
from database.config import get_config
import logging
import os
from pathlib import Path

config = get_config()
if not config:
    print("Failed to load configuration. Exiting.")
    exit(1)

# Access config
DATA_DIRECTORY = config.get("Paths", "data_directory")
AWS_ACCESS_KEY = config.get("AWS", "aws_access_key")
AWS_SECRET_KEY = config.get("AWS", "aws_secret_key")
AWS_S3_BUCKET_NAME = config.get("AWS", "aws_s3_bucket_name")

print(f"Bucket Name: {AWS_S3_BUCKET_NAME}")

ROOT_PATH = Path(__file__).resolve().parents[2]
LOG_PATH = os.path.join(ROOT_PATH / "logs", "s3_loader.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8", mode="a"),
        logging.StreamHandler(),
    ],
)

LOGGER = logging.getLogger(__name__)
LOGGER.info("S3 Loader started.")

s3_client = boto3.client(
    service_name="s3",
    region_name="us-east-1",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)

RAW_DIRECTORY = ROOT_PATH / "raw_data"

for file in RAW_DIRECTORY.iterdir():
    if file.is_file():
        s3_key = f"raw_data/{file.name}"
        response = s3_client.upload_file(str(file), AWS_S3_BUCKET_NAME, s3_key)

    LOGGER.info(f"upload raw files response: {response}")
