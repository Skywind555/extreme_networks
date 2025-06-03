import os
import pathlib
import pandas as pd
from google.cloud import storage, bigquery
from helper import get

# Env setup
BUCKET_NAME = get("GCS_BUCKET")
PROJECT_ID  = get("GCP_PROJECT")
DATASET_ID  = get("BQ_DATASET_SOURCE")
KEY_PATH    = get("GOOGLE_APPLICATION_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

storage_client = storage.Client()
bq_client = bigquery.Client()
RAW_DIR = pathlib.Path(__file__).resolve().parent / "raw_data"

def infer_string_schema(csv_path):
    # Use pandas to read only the header row
    header = pd.read_csv(csv_path, nrows=0).columns
    return [bigquery.SchemaField(col, "STRING") for col in header]

def upload(local_path, blob_name):
    storage_client.bucket(BUCKET_NAME).blob(blob_name).upload_from_filename(local_path)
    print(f"{local_path} -> gs://{BUCKET_NAME}/{blob_name}")

def load_csv(gcs_uri, table, schema):
    cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=schema,
        write_disposition="WRITE_TRUNCATE"
    )
    bq_client.load_table_from_uri(
        gcs_uri,
        f"{PROJECT_ID}.{DATASET_ID}.{table}",
        job_config=cfg
    ).result()
    print(f"Loaded {gcs_uri} into {DATASET_ID}.{table} (all STRING columns)")

# Usage for each file
for filename, tablename in [
    ("customer_day1.csv", "customer"),
    ("revenue_actual.csv", "revenue"),
]:
    csv_path = RAW_DIR / filename
    upload(csv_path, filename)
    schema = infer_string_schema(csv_path)
    load_csv(f"gs://{BUCKET_NAME}/{filename}", tablename, schema)
