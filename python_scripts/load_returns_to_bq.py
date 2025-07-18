import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# Constants
PROJECT_ID = "my-gcp-project-466307"
DATASET_ID = "ecommerce_returns"
TABLE_ID = "returns_data"
CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "returns_sample_cleaned.csv")

# Authenticate
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not credentials_path or not os.path.exists(credentials_path):
    raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS not set or file not found.")

credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# Read and preprocess CSV
df = pd.read_csv(CSV_PATH, encoding="utf-8", on_bad_lines="skip")

# Best practice: Explicit datetime parsing for date columns
df["return_date"] = pd.to_datetime(df["return_date"], errors="coerce")

# Optional: Parse other known datetime columns if available
datetime_cols = ["order_purchase_timestamp", "order_delivered_customer_date", "shipping_limit_date"]
for col in datetime_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")

# Define full table path
full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Load config with explicit schema for best performance
job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    autodetect=True,
)

# Upload to BigQuery
job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
job.result()  # Wait for job to complete

# Confirm success
print(f"✅ Data successfully uploaded to BigQuery table: {full_table_id}")
