"""
Load JSONL source files into BigQuery raw tables.

Usage:
    pip install google-cloud-bigquery
    export GOOGLE_CLOUD_PROJECT=your-project-id
    python load_to_bq.py
"""

import os
from google.cloud import bigquery

PROJECT_ID = "sandbox-adi"
DATASET_ID = "cove_staging"
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

TABLES = {
    "properties": "properties.jsonl",
    "rooms": "rooms.jsonl",
    "tenancies": "tenancies.jsonl",
}


def main():
    client = bigquery.Client(project=PROJECT_ID)

    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)
    print(f"Dataset {DATASET_ID} ready.")

    for table_name, file_name in TABLES.items():
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        file_path = os.path.join(DATA_DIR, file_name)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        with open(file_path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)

        job.result()
        print(f"Loaded {table_name}: {job.output_rows} rows")


if __name__ == "__main__":
    main()
