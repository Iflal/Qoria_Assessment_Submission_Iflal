import os
from google.cloud import bigquery
from google.cloud import storage
import json
import logging

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "qoria-assessment-staging-iflal")
DATASET_ID = "gigmart"
REGION = "asia-east1"
CREDENTIALS_PATH = os.getenv("GOOGLE_KEY_PATH", os.path.join(os.path.dirname(__file__), "..", "key.json"))

storage_client = storage.Client(project=PROJECT_ID)
bq_client = bigquery.Client(project=PROJECT_ID)

def upload_to_gcs(local_file_path, destination_blob_name):
    
    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_file_path)
        logging.info(f"File {local_file_path} uploaded to {destination_blob_name}.")
        return f"gs://{BUCKET_NAME}/{destination_blob_name}"
    except Exception as e:
        logging.error(f"Failed to upload {local_file_path}: {e}")
        return None

def create_dataset_if_not_exists():
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        bq_client.get_dataset(dataset_id)
        logging.info(f"Dataset {dataset_id} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = REGION
        bq_client.create_dataset(dataset, timeout=30)
        logging.info(f"Created dataset {dataset_id}.")

def load_csv_to_bq(gcs_uri, table_name):
    """Loads a CSV file from GCS to a BigQuery table."""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        # CHALLENGE 4: Handle Schema Evolution
        # Allow adding new fields found in CSV to the table schema automatically
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # Allow some bad records instead of failing entire load
        max_bad_records=100,
        ignore_unknown_values=True
    )

    try:
        load_job = bq_client.load_table_from_uri(
            gcs_uri, table_id, job_config=job_config
        )
        load_job.result()
        
        table = bq_client.get_table(table_id)
        logging.info(f"Loaded {table.num_rows} rows to {table_id}")
        
        if load_job.errors:
            logging.warning(f"Bad records in {table_name}: {len(load_job.errors)} errors")
            for error in load_job.errors[:5]:  # Log first 5 errors
                logging.warning(f"  Error: {error}")
        
        if table.num_rows == 0:
            logging.warning(f"WARNING: Zero rows loaded to {table_id}")
    except Exception as e:
        logging.error(f"Failed to load {table_name}: {e}")

def load_json_to_bq(gcs_uri, table_name):
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    # Define explicit schema for POS to prevent autodetect conflicts
    schema = None
    if table_name == "stg_pos_transactions":
        schema = [
            bigquery.SchemaField("transaction_id", "STRING"),
            bigquery.SchemaField("store_id", "STRING"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("product_id", "STRING"),
            bigquery.SchemaField("transaction_date", "DATE"),
            bigquery.SchemaField("quantity", "INTEGER"),
            bigquery.SchemaField("unit_price", "FLOAT"),
            bigquery.SchemaField("amount", "FLOAT"),
            bigquery.SchemaField("event_type", "STRING"),
            bigquery.SchemaField("currency", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
        ]
    elif table_name == "stg_ecommerce_orders":
        schema = [
            bigquery.SchemaField("order_id", "STRING"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("product_id", "STRING"),
            bigquery.SchemaField("order_date", "DATE"),
            bigquery.SchemaField("quantity", "INTEGER"),
            bigquery.SchemaField("unit_price", "FLOAT"),
            bigquery.SchemaField("store_id", "STRING"),
            bigquery.SchemaField("total_amount", "FLOAT"),
            bigquery.SchemaField("payment_type", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
        ]
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema, 
        autodetect=(schema is None),  
        # CHALLENGE 4: Handle Schema Evolution for JSON
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ] if schema else [],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        max_bad_records=100,
        ignore_unknown_values=True
    )

    try:
        load_job = bq_client.load_table_from_uri(
            gcs_uri, table_id, job_config=job_config
        )
        load_job.result()
        
        table = bq_client.get_table(table_id)
        logging.info(f"Loaded {table.num_rows} rows to {table_id}")
        
        
        if load_job.errors:
            logging.warning(f"Bad records in {table_name}: {len(load_job.errors)} errors")
            for error in load_job.errors[:5]:  
                logging.warning(f"  Error: {error}")
        
      
        if table.num_rows == 0:
            logging.warning(f"WARNING: Zero rows loaded to {table_id}")
    except Exception as e:
        logging.error(f"Failed to load {table_name}: {e}")

def main():
    base_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    base_dir = os.path.abspath(base_dir)
    logging.info(f"Looking for data in: {base_dir}")
    create_dataset_if_not_exists()
    
    ingestion_map = {
        "crm": ("stg_crm_customers", load_csv_to_bq),
        "erp": ("stg_erp_inventory", load_csv_to_bq),
        "ecommerce": ("stg_ecommerce_orders", load_json_to_bq),
        "pos": ("stg_pos_transactions", load_json_to_bq)
    }

    for source_folder, (table_name, loader_func) in ingestion_map.items():
        folder_path = os.path.join(base_dir, source_folder)
        if not os.path.exists(folder_path):
            logging.warning(f"Directory not found: {folder_path}")
            continue

        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            
            gcs_blob_name = f"{source_folder}/{filename}"
            
            gcs_uri = upload_to_gcs(file_path, gcs_blob_name)
            
            if gcs_uri:
                 if filename.endswith(".csv") or filename.endswith(".json"):
                    logging.info(f"Triggering BigQuery load for {table_name}...")
                    loader_func(gcs_uri, table_name)

if __name__ == "__main__":
    main()
