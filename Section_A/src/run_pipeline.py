import os
import logging
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = "gigmart"
REGION = "asia-east1"

CREDENTIALS_PATH = os.getenv("GOOGLE_KEY_PATH", os.path.join(os.path.dirname(__file__), "..", "key.json"))

client = bigquery.Client(project=PROJECT_ID)

def run_bq_script(file_path):
    if not os.path.exists(file_path):
        logging.error(f"SQL file not found: {file_path}")
        return False

    logging.info(f"Reading SQL script: {file_path}")
    with open(file_path, 'r') as f:
        query = f.read()

    try:

        query = query.replace('`gigmart.', f'`{PROJECT_ID}.gigmart.')
        
        job_config = bigquery.QueryJobConfig(
            use_legacy_sql=False,
            default_dataset=f"{PROJECT_ID}.{DATASET_ID}"
        )
        
        logging.info(f"Starting execution of {file_path}...")
        query_job = client.query(query, job_config=job_config)
        query_job.result()
        logging.info(f"Successfully executed {file_path}.")
        return True
    except Exception as e:
        logging.error(f"Error running {file_path}: {e}")
        return False

def create_dataset_if_not_exists():
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_id)
        logging.info(f"Dataset {dataset_id} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = REGION
        client.create_dataset(dataset, timeout=30)
        logging.info(f"Created dataset {dataset_id}.")

def create_dimension_tables():   

    create_dim_customers = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.dim_customers` (
        customer_sk INT64 NOT NULL,
        customer_id STRING NOT NULL,
        first_name STRING,
        last_name STRING,
        email STRING,
        loyalty_status STRING,
        city STRING,
        country STRING,
        valid_from TIMESTAMP NOT NULL,
        valid_to TIMESTAMP,
        is_current BOOL NOT NULL
    )
    """
    
    create_dim_product = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.dim_product` (
        product_sk INT64 NOT NULL,
        product_id STRING NOT NULL,
        product_name STRING,
        category STRING,
        brand STRING,
        supplier STRING,
        valid_from TIMESTAMP NOT NULL,
        valid_to TIMESTAMP,
        is_current BOOL NOT NULL
    )
    """
    
    create_fact_transactions = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.fact_transactions` (
        transaction_id STRING NOT NULL,
        customer_sk INT64,
        product_id STRING,
        transaction_date DATE,
        quantity INT64,
        unit_price FLOAT64,
        total_amount FLOAT64,
        source_system STRING,
        created_at TIMESTAMP
    )
    """
    
    tables = [
        ("dim_customers", create_dim_customers),
        ("dim_product", create_dim_product),
        ("fact_transactions", create_fact_transactions)
    ]
    
    for table_name, create_sql in tables:
        try:
            client.query(create_sql).result()
            logging.info(f"Table {table_name} created or already exists.")
        except Exception as e:
            logging.error(f"Error creating {table_name}: {e}")

def main():

    base_dir = os.path.join(os.path.dirname(__file__), "..")
    base_dir = os.path.abspath(base_dir)
    
    logging.info("Starting Transformation Pipeline...")
    
    # Create dataset and tables
    create_dataset_if_not_exists()
    create_dimension_tables()
    
    # Define the order of execution
    # 1. Update Customer Dimension (SCD Type 2)
    # 2. Build Product Dimension (From ERP)
    # 3. Cleanse POS Data
    # 4. Cleanse E-commerce Data
    # 5. Create Audit Log Table (Run once)
    # 6. Build Fact Transactions Table
    
    scripts = [
        os.path.join(base_dir, "sql/dim_customers_scd2_bq.sql"),
        os.path.join(base_dir, "sql/dim_product_bq.sql"),
        os.path.join(base_dir, "sql/stg_pos_clean_bq.sql"),
        os.path.join(base_dir, "sql/stg_ecommerce_clean_bq.sql"),
        os.path.join(base_dir, "sql/audit_log_bq.sql"),
        os.path.join(base_dir, "sql/fact_transactions_bq.sql")
    ]

    logging.info("Starting Transformation Pipeline...")

    for script in scripts:
        success = run_bq_script(script)
        if not success:
            logging.error(f"Pipeline stopped due to failure in {script}")
            break
            
    logging.info("Transformation Pipeline Completed.")

if __name__ == "__main__":
    main()