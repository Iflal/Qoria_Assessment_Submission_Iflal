from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
import ingest_gcp  # Importing our src script


PROJECT_ID = Variable.get("GCP_PROJECT_ID", default_var=" ")
DATASET_ID = "gigmart"

from datetime import timedelta

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    # CHALLENGE 8: Self-Healing/Recovery
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gigmart_retail_pipeline',
    default_args=default_args,
    description='GigaMart ELT: Ingest -> Staging -> Medallion (SCD2 + Facts)',
    schedule_interval='@daily',
    catchup=False,
    tags=['retail', 'production', 'gigmart']
) as dag:

    # 1. Ingestion Tasks (Using PythonOperator to call our script)
    # In a real Airflow setup on GCP (Cloud Composer), might use GCSToBigQueryOperator
    # directly, but reusing our logic ensures consistency.
    
    task_ingest_crm = PythonOperator(
        task_id='ingest_crm_data',
        python_callable=ingest_gcp.main, 
    )

    # Note: Ideally, ingest_gcp.py should be refactored to accept arguments 
    # so we can parallelize these tasks. For now, we run the main function 
    # which does all batch ingestions.

    # 2. SQL Transformations
    
    # Helper to read SQL from the 'sql/' folder relative to the DAG
    import os
    def read_sql(file_name):
        # cloud composer stores DAGs in /home/airflow/gcs/dags
        # so we look for sql folder there
        base_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(base_dir, 'sql', file_name)
        with open(file_path, 'r') as f:
            return f.read()

    # Task: Update Customer Dimension (SCD Type 2)
    task_update_dim_customers = BigQueryInsertJobOperator(
        task_id='update_dim_customers',
        configuration={
            "query": {
                "query": read_sql('dim_customers_scd2_bq.sql'),
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',
    )

    # Task: Build Product Dimension (From ERP)
    task_build_dim_product = BigQueryInsertJobOperator(
        task_id='build_dim_product',
        configuration={
            "query": {
                "query": read_sql('dim_product_bq.sql'),
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',
    )

    # Task: Create Audit Log Table (Run once)
    task_create_audit_table = BigQueryInsertJobOperator(
        task_id='create_audit_log_table',
        configuration={
            "query": {
                "query": read_sql('audit_log_bq.sql'),
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',
    )

    # Task: Cleanse POS Data
    task_cleanse_pos = BigQueryInsertJobOperator(
        task_id='cleanse_pos_transactions',
        configuration={
            "query": {
                "query": read_sql('stg_pos_clean_bq.sql'),
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',
    )

    # Task: Cleanse E-commerce Data
    task_cleanse_ecommerce = BigQueryInsertJobOperator(
        task_id='cleanse_ecommerce_orders',
        configuration={
            "query": {
                "query": read_sql('stg_ecommerce_clean_bq.sql'),
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',
    )

    # Task: Build Fact Transactions
    task_build_facts = BigQueryInsertJobOperator(
        task_id='build_fact_transactions',
        configuration={
            "query": {
                "query": read_sql('fact_transactions_bq.sql'),
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',
    )

    # 3. Data Quality Validation (INTEGRATED)
    import sys
    import os
    from validation import check_data_quality
    
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    
    task_data_quality_checks = PythonOperator(
        task_id='run_data_quality_checks',
        python_callable=check_data_quality.run_checks,
    )

    # --- Dependencies ---
    
    # Step 1: Ingest raw data
    # Step 2: Create audit infrastructure
    # Step 3: Build dimensions (customer & product)
    # Step 4: Cleanse staging data (POS & E-commerce)
    # Step 5: Build fact table from cleaned data
    # Step 6: Run data quality validation
    
    task_ingest_crm >> task_create_audit_table
    
    task_create_audit_table >> [task_update_dim_customers, task_build_dim_product, task_cleanse_pos, task_cleanse_ecommerce]
    
    [task_update_dim_customers, task_build_dim_product, task_cleanse_pos, task_cleanse_ecommerce] >> task_build_facts
    
    task_build_facts >> task_data_quality_checks
