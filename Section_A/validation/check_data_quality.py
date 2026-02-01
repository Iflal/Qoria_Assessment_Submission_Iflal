from google.cloud import bigquery
import logging
import sys
import os

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = "gigmart"

CREDENTIALS_PATH = os.getenv("GOOGLE_KEY_PATH", os.path.join(os.path.dirname(__file__), "..", "key.json"))


client = bigquery.Client(project=PROJECT_ID)
logging.info(f"Using credentials from: {CREDENTIALS_PATH}")


def run_test(test_name, query, expected_result=0):
    try:
        query_job = client.query(query)
        results = list(query_job.result())
        count = results[0][0] if results else 0
        
        if count == expected_result:
            logging.info(f"[PASS] {test_name}")
            return True
        else:
            logging.error(f"[FAIL] {test_name}: Expected {expected_result}, Got {count}")
            return False
    except Exception as e:
        logging.error(f"[ERROR] {test_name}: {e}")
        return False

def main():
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    logging.info(f"Starting Data Quality Checks on {dataset_ref}...")
    
    all_tests_passed = True

    # -------------------------------------------------------------------------
    # Test 1: Uniqueness Check
    # Ensure no duplicate transactions in Fact table
    # -------------------------------------------------------------------------
    query_uniqueness = f"""
        SELECT COUNT(*) 
        FROM (
            SELECT transaction_id, COUNT(*)
            FROM `{dataset_ref}.fact_transactions`
            GROUP BY transaction_id
            HAVING COUNT(*) > 1
        )
    """
    if not run_test("Uniqueness Check (Fact Transactions)", query_uniqueness, 0):
        all_tests_passed = False

    # -------------------------------------------------------------------------
    # Test 2: Referential Integrity Check
    # Ensure every transaction maps to a valid customer_sk (even if it's -1)
    # -------------------------------------------------------------------------
    query_ref_integrity = f"""
        SELECT COUNT(*)
        FROM `{dataset_ref}.fact_transactions` f
        LEFT JOIN `{dataset_ref}.dim_customers` d ON f.customer_sk = d.customer_sk
        WHERE d.customer_sk IS NULL
    """
    if not run_test("Referential Integrity (Customer FK)", query_ref_integrity, 0):
        all_tests_passed = False

    # -------------------------------------------------------------------------
    # Test 3: Null Check
    # Critical columns like Total Amount should not be NULL
    # -------------------------------------------------------------------------
    query_nulls = f"""
        SELECT COUNT(*)
        FROM `{dataset_ref}.fact_transactions`
        WHERE total_amount IS NULL OR created_at IS NULL
    """
    if not run_test("Null Value Check (Critical Columns)", query_nulls, 0):
        all_tests_passed = False

    # -------------------------------------------------------------------------
    # Test 4: SCD Validity Check
    # Ensure valid_to is always >= valid_from
    # -------------------------------------------------------------------------
    query_scd_logic = f"""
        SELECT COUNT(*)
        FROM `{dataset_ref}.dim_customers`
        WHERE valid_to IS NOT NULL AND valid_to < valid_from
    """
    if not run_test("SCD Logic Check (Time Travel)", query_scd_logic, 0):
        all_tests_passed = False

    if all_tests_passed:
        logging.info("All Data Quality checks passed successfully!")
        return True
    else:
        logging.error("Some Data Quality checks failed.")
        return False

def run_checks():
    """Wrapper for Airflow - raises exception on failure"""
    if not main():
        raise ValueError("Data Quality checks failed!")

if __name__ == "__main__":
    result = main()
    sys.exit(0 if result else 1)
