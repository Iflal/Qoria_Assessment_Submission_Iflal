from google.cloud import pubsub_v1
import json
import time
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_ID = "pos-transactions"

CREDENTIALS_PATH = os.getenv("GOOGLE_KEY_PATH", os.path.join(os.path.dirname(__file__), "..", "key.json"))


publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def publish_messages():
    base_dir = os.path.join(os.path.dirname(__file__), "..")
    file_path = os.path.join(base_dir, "data/pos/pos_data_day1.json")
    
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return

    with open(file_path, "r") as f:
        logging.info(f"Streaming data from {file_path} to {topic_path}...")
        
        for line in f:
            try:
                
                time.sleep(60) 
                data = json.loads(line)
                data['published_at'] = datetime.utcnow().isoformat()

                future = publisher.publish(
                    topic_path, 
                    json.dumps(data).encode("utf-8")
                )
                logging.info(f"Published transaction: {data.get('transaction_id')} | ID: {future.result()}")
                
            except json.JSONDecodeError:
                logging.warning(f"Skipping invalid JSON line: {line.strip()}")
            except Exception as e:
                logging.error(f"Error publishing: {e}")

if __name__ == "__main__":
    try:
        publisher.create_topic(request={"name": topic_path})
        logging.info(f"Created topic: {topic_path}")
    except Exception:
        logging.info(f"Topic {topic_path} already exists.")

    publish_messages()
