import os
import logging
import sys
from google.cloud import pubsub_v1

# Configuration
PROJECT_ID = os.getenv("PROJECT_ID", "my-project")
TOPIC_ID = os.getenv("TOPIC_ID", "crawler-topic")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    if len(sys.argv) < 2:
        print("Usage: python master.py <url1> [url2 ...]")
        sys.exit(1)

    urls = sys.argv[1:]

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    logging.info(f"Seeding {len(urls)} URLs to {topic_path}")

    futures = []
    for url in urls:
        future = publisher.publish(topic_path, url.encode('utf-8'))
        futures.append(future)

    for i, future in enumerate(futures):
        try:
            msg_id = future.result()
            logging.info(f"Published {urls[i]} - Message ID: {msg_id}")
        except Exception as e:
            logging.error(f"Failed to publish {urls[i]}: {e}")

if __name__ == "__main__":
    main()
