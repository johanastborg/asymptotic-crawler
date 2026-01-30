import os
import asyncio
import aiohttp
import logging
from google.cloud import pubsub_v1
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from google.api_core.exceptions import DeadlineExceeded, RetryError

# Configuration
PROJECT_ID = os.getenv("PROJECT_ID", "my-project")
SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID", "crawler-sub")
TOPIC_ID = os.getenv("TOPIC_ID", "crawler-topic")
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "10"))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def fetch_and_parse(session, url):
    """
    Fetches the URL and extracts all valid links.
    """
    try:
        logging.info(f"Fetching {url}")
        async with session.get(url, timeout=10) as response:
            if response.status != 200:
                logging.warning(f"Failed to fetch {url}: Status {response.status}")
                return []

            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            links = []
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                full_url = urljoin(url, href)
                # Basic filtering for http/https
                parsed = urlparse(full_url)
                if parsed.scheme in ('http', 'https'):
                    links.append(full_url)
            return links
    except Exception as e:
        logging.error(f"Error processing {url}: {e}")
        return []

async def process_messages(messages, publisher, topic_path):
    """
    Processes a batch of messages: fetches URLs and publishes found links.
    """
    async with aiohttp.ClientSession() as session:
        tasks = []
        for msg in messages:
            url = msg.message.data.decode('utf-8').strip()
            tasks.append(fetch_and_parse(session, url))

        # Fork-Join: Wait for all fetches to complete
        results = await asyncio.gather(*tasks)

        # Publish new links
        # Note: In a production system, we should deduplicate links before publishing.
        for links in results:
            for link in links:
                try:
                    publisher.publish(topic_path, link.encode('utf-8'))
                except Exception as e:
                    logging.error(f"Failed to publish link {link}: {e}")

def main():
    subscriber = pubsub_v1.SubscriberClient()
    publisher = pubsub_v1.PublisherClient()

    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    logging.info(f"Worker starting. Listening on {subscription_path}")

    while True:
        try:
            # Synchronous pull
            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": MAX_MESSAGES},
                timeout=5.0
            )

            if not response.received_messages:
                continue

            messages = response.received_messages
            logging.info(f"Received {len(messages)} messages")

            # Run async processing
            asyncio.run(process_messages(messages, publisher, topic_path))

            # Acknowledge
            ack_ids = [msg.ack_id for msg in messages]
            subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})
            logging.info(f"Acknowledged {len(ack_ids)} messages")

        except (RetryError, DeadlineExceeded):
            # Timeout in pull is expected if no messages are available immediately
            continue
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
            # Sleep briefly to avoid busy loop in case of persistent errors
            import time
            time.sleep(1)

if __name__ == "__main__":
    main()
