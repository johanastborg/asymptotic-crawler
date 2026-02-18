# GKE Crawler

A scalable web crawler designed to run on Google Kubernetes Engine (GKE) Autopilot. It uses a master-worker architecture coordinated by Google Cloud Pub/Sub.

## Architecture

- **Master**: Seeds the initial URLs into a Pub/Sub topic.
- **Worker**: Consumes URLs from the subscription, fetches the content, extracts links, and publishes new links back to the topic. Workers use `aiohttp` for asynchronous HTTP requests and `BeautifulSoup` for HTML parsing.
- **Kueue**: Job management on Kubernetes.

## Features

- **Scalable**: Designed for GKE Autopilot.
- **Asynchronous**: High-performance crawling using `asyncio` and `aiohttp`.
- **WebMCP Support**: Automatically detects if a crawled page supports WebMCP (checks for `webmcp.js`).

## Usage

### Prerequisites

- Google Cloud Project with Pub/Sub enabled.
- GKE Cluster.
- Python 3.12+

### Running Locally

1. Install dependencies:
   ```bash
   pip install -r src/requirements.txt
   ```

2. Set environment variables:
   ```bash
   export PROJECT_ID=your-project-id
   export TOPIC_ID=crawler-topic
   export SUBSCRIPTION_ID=crawler-sub
   ```

3. Run the master to seed URLs:
   ```bash
   python src/master.py https://example.com
   ```

4. Run the worker:
   ```bash
   python src/worker.py
   ```

### Deployment

Kubernetes manifests are located in the `k8s/` directory.

## WebMCP Support

The crawler includes support for detecting WebMCP-enabled websites. When a page containing the `webmcp.js` script is crawled, the worker logs a "WebMCP detected" message. This allows for identifying sites that expose tools for AI agents via the WebMCP protocol.
