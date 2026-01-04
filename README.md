# Google Pub/Sub Client

A Python client for publishing events to Google Cloud Pub/Sub topics.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure environment variables in `.env`:
   ```bash
   GOOGLE_CLOUD_PROJECT=your-project-id
   PUBSUB_TOPIC=your-topic-name
   GOOGLE_APPLICATION_CREDENTIALS=cred.json
   ```


### Publish from JSON file:
```bash
python3 publish_event.py --from-file example_event.json
```

## Checking Messages

### Option 1: Google Cloud Console

1. Go to [Google Cloud Console → Pub/Sub → Topics](https://console.cloud.google.com/cloudpubsub/topic/list)
2. Click on your topic name
3. Click **"Create Subscription"** (if you don't have one)
   - Subscription ID: `test-subscription` (or any name)
   - Delivery type: **Pull**
   - Click **Create**
4. Go to **Subscriptions** in the left menu
5. Click on your subscription
6. Click **"View Messages"** or **"Pull"** to see messages


