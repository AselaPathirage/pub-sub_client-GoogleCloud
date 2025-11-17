"""
Script to publish events to Google Pub/Sub with the specified event structure.
"""

import os
import sys
import json
import logging
import argparse
from typing import Optional
from pubsub_client import PubSubClient

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def publish_event(
    session_id: str,
    prompt: str,
    image_base64: Optional[str] = None,
    speaking_rate: Optional[float] = None,
    language: Optional[str] = None,
    trace_id: Optional[str] = None,
    project_id: Optional[str] = None,
    topic_name: Optional[str] = None,
    credentials_path: Optional[str] = None,
) -> str:
    """
    Publish an event to Pub/Sub with the specified structure.

    Args:
        session_id: Unique identifier for the user session
        prompt: Text prompt or instruction
        image_base64: Optional base64-encoded image (if multimodal)
        speaking_rate: Optional float for TTS rate tuning
        language: Optional language code (e.g., 'en', 'es')
        trace_id: Optional trace ID for observability
        project_id: Google Cloud project ID (overrides env var)
        topic_name: Pub/Sub topic name (overrides env var)
        credentials_path: Path to credentials JSON (overrides env var)

    Returns:
        Message ID of the published message
    """
    # Get configuration from args or environment
    project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
    topic_name = topic_name or os.getenv("PUBSUB_TOPIC")
    credentials_path = credentials_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if not project_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT must be set in environment or passed as argument")
    if not topic_name:
        raise ValueError("PUBSUB_TOPIC must be set in environment or passed as argument")

    # Build event payload
    event_data = {
        "session_id": session_id,
        "prompt": prompt,
    }

    # Add optional fields if provided
    if image_base64 is not None:
        event_data["image_base64"] = image_base64
    if speaking_rate is not None:
        event_data["speaking_rate"] = speaking_rate
    if language is not None:
        event_data["language"] = language
    if trace_id is not None:
        event_data["trace_id"] = trace_id

    # Initialize client and publish
    client = PubSubClient(
        project_id=project_id,
        topic_name=topic_name,
        credentials_path=credentials_path,
    )

    try:
        # Add trace_id as an attribute for easier filtering
        attributes = {}
        if trace_id:
            attributes["trace_id"] = trace_id
        if session_id:
            attributes["session_id"] = session_id

        message_id = client.publish_json(event_data, attributes=attributes)
        logger.info(f"Successfully published event. Message ID: {message_id}")
        return message_id
    finally:
        client.close()


def main():
    """CLI entry point for publishing events."""
    parser = argparse.ArgumentParser(
        description="Publish events to Google Pub/Sub",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Publish a simple event
  python publish_event.py --session-id "sess_123" --prompt "Hello, world!"

  # Publish with all fields
  python publish_event.py \\
    --session-id "sess_123" \\
    --prompt "Describe this image" \\
    --image-base64 "iVBORw0KGgoAAAANS..." \\
    --speaking-rate 1.2 \\
    --language "en" \\
    --trace-id "trace_456"

  # Publish from JSON file
  python publish_event.py --from-file event.json
        """
    )

    parser.add_argument(
        "--session-id",
        help="Unique identifier for the user session (required if not using --from-file)"
    )
    parser.add_argument(
        "--prompt",
        help="Text prompt or instruction (required if not using --from-file)"
    )
    parser.add_argument(
        "--image-base64",
        help="Optional base64-encoded image (if multimodal)"
    )
    parser.add_argument(
        "--speaking-rate",
        type=float,
        help="Optional float for TTS rate tuning"
    )
    parser.add_argument(
        "--language",
        help="Optional language code (e.g., 'en', 'es')"
    )
    parser.add_argument(
        "--trace-id",
        help="Optional trace ID for observability"
    )
    parser.add_argument(
        "--from-file",
        help="Load event data from JSON file (overrides other args)"
    )
    parser.add_argument(
        "--project-id",
        help="Google Cloud project ID (overrides GOOGLE_CLOUD_PROJECT env var)"
    )
    parser.add_argument(
        "--topic",
        help="Pub/Sub topic name (overrides PUBSUB_TOPIC env var)"
    )
    parser.add_argument(
        "--credentials",
        help="Path to credentials JSON (overrides GOOGLE_APPLICATION_CREDENTIALS env var)"
    )

    args = parser.parse_args()

    # Validate arguments
    if not args.from_file:
        if not args.session_id or not args.prompt:
            parser.error("--session-id and --prompt are required when not using --from-file")

    try:
        # If --from-file is provided, load from file
        if args.from_file:
            with open(args.from_file, 'r') as f:
                event_data = json.load(f)
            
            # Validate required fields in JSON file
            if "session_id" not in event_data:
                raise ValueError("JSON file must contain 'session_id' field")
            if "prompt" not in event_data:
                raise ValueError("JSON file must contain 'prompt' field")
            
            message_id = publish_event(
                session_id=event_data.get("session_id"),
                prompt=event_data.get("prompt"),
                image_base64=event_data.get("image_base64"),
                speaking_rate=event_data.get("speaking_rate"),
                language=event_data.get("language"),
                trace_id=event_data.get("trace_id"),
                project_id=args.project_id,
                topic_name=args.topic,
                credentials_path=args.credentials,
            )
        else:
            message_id = publish_event(
                session_id=args.session_id,
                prompt=args.prompt,
                image_base64=args.image_base64,
                speaking_rate=args.speaking_rate,
                language=args.language,
                trace_id=args.trace_id,
                project_id=args.project_id,
                topic_name=args.topic,
                credentials_path=args.credentials,
            )

        print(f"✓ Event published successfully!")
        print(f"  Message ID: {message_id}")
        return 0

    except Exception as e:
        logger.error(f"Failed to publish event: {e}")
        print(f"✗ Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

