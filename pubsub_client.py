"""
Google Pub/Sub Client for publishing events to topics.
"""

import json
import logging
from typing import Dict, List, Optional, Union, Any
from google.cloud import pubsub_v1
from google.api_core import exceptions

logger = logging.getLogger(__name__)


class PubSubClient:
    """Client for publishing messages to Google Cloud Pub/Sub topics."""

    def __init__(
        self,
        project_id: str,
        topic_name: str,
        credentials_path: Optional[str] = None,
    ):
        """
        Initialize the Pub/Sub client.

        Args:
            project_id: Google Cloud project ID
            topic_name: Name of the Pub/Sub topic to publish to
            credentials_path: Optional path to service account credentials JSON file.
                            If not provided, uses default credentials.
        """
        self.project_id = project_id
        self.topic_name = topic_name
        self.topic_path = None
        self.publisher = None

        # Initialize publisher client
        if credentials_path:
            self.publisher = pubsub_v1.PublisherClient.from_service_account_json(
                credentials_path
            )
        else:
            self.publisher = pubsub_v1.PublisherClient()

        # Construct the fully qualified topic path
        self.topic_path = self.publisher.topic_path(project_id, topic_name)

        # Verify topic exists or create it
        self._ensure_topic_exists()

    def _ensure_topic_exists(self):
        """Verify that the topic exists, create if it doesn't."""
        try:
            self.publisher.get_topic(request={"topic": self.topic_path})
            logger.info(f"Topic {self.topic_path} exists")
        except exceptions.NotFound:
            logger.warning(f"Topic {self.topic_path} not found. Creating it...")
            try:
                self.publisher.create_topic(request={"name": self.topic_path})
                logger.info(f"Topic {self.topic_path} created successfully")
            except Exception as e:
                logger.error(f"Failed to create topic: {e}")
                raise

    def publish(
        self,
        data: Union[str, bytes, Dict[str, Any]],
        attributes: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Publish a single message to the topic.

        Args:
            data: Message data. Can be a string, bytes, or dictionary (will be JSON encoded).
            attributes: Optional dictionary of message attributes (key-value pairs).

        Returns:
            Message ID of the published message.

        Raises:
            Exception: If publishing fails.
        """
        # Convert data to bytes
        if isinstance(data, dict):
            data_bytes = json.dumps(data).encode("utf-8")
        elif isinstance(data, str):
            data_bytes = data.encode("utf-8")
        elif isinstance(data, bytes):
            data_bytes = data
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")

        # Prepare attributes
        attrs = attributes or {}

        try:
            # Publish the message
            future = self.publisher.publish(
                self.topic_path, data_bytes, **attrs
            )
            message_id = future.result()
            logger.info(f"Published message {message_id} to {self.topic_path}")
            return message_id
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            raise

    def publish_batch(
        self,
        messages: List[Dict[str, Any]],
    ) -> List[str]:
        """
        Publish multiple messages to the topic in a batch.

        Args:
            messages: List of message dictionaries. Each dictionary should have:
                     - 'data': The message data (str, bytes, or dict)
                     - 'attributes': Optional dict of message attributes

        Returns:
            List of message IDs for published messages.

        Raises:
            Exception: If batch publishing fails.
        """
        message_ids = []
        futures = []

        for msg in messages:
            data = msg.get("data")
            attributes = msg.get("attributes")

            # Convert data to bytes
            if isinstance(data, dict):
                data_bytes = json.dumps(data).encode("utf-8")
            elif isinstance(data, str):
                data_bytes = data.encode("utf-8")
            elif isinstance(data, bytes):
                data_bytes = data
            else:
                raise ValueError(f"Unsupported data type: {type(data)}")

            # Prepare attributes
            attrs = attributes or {}

            try:
                future = self.publisher.publish(
                    self.topic_path, data_bytes, **attrs
                )
                futures.append(future)
            except Exception as e:
                logger.error(f"Failed to queue message for publishing: {e}")
                raise

        # Wait for all messages to be published
        for future in futures:
            try:
                message_id = future.result()
                message_ids.append(message_id)
                logger.debug(f"Published message {message_id}")
            except Exception as e:
                logger.error(f"Failed to publish message: {e}")
                raise

        logger.info(f"Published {len(message_ids)} messages to {self.topic_path}")
        return message_ids

    def publish_json(
        self,
        data: Dict[str, Any],
        attributes: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Convenience method to publish a JSON-serializable dictionary.

        Args:
            data: Dictionary to be JSON encoded and published.
            attributes: Optional dictionary of message attributes.

        Returns:
            Message ID of the published message.
        """
        return self.publish(data, attributes)

    def close(self):
        """Close the publisher client and release resources."""
        # PublisherClient doesn't have a close() method, but we keep this
        # method for API compatibility. Resources are managed by the client library.
        logger.debug("Pub/Sub publisher client cleanup (no-op)")

