"""
RabbitMQQueue — Production-ready RabbitMQ queue handler for CloudAMQP or any AMQP broker.

Supports single/batch produce, blocking and non-blocking consume, JSON messages,
queue management, exchange binding, auto-reconnection, and context manager.
"""

from __future__ import annotations

import json
import logging
import threading
from time import sleep
from typing import Any, Callable, Dict, List, Optional, Union

try:
    import pika
    from pika.adapters.blocking_connection import BlockingChannel
    from pika.spec import BasicProperties
except ImportError:
    raise ImportError("Install pika: pip install pika")

logger = logging.getLogger(__name__)


class RabbitMQQueue:
    """
    Production-ready RabbitMQ queue handler.Supports CloudAMQP or any AMQP broker.

    Attributes:
        queue_name (str): Name of the queue.
        amqp_url (str): Full AMQP connection URL.
        durable (bool): Whether the queue survives broker restarts.
    """

    def __init__(
        self,
        queue_name: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: int = 5672,
        vhost: str = "/",
        amqp_url: Optional[str] = None,
        durable: bool = True,
        use_ssl: bool = False,
        heartbeat: int = 60,
        connection_attempts: int = 3,
        retry_delay: float = 5.0,
    ) -> None:
        """
        Initialize the queue handler. Connect via amqp_url or individual params.

        Args:
            queue_name: Name of the queue.
            username: RabbitMQ username (not needed if amqp_url provided).
            password: RabbitMQ password (not needed if amqp_url provided).
            host: RabbitMQ host (not needed if amqp_url provided).
            port: AMQP port (5672, 5671 for TLS).
            vhost: Virtual host.
            amqp_url: Full AMQP URL. If provided, username/password/host/port/vhost are ignored.
            durable: Whether the queue survives broker restarts.
            use_ssl: Use amqps:// scheme (ignored if amqp_url is provided).
            heartbeat: Heartbeat interval in seconds (0 to disable).
            connection_attempts: Max connection retries.
            retry_delay: Seconds between connection retries.
        """
        self.queue_name = queue_name
        self.durable = durable
        self.heartbeat = heartbeat
        self.connection_attempts = connection_attempts
        self.retry_delay = retry_delay

        if amqp_url:
            self.amqp_url = amqp_url
        else:
            protocol = "amqps" if use_ssl else "amqp"
            self.amqp_url = (
                f"{protocol}://{username}:{password}@{host}:{port}/{vhost.lstrip('/')}"
            )

        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[BlockingChannel] = None
        self._consumer_tag: Optional[str] = None
        self._consuming = False
        self._connect()

    # ──────────────────────────────────────────────
    # CONNECTION MANAGEMENT
    # ──────────────────────────────────────────────

    def _connect(self) -> None:
        """Establish connection and channel, declare the queue."""
        params = pika.URLParameters(self.amqp_url)
        params.heartbeat = self.heartbeat
        params.connection_attempts = self.connection_attempts
        params.retry_delay = self.retry_delay
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=self.durable)

    def _ensure_connection(self) -> None:
        """Reconnect if the connection is closed."""
        if self.connection is None or self.connection.is_closed:
            self._connect()

    @property
    def is_connected(self) -> bool:
        """Check if the connection is open."""
        return self.connection is not None and self.connection.is_open

    # ──────────────────────────────────────────────
    # PRODUCE
    # ──────────────────────────────────────────────

    def produce(
        self,
        message: Union[str, bytes, Dict[str, Any], List[Any]],
        persistent: bool = True,
        content_type: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Publish a single message to the queue.

        Args:
            message: String, bytes, or JSON-serializable dict/list.
            persistent: Make message survive broker restarts.
            content_type: MIME type (auto-set to application/json for dict/list).
            headers: Custom headers for the message.

        Returns:
            True if published successfully.
        """
        self._ensure_connection()
        if isinstance(message, (dict, list)):
            body = json.dumps(message, default=str)
            content_type = content_type or "application/json"
        elif isinstance(message, str):
            body = message
            content_type = content_type or "text/plain"
        else:
            body = message
            content_type = content_type or "application/octet-stream"

        props = BasicProperties(
            delivery_mode=2 if persistent else 1,
            content_type=content_type,
            headers=headers,
        )
        self.channel.basic_publish(
            exchange="",
            routing_key=self.queue_name,
            body=body.encode() if isinstance(body, str) else body,
            properties=props,
        )
        return True

    def produce_batch(
        self,
        messages: List[Union[str, bytes, Dict[str, Any], List[Any]]],
        persistent: bool = True,
    ) -> int:
        """
        Publish multiple messages to the queue.

        Args:
            messages: List of messages to publish.
            persistent: Make messages survive broker restarts.

        Returns:
            Number of messages published.
        """
        for msg in messages:
            self.produce(msg, persistent=persistent)
        return len(messages)

    # ──────────────────────────────────────────────
    # CONSUME (blocking)
    # ──────────────────────────────────────────────

    def consume(
        self,
        callback: Callable[[str, Optional[Dict[str, Any]]], Any],
        auto_ack: bool = False,
        prefetch: int = 1,
    ) -> None:
        """
        Start blocking consumption of messages.

        The callback receives (body, metadata) where metadata is a dict with
        delivery_tag, routing_key, content_type, headers, etc., or None.

        Args:
            callback: Function to process each message.
            auto_ack: Auto-acknowledge messages.
            prefetch: Prefetch count for fair dispatch.
        """
        self._ensure_connection()

        def _wrapper(ch: BlockingChannel, method: Any, properties: Any, body: bytes) -> None:
            meta = {
                "delivery_tag": method.delivery_tag,
                "routing_key": method.routing_key,
                "exchange": method.exchange,
                "content_type": properties.content_type if properties else None,
                "content_encoding": properties.content_encoding if properties else None,
                "headers": properties.headers if properties else None,
                "delivery_mode": properties.delivery_mode if properties else None,
                "priority": properties.priority if properties else None,
                "correlation_id": properties.correlation_id if properties else None,
                "reply_to": properties.reply_to if properties else None,
                "message_id": properties.message_id if properties else None,
                "timestamp": properties.timestamp if properties else None,
                "type": properties.type if properties else None,
                "user_id": properties.user_id if properties else None,
                "app_id": properties.app_id if properties else None,
            }
            try:
                callback(body.decode(), meta)
            finally:
                if not auto_ack:
                    ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_qos(prefetch_count=prefetch)
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=_wrapper,
            auto_ack=auto_ack,
        )
        logger.info("Waiting for messages in '%s'. Press CTRL+C to exit.", self.queue_name)
        self._consuming = True
        try:
            self.channel.start_consuming()
        finally:
            self._consuming = False

    def stop_consuming(self) -> None:
        """Stop the blocking consume loop gracefully."""
        if self.channel and self._consuming:
            self.channel.stop_consuming()
            self._consuming = False

    # ──────────────────────────────────────────────
    # NON-BLOCKING GET
    # ──────────────────────────────────────────────

    def get(
        self,
        auto_ack: bool = True,
        include_metadata: bool = False,
    ) -> Optional[Union[str, Dict[str, Any]]]:
        """
        Get a single message from the queue (non-blocking).

        Args:
            auto_ack: Auto-acknowledge the message.
            include_metadata: If True, return dict with body, metadata, delivery_tag.

        Returns:
            Message body as string, or dict with metadata if include_metadata=True,
            or None if queue is empty.
        """
        self._ensure_connection()
        method_frame, header_frame, body = self.channel.basic_get(
            self.queue_name, auto_ack=auto_ack
        )
        if method_frame is None:
            return None
        decoded = body.decode()
        if include_metadata:
            return {
                "body": decoded,
                "delivery_tag": method_frame.delivery_tag,
                "routing_key": method_frame.routing_key,
                "exchange": method_frame.exchange,
                "redelivered": method_frame.redelivered,
                "content_type": header_frame.content_type if header_frame else None,
                "headers": header_frame.headers if header_frame else None,
            }
        return decoded

    def get_all_messages(self, auto_ack: bool = True) -> List[str]:
        """
        Get all messages from the queue (destructive — removes them).

        Args:
            auto_ack: Auto-acknowledge messages.

        Returns:
            List of message bodies as strings.
        """
        messages: List[str] = []
        while True:
            msg = self.get(auto_ack=auto_ack)
            if msg is None:
                break
            messages.append(msg)
        return messages

    # ──────────────────────────────────────────────
    # QUEUE MANAGEMENT
    # ──────────────────────────────────────────────

    def queue_size(self) -> int:
        """Return the number of messages currently in the queue."""
        self._ensure_connection()
        q = self.channel.queue_declare(
            queue=self.queue_name, durable=self.durable, passive=True
        )
        return q.method.message_count

    def consumer_count(self) -> int:
        """Return the number of active consumers on the queue."""
        self._ensure_connection()
        q = self.channel.queue_declare(
            queue=self.queue_name, durable=self.durable, passive=True
        )
        return q.method.consumer_count

    def purge(self) -> int:
        """
        Remove all messages from the queue.

        Returns:
            Number of messages purged.
        """
        self._ensure_connection()
        result = self.channel.queue_purge(self.queue_name)
        return result.method.message_count

    def delete_queue(self, if_unused: bool = False, if_empty: bool = False) -> None:
        """
        Delete the queue.

        Args:
            if_unused: Only delete if no consumers.
            if_empty: Only delete if no messages.
        """
        self._ensure_connection()
        self.channel.queue_delete(self.queue_name, if_unused=if_unused, if_empty=if_empty)

    # ──────────────────────────────────────────────
    # EXCHANGE BINDING
    # ──────────────────────────────────────────────

    def bind_queue(
        self,
        exchange: str,
        routing_key: Optional[str] = None,
        exchange_type: str = "direct",
        exchange_durable: bool = True,
    ) -> None:
        """
        Declare an exchange and bind the queue to it.

        Args:
            exchange: Exchange name.
            routing_key: Routing key (defaults to queue_name if None).
            exchange_type: Exchange type: direct, topic, fanout, headers.
            exchange_durable: Whether the exchange survives broker restarts.
        """
        self._ensure_connection()
        self.channel.exchange_declare(
            exchange=exchange, exchange_type=exchange_type, durable=exchange_durable
        )
        self.channel.queue_bind(
            queue=self.queue_name,
            exchange=exchange,
            routing_key=routing_key or self.queue_name,
        )

    def unbind_queue(self, exchange: str, routing_key: Optional[str] = None) -> None:
        """
        Unbind the queue from an exchange.

        Args:
            exchange: Exchange name.
            routing_key: Routing key (defaults to queue_name if None).
        """
        self._ensure_connection()
        self.channel.queue_unbind(
            queue=self.queue_name,
            exchange=exchange,
            routing_key=routing_key or self.queue_name,
        )

    # ──────────────────────────────────────────────
    # LIFECYCLE
    # ──────────────────────────────────────────────

    def close(self) -> None:
        """Close the channel and connection."""
        self._consuming = False
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
        except Exception:
            pass
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception:
            pass

    def __repr__(self) -> str:
        return (
            f"RabbitMQQueue(queue='{self.queue_name}', "
            f"url='{self.amqp_url.split('@')[-1] if '@' in self.amqp_url else self.amqp_url}')"
        )

    def __enter__(self) -> "RabbitMQQueue":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


# ──────────────────────────────────────────────
# USAGE EXAMPLES
# ──────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    # ── PRODUCE ────────────────────────────────
    print("=" * 60)
    print("RabbitMQQueue - Usage Examples")
    print("=" * 60)

    print("\n--- Basic Produce/Consume ---")
    with RabbitMQQueue(
        queue_name="demo_queue",
        amqp_url="amqp://guest:guest@localhost:5672/%2F",
        durable=False,
    ) as q:
        q.purge()
        q.produce("Hello, World!")
        q.produce({"user": "alice", "action": "login"})
        q.produce_batch(["msg1", "msg2", "msg3"])

        print(f"Queue size: {q.queue_size()}")  # 5

        # Get all messages
        msgs = q.get_all_messages()
        print(f"Got {len(msgs)} messages")

    # ── CONSUME ────────────────────────────────
    print("\n--- Consume (runs briefly) ---")

    def on_message(body: str, meta: Optional[Dict[str, Any]]) -> None:
        print(f"Received: {body}")
        if meta:
            print(f"  delivery_tag: {meta['delivery_tag']}")

    q2 = RabbitMQQueue(
        queue_name="demo_queue",
        amqp_url="amqp://guest:guest@localhost:5672/%2F",
        durable=False,
    )
    q2.purge()
    for i in range(3):
        q2.produce(f"Message {i}")
    import threading
    t = threading.Thread(target=lambda: (sleep(1), q2.stop_consuming()), daemon=True)
    t.start()
    q2.consume(on_message, auto_ack=True)
    q2.close()
    print("\nDone!")
