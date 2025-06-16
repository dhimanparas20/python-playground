import pika
from typing import Callable, Optional, Any
from time import sleep

class RabbitMQQueue:
    """
    Production-ready RabbitMQ queue handler for CloudAMQP or any AMQP broker.
    """

    def __init__(
        self,
        queue_name: str,
        username: str,
        password: str,
        host: str,
        port: int = 5672,
        vhost: str = '/',
        durable: bool = True,
        use_ssl: bool = True
    ) -> None:
        """
        Initialize the queue handler and connect to the broker.

        Args:
            queue_name (str): Name of the queue.
            username (str): RabbitMQ username.
            password (str): RabbitMQ password.
            host (str): RabbitMQ host.
            port (int): RabbitMQ port (5672 or 5671 for TLS).
            vhost (str): RabbitMQ virtual host.
            durable (bool): Whether the queue should survive broker restarts.
            use_ssl (bool): Use SSL/TLS (amqps) or not (amqp).
        """
        self.queue_name = queue_name
        self.durable = durable
        protocol = "amqps" if use_ssl else "amqp"
        self.amqp_url = (
            f"{protocol}://{username}:{password}@{host}:{port}/{vhost.lstrip('/')}"
        )
        self.connection = None
        self.channel = None
        self._connect()

    def _connect(self) -> None:
        """Establish connection and channel, declare the queue."""
        params = pika.URLParameters(self.amqp_url)
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=self.durable)

    def produce(self, message: str, persistent: bool = True) -> None:
        """
        Publish a message to the queue.

        Args:
            message (str): The message to send.
            persistent (bool): Make message survive broker restarts.
        """
        props = pika.BasicProperties(delivery_mode=2) if persistent else None
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body=message,
            properties=props
        )

    def consume(self, callback: Callable[[str], Any], auto_ack: bool = False, prefetch: int = 1) -> None:
        """
        Start consuming messages from the queue.

        Args:
            callback (Callable[[str], Any]): Function to process each message body (as string).
            auto_ack (bool): Whether to automatically acknowledge messages.
            prefetch (int): Number of messages to prefetch (for fair dispatch).
        """
        def _internal_callback(ch, method, properties, body):
            callback(body.decode())
            if not auto_ack:
                ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_qos(prefetch_count=prefetch)
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=_internal_callback,
            auto_ack=auto_ack
        )
        print(f"[*] Waiting for messages in '{self.queue_name}'. To exit press CTRL+C")
        self.channel.start_consuming()

    def get(self, auto_ack: bool = True) -> Optional[str]:
        """
        Get a single message from the queue (non-blocking).

        Args:
            auto_ack (bool): Whether to automatically acknowledge the message.

        Returns:
            Optional[str]: The message body, or None if queue is empty.
        """
        method_frame, header_frame, body = self.channel.basic_get(self.queue_name, auto_ack=auto_ack)
        if method_frame:
            return body.decode()
        return None

    def purge(self) -> None:
        """Remove all messages from the queue."""
        self.channel.queue_purge(self.queue_name)

    def close(self) -> None:
        """Close the channel and connection."""
        if self.channel:
            self.channel.close()
        if self.connection:
            self.connection.close()

    def queue_size(self) -> int:
        """Return the number of messages in the queue."""
        q = self.channel.queue_declare(queue=self.queue_name, durable=self.durable, passive=True)
        return q.method.message_count

    def get_all_messages(self, auto_ack: bool = True) -> list:
        """
        Get all messages from the queue as a list (destructive: removes them from the queue).
        """
        messages = []
        while True:
            msg = self.get(auto_ack=auto_ack)
            if msg is None:
                break
            messages.append(msg)
        return messages

# --- Example Usage ---

if __name__ == "__main__":
    # Fill in your actual credentials here
    queue = RabbitMQQueue(
        queue_name="test_queue",
        username="hxssuxdc",
        password="7f5Hn77d84IiiOFVQeKE4ZfHYKRpYBmj",
        host="puffin.rmq2.cloudamqp.com",
        port=5671,  # Use 5671 for TLS, 5672 for non-TLS
        vhost="hxssuxdc",
        durable=True,
        use_ssl=True
    )

    # Producer example
    queue.purge()
    for i in range(3):
        queue.produce(f"Message {i}")
    sleep(1)
    print("Queue size after produce:", queue.queue_size())  # Should show 3 if no consumer is running
    queue.close()

    # Consumer example
    def process_message(msg):
        print(f"Consumed: {msg}")
        # exit() if queue.queue_size()==0 else None

    queue = RabbitMQQueue(
        queue_name="test_queue",
        username="hxssuxdc",
        password="7f5Hn77d84IiiOFVQeKE4ZfHYKRpYBmj",
        host="puffin.rmq2.cloudamqp.com",
        port=5671,
        vhost="hxssuxdc",
        durable=True,
        use_ssl=True
    )
    queue.consume(process_message)  # This will block and consume messages
    queue.close()
