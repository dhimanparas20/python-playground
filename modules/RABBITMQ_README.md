# RabbitMQQueue — Production-Ready RabbitMQ Utility

A comprehensive Python class for RabbitMQ queue operations. Supports produce, consume, batch messaging, JSON messages, exchange binding, queue management, auto-reconnection, and context manager.
- Supports : Supports CloudAMQP or any AMQP broker

## Installation

```bash
pip install pika
```

## Quick Start

```python
from modules.RabbitMQQueue_play import RabbitMQQueue

# Connect via individual params
q = RabbitMQQueue(
    queue_name="tasks",
    username="guest", password="guest",
    host="localhost", port=5672,
    vhost="/",
)

# Connect via full AMQP URL
q = RabbitMQQueue(
    queue_name="tasks",
    amqp_url="amqp://guest:guest@localhost:5672/%2F",
)
```

## Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `queue_name` | `str` | *required* | Queue name |
| `username` | `Optional[str]` | `None` | RabbitMQ username (omit if using `amqp_url`) |
| `password` | `Optional[str]` | `None` | RabbitMQ password (omit if using `amqp_url`) |
| `host` | `Optional[str]` | `None` | RabbitMQ host (omit if using `amqp_url`) |
| `port` | `int` | `5672` | AMQP port (5671 for TLS) |
| `vhost` | `str` | `"/"` | Virtual host |
| `amqp_url` | `Optional[str]` | `None` | Full AMQP URL (overrides individual params) |
| `durable` | `bool` | `True` | Queue survives broker restarts |
| `use_ssl` | `bool` | `False` | Use amqps:// scheme |
| `heartbeat` | `int` | `60` | Heartbeat interval (0 to disable) |
| `connection_attempts` | `int` | `3` | Max connection retries |
| `retry_delay` | `float` | `5.0` | Seconds between retries |

## Features

### Produce Messages

```python
# Plain string
q.produce("Hello, World!")

# JSON dict/list (auto-serialized, content-type: application/json)
q.produce({"user": "alice", "action": "login"})
q.produce([1, 2, 3, "done"])

# Bytes
q.produce(b"raw binary data")

# Custom headers
q.produce("important", headers={"priority": "high", "source": "api"})

# Non-persistent (faster, lost on broker restart)
q.produce("transient", persistent=False)
```

### Batch Produce

```python
count = q.produce_batch(["msg1", "msg2", "msg3", {"key": "value"}])
# count = 4
```

### Blocking Consume

```python
def handle_message(body: str, metadata: Optional[dict]):
    print(f"Received: {body}")
    if metadata:
        print(f"  delivery_tag: {metadata['delivery_tag']}")
        print(f"  content_type: {metadata['content_type']}")

q.consume(handle_message, auto_ack=False, prefetch=1)
# Blocks until q.stop_consuming() is called
```

The callback receives `(body, metadata)` where metadata contains:

| Field | Description |
|-------|-------------|
| `delivery_tag` | Message delivery tag (for nack/reject) |
| `routing_key` | Routing key used |
| `exchange` | Exchange the message came from |
| `content_type` | MIME type |
| `headers` | Custom headers dict |
| `delivery_mode` | 1 = non-persistent, 2 = persistent |
| `correlation_id` | Correlation ID |
| `reply_to` | Reply-to queue |
| `message_id` | Message ID |
| `timestamp` | Publication timestamp |
| `type` | Message type |
| `app_id` | Application ID |

### Stop Consuming

```python
# From another thread
q.stop_consuming()
```

### Non-Blocking Get

```python
# Simple get
msg = q.get()
if msg:
    print(msg)

# Get with metadata
result = q.get(include_metadata=True)
if result:
    print(result["body"])
    print(result["delivery_tag"])

# Get all (drains the queue)
all_msgs = q.get_all_messages()
```

### Queue Management

```python
# Message count
size = q.queue_size()

# Consumer count
consumers = q.consumer_count()

# Purge all messages
purged = q.purge()

# Delete the queue
q.delete_queue()
q.delete_queue(if_unused=True, if_empty=True)
```

### Exchange Binding

```python
# Bind queue to an exchange
q.bind_queue(exchange="my_exchange", routing_key="my.routing.key")
q.bind_queue(exchange="logs", exchange_type="fanout")  # routing_key ignored

# Unbind
q.unbind_queue(exchange="my_exchange", routing_key="my.routing.key")
```

### Connection Management

```python
# Check connection status
if q.is_connected:
    print("Connected!")

# Reconnection is automatic — _ensure_connection is called before every
# produce/get/purge/queue_size operation. If the connection dropped,
# it reconnects with the configured heartbeat, connection_attempts,
# and retry_delay.
```

### Context Manager

```python
with RabbitMQQueue(queue_name="tasks", amqp_url="amqp://...") as q:
    q.produce("hello")
    # connection auto-closed on exit
```

## Use Cases

| Use Case | Config |
|----------|--------|
| Task queue | `durable=True`, `persistent=True` |
| Transient events | `durable=False`, `produce(msg, persistent=False)` |
| JSON RPC | `produce({"method":"add","params":[1,2]})` |
| Fanout broadcast | `bind_queue(exchange="broadcast", exchange_type="fanout")` |
| Priority jobs | `produce(msg, headers={"priority": "high"})` |
| CloudAMQP | `use_ssl=True`, `port=5671` |

## Production Notes

- `_ensure_connection()` auto-reconnects if the connection is closed — no need to reconnect manually
- `produce()` supports `str`, `bytes`, `dict`, `list` — dict/list auto-serialized to JSON
- `consume()` callback receives `(body, metadata)` — full message context
- `stop_consuming()` gracefully exits the blocking consume loop
- `close()` is safe to call multiple times — guards against already-closed connections
- All methods have full **type hints** and **docstrings**
- Context manager ensures connections are closed properly
- Heartbeat prevents silent connection drops on idle links
- Connection attempts with retry delay handles temporary broker unavailability
