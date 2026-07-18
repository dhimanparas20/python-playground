# EMQXClient — Production-Ready EMQX v5 REST API Client

A comprehensive Python client for the EMQX v5 MQTT broker REST API. Provides complete management of users, clients, subscriptions, topics, messages, API keys, rules, data bridges, listeners, nodes, alerts, authentication, and authorization.

## Installation

```bash
pip install requests
```

## Quick Start

```python
from modules.EMXQ_MQTT import EMQXClient

client = EMQXClient(
    base_url="http://localhost:18083",
    api_key="your-api-key",
    api_secret="your-api-secret",
    max_retries=2,   # auto-retry on transient failures
)
```

## Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `base_url` | `str` | *required* | EMQX API base URL (e.g., `http://localhost:18083`) |
| `api_key` | `str` | *required* | API key for HTTP Basic auth |
| `api_secret` | `str` | *required* | API secret for HTTP Basic auth |
| `timeout` | `int` | `10` | Request timeout in seconds |
| `max_retries` | `int` | `0` | Number of retries on connection errors/429 rate limits |
| `retry_delay` | `float` | `1.0` | Base seconds between retries (exponential backoff) |

## Features

### Broker Status & Stats

```python
# Broker status (running/stopped)
status = client.get_status()

# Cluster nodes
nodes = client.get_nodes()
node = client.get_node("emqx@127.0.0.1")

# License info
license = client.get_license()

# Broker statistics (connections, topics, subscriptions, etc.)
stats = client.get_stats()
```

### User Management (Built-in Database)

```python
# List all users
users = client.list_users()

# CRUD
client.create_user("johndoe", "securepass123", is_superuser=False)
user = client.get_user("johndoe")
client.update_user("johndoe", password="newpass456")
client.update_user("johndoe", is_superuser=True)
client.delete_user("johndoe")
```

### Client Management

```python
# List all connected clients (auto-paginated)
clients = client.list_clients()

# Get client details
client_info = client.get_client("my-client-id")

# Disconnect a client
client.disconnect_client("bad-client")

# Ban a client ID
client.ban_client("spammer", reason="violating terms", until=1780000000)

# List / unban
banned = client.list_banned_clients()
client.unban_client("spammer")
```

### Subscription Management

```python
# List all subscriptions (auto-paginated)
subs = client.list_subscriptions()

# Subscriptions for a specific client
subs = client.list_client_subscriptions("client-123")

# Get a specific subscription
sub = client.get_subscription("sensor/temp", "client-123")
```

### Topic Management

```python
# List all topics with subscriptions (auto-paginated)
topics = client.list_topics()

# Topic metrics (message rates, counts)
metrics = client.get_topic_metrics("sensor/temp")

# Subscriptions for a specific topic
subs = client.get_topic_subscriptions("sensor/temp")

# Topic aliases
aliases = client.list_topic_alias()
```

### Message Publishing

```python
# Plain text
client.publish_message("notifications", "Hello, World!")

# JSON payload (auto-serialized)
client.publish_message("sensor/data", {"temperature": 23.5, "humidity": 60}, qos=1)

# With MQTT5 properties
client.publish_message("alerts", "system down",
    qos=2, retain=True,
    properties={"message_expiry_interval": 3600}
)

# Specify content type and encoding
client.publish_message("events", "user_login", content_type="json")
```

### API Key Management

```python
# List all API keys
keys = client.list_api_keys()

# Create a scoped API key
client.create_api_key(
    description="read-only monitoring",
    permissions=["mqtt:subscribe", "mqtt:publish"],
    expire_at=1800000000,
)

# Delete
client.delete_api_key("key-id-123")
```

### Listener Management

```python
# List all listeners
listeners = client.list_listeners()

# Get specific listener
listener = client.get_listener("mqtt:tcp:default")
```

### Rule Engine

```python
# List all rules
rules = client.list_rules()

# Create a rule — republish sensor data above threshold
client.create_rule(
    rawsql="SELECT * FROM \"sensor/temp\" WHERE payload.temperature > 40",
    description="High temperature alert",
    actions=[{
        "function": "republish",
        "args": {
            "topic": "alerts/high_temp",
            "payload": "High temperature detected: ${payload.temperature}"
        }
    }],
)

# Get / update / delete
rule = client.get_rule("rule-id-123")
client.update_rule("rule-id-123", enable=False)
client.delete_rule("rule-id-123")
```

### Data Bridges

```python
# List all bridges
bridges = client.list_bridges()

# Create a bridge to Kafka
client.create_bridge(
    name="kafka_bridge",
    bridge_type="kafka",
    config={
        "bootstrap_hosts": "localhost:9092",
        "topic": "emqx_events",
        "authentication": {"mechanism": "plain", "username": "...", "password": "..."},
    },
)

# Get / update / delete
bridge = client.get_bridge("kafka_bridge")
client.update_bridge("kafka_bridge", enable=False)
client.delete_bridge("kafka_bridge")
```

### Alerts

```python
# List all alerts
alerts = client.list_alerts()

# Deactivate an alert
client.deactivate_alert("high_memory_usage")
```

### Authentication & Authorization

```python
# List authentication backends
authenticators = client.list_authenticators()

# List authorization (ACL) sources
acl_sources = client.list_authorization_sources()
```

### Pagination

All `list_*` methods that return multiple items use auto-pagination:

```python
# List all clients across all pages
all_clients = client.list_clients(page_size=50)
# Internally calls the API multiple times with page=1, page=2, ...
# until all results are collected
```

### Retry Logic

When `max_retries > 0`, the client retries on:
- **Connection errors** (network timeout, DNS failure) — with exponential backoff
- **HTTP 429** (rate limit) — respects the `Retry-After` header

```python
client = EMQXClient(base_url="...", api_key="...", api_secret="...", max_retries=3)
```

### Context Manager

```python
with EMQXClient(base_url="...", api_key="...", api_secret="...") as client:
    print(client.get_status())
    print(len(client.list_clients()))
    # session auto-closed on exit
```

## Query Parameters

Pagination-aware methods accept optional `**kwargs` for additional query parameters:

```python
# All list_* methods forward kwargs as query params
clients = client._list_all("/api/v5/clients", params={"node": "emqx@node1", "limit": 50})
```

## Error Handling

All API errors raise `EMQXAPIError` with:

```python
try:
    client.get_user("nonexistent")
except EMQXAPIError as e:
    print(e)             # "HTTP 404: ..."
    print(e.status_code) # 404
    print(e.response)    # raw requests.Response object
```

## Use Cases

| Use Case | Key Methods |
|----------|-------------|
| MQTT user management | `create_user`, `update_user`, `delete_user` |
| Monitor connected clients | `list_clients`, `get_client` |
| Ban abusive clients | `ban_client`, `list_banned_clients`, `unban_client` |
| Topic stats | `list_topics`, `get_topic_metrics` |
| Publish via REST | `publish_message` (MQTT publish over HTTP) |
| Data integration | `create_bridge`, `list_bridges` (Kafka, MySQL, HTTP, etc.) |
| ETL / filtering | `create_rule`, `list_rules` (SQL-like rule engine) |
| Security audit | `list_api_keys`, `list_authenticators`, `list_authorization_sources` |
| Cluster health | `get_status`, `get_nodes`, `get_license`, `get_stats` |

## Production Notes

- `list_clients`, `list_subscriptions`, `list_topics` auto-paginate — no manual page tracking
- `_request` retries on connection errors and HTTP 429 with exponential backoff
- `EMQXAPIError` includes `status_code` and raw `response` for detailed error handling
- All methods have full **type hints** and **docstrings**
- Context manager ensures HTTP sessions are closed properly
- Uses a single `requests.Session` — connection reuse for performance
- Supports EMQX v5 API endpoints (tested on 5.x)
