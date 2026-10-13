# Logger — Production-Ready Structured Logger with SUCCESS Level

A custom colorized logger with a custom `SUCCESS` log level (25), structured JSON output, rotating file handlers, distributed tracing support, static context tags, and automatic redaction of sensitive data (passwords, tokens, API keys, credit cards).

## Installation

```bash
pip install colorlog
```

## Quick Start

```python
from modules.logger import get_logger

# Default: JSON output to console
logger = get_logger(__name__)
logger.info("Server starting on port 8000")
# {"level": "INFO", "logger": "__main__", "message": "Server starting on port 8000", ...}

# Success level (between INFO and WARNING)
logger.success("Database connection established")

# Colored console output (non-JSON)
logger = get_logger(__name__, json_output=False)
logger.info("Request received")
```

## `get_logger()` Parameters

All parameters except `name` are keyword-only.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | *required* | Logger name (typically `__name__`) |
| `show_time` | `bool` | `False` | Include ISO-8601 timestamp in output |
| `json_output` | `bool \| None` | env `LOG_FORMAT` or `True` | JSON lines vs colored text |
| `log_file` | `str \| None` | `None` | Path to rotating log file |
| `max_bytes` | `int \| str` | `10MB` | Max file size before rotation (`"10MB"`, `"500KB"`, `"1GB"`, or raw int) |
| `backup_count` | `int` | `5` | Number of rotated files to keep |
| `trace_id` | `str \| None` | `None` | Correlation ID for distributed tracing (auto-generated if omitted) |
| `context` | `dict \| None` | `None` | Static key-value pairs attached to every log record |
| `redact_sensitive` | `bool` | `True` | Auto-redact passwords, tokens, API keys, credit cards |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `DEBUG` | Set log level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `LOG_FORMAT` | `json` | Output format: `"json"` or `"text"` |

## Features

### Log Levels

| Level | Value | Color | Description |
|-------|-------|-------|-------------|
| `DEBUG` | 10 | Bold white | Detailed debug information |
| `INFO` | 20 | Bold cyan | General informational messages |
| **`SUCCESS`** | **25** | **Bold green** | Successful operation completion |
| `WARNING` | 30 | Bold yellow | Warning about potential issues |
| `ERROR` | 40 | Red | Error that was handled |
| `CRITICAL` | 50 | Bold red | Critical failure requiring immediate attention |

### JSON Output (default)

```python
logger = get_logger(__name__)
logger.info("User logged in", extra={"user_id": "abc123"})
# {"level":"INFO","logger":"myapp","message":"User logged in",
#  "module":"views","function":"login","line":42,
#  "worker_id":"-12345","trace_id":"a1b2c3d4e5f6g7h8"}
```

With `show_time=True`:

```python
logger = get_logger(__name__, show_time=True)
# {"level":"INFO","logger":"myapp","message":"...",
#  "timestamp":"2026-07-18T10:30:00.123456", ...}
```

### Colored Console Output (text mode)

```python
logger = get_logger(__name__, json_output=False)

# With timestamps
logger = get_logger(__name__, json_output=False, show_time=True)
# [2026-07-18 10:30:00] [myapp-12345] INFO  Server starting
```

Each level has a distinct color:
- `SUCCESS` — bold green
- `INFO` — bold cyan
- `DEBUG` — bold white
- `WARNING` — bold yellow
- `ERROR` — red
- `CRITICAL` — bold red

### Distributed Tracing

```python
# Custom trace ID
logger = get_logger(__name__, trace_id="req-abc-123")

# Auto-generated trace ID (16-char hex)
logger = get_logger(__name__)  # trace_id auto-generated
```

The `trace_id` is attached to every log record in both JSON and text output.

### Static Context Tags

```python
logger = get_logger(__name__, context={
    "service": "api",
    "env": "production",
    "region": "us-east-1",
    "version": "2.3.1",
})
logger.info("Processing order #1024")
# JSON includes: {"context": {"service": "api", "env": "production", ...}}
```

### Rotating File Handler

```python
# Log to a file with rotation at 10MB per file, keep 5 backups
logger = get_logger(__name__, log_file="/var/log/app.log")

# Custom rotation size
logger = get_logger(__name__, log_file="/var/log/app.log", max_bytes="100MB", backup_count=10)
logger = get_logger(__name__, log_file="/var/log/app.log", max_bytes="500KB")
logger = get_logger(__name__, log_file="/var/log/app.log", max_bytes=10485760)  # raw bytes
```

File output always uses JSON format with timestamps, regardless of `json_output` setting.

### Sensitive Data Redaction

Enabled by default. Automatically redacts from log messages:

```python
logger = get_logger(__name__)
logger.info("User password=Secret123!")
# Message becomes: "User password=***"

logger.info("Authorization: Bearer eyJhbGciOiJIUzI1NiIs...")
# Message becomes: "Authorization: Bearer ***"

logger.info("Credit card: 4111-1111-1111-1111")
# Message becomes: "Credit card: ****-****-****-****"

# Disable if needed
logger = get_logger(__name__, redact_sensitive=False)
```

**Redacted patterns:**
- `password`, `passwd`, `pwd` followed by `=` or `:` and a value
- `secret`, `token`, `api_key`, `apikey`, `api_secret` followed by `=` or `:` and a value
- `Bearer ` followed by a token
- 16-digit credit card numbers (with or without dashes/spaces)

### Worker ID

Auto-attached to every log record:

```python
# Reads UVICORN_WORKER env var if set, otherwise uses PID
# Output: [myapp-12345] or [myapp-uwsgi-1]
```

Useful for identifying which worker/process produced a log in multi-worker deployments.

## Use Cases

| Use Case | Config |
|----------|--------|
| Local development | `get_logger(__name__, json_output=False)` |
| Production JSON logs | `get_logger(__name__)` (default) |
| Distributed tracing | `get_logger(__name__, trace_id="req-xxx")` |
| Microservice logging | `get_logger(__name__, context={"service": "orders"})` |
| File audit trail | `get_logger(__name__, log_file="/var/log/audit.log")` |
| CI/CD pipelines | `LOG_FORMAT=text LOG_LEVEL=INFO` |

## Production Notes

- Logger is **idempotent** — calling `get_logger` with the same `name` returns the same instance with handlers added only once
- `json_output` respects the `LOG_FORMAT` env var — set it at deployment level
- `LOG_LEVEL` env var controls verbosity without code changes
- Sensitive data redaction is on by default — catch secrets before they reach your log aggregator
- `RotatingFileHandler` prevents disk from filling up — configurable `max_bytes` and `backup_count`
- `max_bytes` accepts human-readable strings (`"10MB"`, `"500KB"`, `"1GB"`, `"2TB"`) or raw integers
- Full type hints on all public functions
