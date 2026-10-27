# Python Playground

**Version:** 0.1.0 | **Python:** 3.14 | **Package Manager:** uv

A universal sandbox and learning project exploring Python across security, data generation, concurrency, infrastructure automation, and language fundamentals. All modules are production-quality with dedicated READMEs.

---

## Quick Start

```bash
git clone <repo-url>
cd python-playground
uv sync          # install all dependencies
python main.py   # prints "Hello from python-playground!"
```

### Interactive Practice with IPython

```bash
uv run ipython
```

Opens an interactive IPython shell with all dependencies loaded. Import any module and start practicing:

```python
In [1]: from modules.logger import get_logger
In [2]: log = get_logger("test")
In [3]: log.success("it works!")   # custom SUCCESS level
In [4]: log.info("hello %s", "world")

In [5]: from faker import Faker
In [6]: fake = Faker()
In [7]: fake.name(), fake.email()

In [8]: from modules.datetime_parser import convert_datetime, timestamp_to_string
In [9]: convert_datetime("2024-01-01", from_tz="UTC", to_tz="Asia/Kolkata")

In [10]: from modules.Redis.redis_core_util import RedisHashUtil
In [11]: rh = RedisHashUtil()
In [12]: rh.set_hash("myhash", {"name": "test", "value": 123})

In [13]: from modules.MongoDB.mongo_util import MongoUtil
In [14]: mu = MongoUtil()
In [15]: mu.create("users", {"name": "Alice", "email": "alice@example.com"})

In [16]: from modules.s3.s3_util import S3Util
In [17]: s3 = S3Util()
In [18]: s3.list_files("my-bucket")

In [19]: from modules.qdrant import QdrantUtil, get_embeddings
In [20]: qd = QdrantUtil()                       # or embeddings=get_embeddings("openai")
In [21]: qd.list_collections()

In [22]: from modules.emqx import EMQXClient
In [23]: client = EMQXClient()
In [24]: client.get_status()

In [25]: from modules.rabbitmq import RabbitMQQueue
In [26]: q = RabbitMQQueue("my_queue")
In [27]: q.produce({"key": "value"})

In [28]: from modules.function_timer import timeit
In [29]: @timeit
    ...: def slow_add(a, b):
    ...:     import time; time.sleep(1)
    ...:     return a + b
In [30]: slow_add(1, 2)

In [31]: from encrypt_and_decrypt_data import hash_password, validate_password
In [32]: h = hash_password("mysecret123")
In [33]: validate_password("mysecret123", h)

In [34]: from faker_fake_data import make_fake_people
In [35]: df = make_fake_people(5)    # returns DataFrame

In [36]: import multiprocessing_and_multiprocessing.learn_threding as lt
In [37]: lt.main()                   # run threading demo

In [38]: import multiprocessing_and_multiprocessing.cpu_bound_comp as cpu
In [39]: cpu.main()                  # run CPU-bound comparison

In [40]: from code_practice.map_func import process_students
In [41]: process_students()          # run map/filter/lambda demo
```

Tab-completion and `?` work too — e.g. `MongoUtil?` shows full docstring.

---

## Directory Structure

```
python-playground/
│
├── main.py                                # Entry point
├── encrypt_and_decrypt_data.py            # Password hashing + data encryption demo
├── faker_fake_data.py                     # Generates 50K fake people records
├── ssh_key_gen.py                         # Ed25519 SSH key pair generator
├── pyproject.toml                         # Dependencies & project config
├── uv.lock                                # Lockfile
├── .python-version                        # 3.14
│
├── code_practice/                         # Python fundamentals practice
│   ├── decorators.py
│   ├── list_single_element_finder.py
│   └── map_func.py
│
├── multiprocessing_and_multiprocessing/   # Concurrency & parallelism learning
│   ├── README.md                          # Full guide: threading vs multiprocessing vs asyncio
│   ├── learn_threding.py
│   ├── learn_multiprocessing.py
│   ├── cpu_bound_comp.py
│   ├── io_bound_comp.py
│   └── image_downloader.py
│
└── modules/                               # Production-grade utility modules
    ├── function_timer_decorator.py        # Execution time decorators
    ├── wa_util.py                         # WAHA WhatsApp HTTP API client
    ├── logger.py                          # Structured logger (JSON, colors, redaction, tracing)
    ├── EMXQ_MQTT.py                       # EMQX v5 REST API client
    ├── RabbitMQQueue_play.py              # RabbitMQ queue handler
    ├── datetime_parser/                   # Datetime parsing with 100+ timezone aliases
    ├── MongoDB/                           # MongoDB CRUD utility
    ├── Redis/                             # Redis hash & cache utility suite
    ├── s3/                                # AWS S3 utility
    └── qdrant/                            # Qdrant vector DB utility
```

---

## Feature Overview

### Security

| File | What It Does | Libraries |
|------|-------------|-----------|
| `encrypt_and_decrypt_data.py` | bcrypt password hashing + Fernet symmetric encryption | `bcrypt`, `cryptography` |
| `ssh_key_gen.py` | Generate Ed25519 SSH key pairs, append to `authorized_keys` | `cryptography`, `subprocess` |

### Data Generation

| File | What It Does | Libraries |
|------|-------------|-----------|
| `faker_fake_data.py` | Generate 50K fake records (name, email, address, phone, etc.) with CSV/JSON export | `faker` |

### Concurrency & Parallelism

All files under `multiprocessing_and_multiprocessing/`. See its [README](multiprocessing_and_multiprocessing/README.md) for the complete guide.

| File | Topic |
|------|-------|
| `learn_threding.py` | Threading basics: `Thread`, `start`, `join`, `active_count` |
| `learn_multiprocessing.py` | Multiprocessing basics: `Process`, `start`, `join`, `pid`, `active_children` |
| `cpu_bound_comp.py` | 6 approaches compared for CPU-bound math work (sequential, threading, multiprocessing, Pool, asyncio, asyncio+ProcessPool) |
| `io_bound_comp.py` | 5 approaches compared for I/O-bound sleep work (sequential, threading, multiprocessing, asyncio native, asyncio+ThreadPool) |
| `image_downloader.py` | Downloads 10 images from picsum.photos — 5 strategies with `tqdm` progress bars |

### Code Practice

| File | What It Teaches |
|------|----------------|
| `code_practice/decorators.py` | Creating and applying decorators |
| `code_practice/list_single_element_finder.py` | Finding elements that appear exactly once in a list |
| `code_practice/map_func.py` | `map()`, `lambda`, `filter()`, `zip()` with student score data |

### Utility Modules

Each module has its own comprehensive README with full API docs.

| Module | README | What It Does |
|--------|--------|-------------|
| `function_timer_decorator.py` | — | `@timeit` and `@timeit_stats` decorators with color-coded output |
| `wa_util.py` | [modules/WAHA_README.md](modules/WAHA_README.md) | `WhatsAppUtil` + `WAHAWebhook`: WAHA REST client for sessions, chat send/receive, media, webhooks |
| `logger.py` | [modules/LOGGER_README.md](modules/LOGGER_README.md) | Structured logger: SUCCESS level, JSON/text, rotating files, redaction, distributed tracing, context tags |
| `EMXQ_MQTT.py` | [modules/EMQX_README.md](modules/EMQX_README.md) | EMQX v5 REST API client: users, clients, subscriptions, topics, messages, API keys, rules, bridges, listeners, nodes, auth |
| `RabbitMQQueue_play.py` | [modules/RABBITMQ_README.md](modules/RABBITMQ_README.md) | RabbitMQ queue handler: produce/consume/batch, exchange binding, auto-reconnect, context manager |
| `datetime_parser/` | [modules/datetime_parser/DATETIME_PARSER_README.md](modules/datetime_parser/DATETIME_PARSER_README.md) | Datetime parsing/conversion with 100+ timezone aliases: `convert_datetime`, `timestamp_to_string`, `string_to_timestamp`, `format_iso`, `resolve_timezone`, `list_supported_timezones` |
| `MongoDB/mongo_util.py` | [modules/MongoDB/MONGO_UTIL_README.md](modules/MongoDB/MONGO_UTIL_README.md) | Full MongoDB utility: CRUD, bulk ops, TTL, indexes, search, JSON/CSV import/export, dedup, secure hashing, admin operations |
| `Redis/` | [modules/Redis/REDIS_UTIL_README.md](modules/Redis/REDIS_UTIL_README.md) | `RedisHashUtil` (hash storage, CRUD, bulk, TTL, locks, indexes, async) + `RedisStringUtil` (string KV store, memoize, async) + `RedisCache` (cache layer on string util, cache-aside, TTL, async) |
| `s3/` | [modules/s3/S3_UTIL_README.md](modules/s3/S3_UTIL_README.md) | `S3Util`: upload, download, delete, list, fetch, metadata, existence checks. Backward-compatible wrappers for all operations |
| `qdrant/` | [modules/qdrant/QDRANT_UTIL_README.md](modules/qdrant/QDRANT_UTIL_README.md) | `QdrantUtil` (collections/search/upsert) + `DocumentUtil` (load/split/embed PDF, MD, HTML, CSV, TXT, URL, dirs) |
| `llm_utils/` | [modules/llm_utils/LLM-UTIL-README.md](modules/llm_utils/LLM-UTIL-README.md) | LangChain chat models + agents across OpenAI, Google, OpenRouter, Groq, Vercel AI Gateway, OpenCode (Zen + Go); `register_provider` for any other OpenAI-compatible provider; MCP / file tools; token + cost summaries |

---

## Libraries & Dependencies

All managed via `pyproject.toml` — install with `uv sync`.

| Library | Version | Used In |
|---------|---------|---------|
| `bcrypt` | >=4.1.0 | Password hashing (encrypt_and_decrypt_data, mongo_util, redis) |
| `boto3` | >=1.34.0 | AWS S3 operations |
| `botocore` | >=1.34.0 | AWS S3 operations (internal) |
| `colorlog` | >=6.8.2 | Colored log output |
| `cryptography` | >=42.0.5 | Fernet encryption, SSH key generation |
| `dateparser` | >=1.2.0 | Flexible datetime string parsing |
| `faker` | >=25.0.0 | Fake data generation |
| `ipython` | >=9.15.0 | Interactive Python (dev use) |
| `passlib` | >=1.7.4 | PBKDF2 hashing (legacy redis) |
| `pika` | >=1.3.2 | RabbitMQ AMQP client |
| `pydantic` | >=2.10.0 | WAHA client models (`wa_util.py`) |
| `pymongo` | >=4.10.0 | MongoDB driver |
| `python-dotenv` | >=1.0.1 | Environment variable loading |
| `qdrant-client` | >=1.19.1 | Qdrant vector DB client |
| `redis` | >=5.0.0 | Redis driver (sync + async) |
| `requests` | >=2.32.0 | HTTP client (EMQX, image downloader) |
| `tqdm` | >=4.66.2 | Progress bars (image downloader) |

---

## Use Cases

| Scenario | Module/File |
|----------|-------------|
| Hash & verify passwords | `encrypt_and_decrypt_data.py`, `MongoDB/mongo_util.py`, `Redis/` |
| Encrypt/decrypt sensitive data | `encrypt_and_decrypt_data.py` (Fernet) |
| Generate SSH key pairs | `ssh_key_gen.py` |
| Generate fake datasets | `faker_fake_data.py` |
| Learn threading basics | `multiprocessing_and_multiprocessing/learn_threding.py` |
| Benchmark CPU-bound parallel code | `multiprocessing_and_multiprocessing/cpu_bound_comp.py` |
| Benchmark I/O-bound concurrent code | `multiprocessing_and_multiprocessing/io_bound_comp.py` |
| Download files with progress bars | `multiprocessing_and_multiprocessing/image_downloader.py` |
| Manage EMQX MQTT broker (REST API) | `modules/EMXQ_MQTT.py` |
| Send/receive WhatsApp via self-hosted WAHA | `modules/wa_util.py` |
| Produce/consume RabbitMQ queues | `modules/RabbitMQQueue_play.py` |
| CRUD on MongoDB (with bulk, TTL, indexes) | `modules/MongoDB/mongo_util.py` |
| Flat Redis storage + caching (hash/string/cache) | `modules/Redis/redis_core_util.py` |
| Upload/download files to S3 | `modules/s3/s3_util.py` |
| Vector search / Qdrant CRUD | `modules/qdrant/qdrant_util.py` |
| Structured JSON logging with tracing | `modules/logger.py` |
| Parse/convert datetimes across 100+ timezones | `modules/datetime_parser/` |
| Time function execution with decorators | `modules/function_timer_decorator.py` |
| Practice decorators, map, filter, list logic | `code_practice/` |

---

## Related Docs

- [Multiprocessing & Threading Guide](multiprocessing_and_multiprocessing/README.md)
- [WAHA / WhatsApp Module](modules/WAHA_README.md)
- [Logger Module](modules/LOGGER_README.md)
- [EMQX Client Module](modules/EMQX_README.md)
- [RabbitMQ Queue Module](modules/RABBITMQ_README.md)
- [Datetime Parser Module](modules/datetime_parser/DATETIME_PARSER_README.md)
- [MongoDB Utility Module](modules/MongoDB/MONGO_UTIL_README.md)
- [Redis Utility Module](modules/Redis/REDIS_UTIL_README.md)
- [S3 Utility Module](modules/s3/S3_UTIL_README.md)
- [Qdrant Utility Module](modules/qdrant/QDRANT_UTIL_README.md)
- [LLM Utility Module](modules/llm_utils/LLM-UTIL-README.md)
