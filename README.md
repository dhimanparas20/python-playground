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
    ├── logger.py                          # Structured logger (JSON, colors, redaction, tracing)
    ├── EMXQ_MQTT.py                       # EMQX v5 REST API client
    ├── RabbitMQQueue_play.py              # RabbitMQ queue handler
    ├── datetime_parser/                   # Datetime parsing with 100+ timezone aliases
    ├── MongoDB/                           # MongoDB CRUD utility
    ├── Redis/                             # Redis hash & cache utility suite
    └── s3/                                # AWS S3 utility
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
| `logger.py` | [modules/LOGGER_README.md](modules/LOGGER_README.md) | Structured logger: SUCCESS level, JSON/text, rotating files, redaction, distributed tracing, context tags |
| `EMXQ_MQTT.py` | [modules/EMQX_README.md](modules/EMQX_README.md) | EMQX v5 REST API client: users, clients, subscriptions, topics, messages, API keys, rules, bridges, listeners, nodes, auth |
| `RabbitMQQueue_play.py` | [modules/RABBITMQ_README.md](modules/RABBITMQ_README.md) | RabbitMQ queue handler: produce/consume/batch, exchange binding, auto-reconnect, context manager |
| `datetime_parser/` | [modules/datetime_parser/DATETIME_PARSER_README.md](modules/datetime_parser/DATETIME_PARSER_README.md) | Datetime parsing/conversion with 100+ timezone aliases: `convert_datetime`, `timestamp_to_string`, `string_to_timestamp`, `format_iso`, `resolve_timezone`, `list_supported_timezones` |
| `MongoDB/mongo_util.py` | [modules/MongoDB/MONGO_UTIL_README.md](modules/MongoDB/MONGO_UTIL_README.md) | Full MongoDB utility: CRUD, bulk ops, TTL, indexes, search, JSON/CSV import/export, dedup, secure hashing, admin operations |
| `Redis/` | [modules/Redis/REDIS_UTIL_README.md](modules/Redis/REDIS_UTIL_README.md) | `RedisHashUtil` (hash storage, CRUD, bulk, TTL, locks, indexes, async) + `RedisCacheManager` (string cache, cache-aside, `@cache_result` decorator, async) |
| `s3/` | [modules/s3/S3_UTIL_README.md](modules/s3/S3_UTIL_README.md) | `S3Util`: upload, download, delete, list, fetch, metadata, existence checks. Backward-compatible wrappers for all operations |

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
| `pymongo` | >=4.10.0 | MongoDB driver |
| `python-dotenv` | >=1.0.1 | Environment variable loading |
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
| Produce/consume RabbitMQ queues | `modules/RabbitMQQueue_play.py` |
| CRUD on MongoDB (with bulk, TTL, indexes) | `modules/MongoDB/mongo_util.py` |
| Hash-based Redis storage + string caching | `modules/Redis/redis_core_util.py` |
| Upload/download files to S3 | `modules/s3/s3_util.py` |
| Structured JSON logging with tracing | `modules/logger.py` |
| Parse/convert datetimes across 100+ timezones | `modules/datetime_parser/` |
| Time function execution with decorators | `modules/function_timer_decorator.py` |
| Practice decorators, map, filter, list logic | `code_practice/` |

---

## Related Docs

- [Multiprocessing & Threading Guide](multiprocessing_and_multiprocessing/README.md)
- [Logger Module](modules/LOGGER_README.md)
- [EMQX Client Module](modules/EMQX_README.md)
- [RabbitMQ Queue Module](modules/RABBITMQ_README.md)
- [Datetime Parser Module](modules/datetime_parser/DATETIME_PARSER_README.md)
- [MongoDB Utility Module](modules/MongoDB/MONGO_UTIL_README.md)
- [Redis Utility Module](modules/Redis/REDIS_UTIL_README.md)
- [S3 Utility Module](modules/s3/S3_UTIL_README.md)
