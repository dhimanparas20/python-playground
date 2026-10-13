# WhatsAppUtil — WAHA (WhatsApp HTTP API) Client

Python client for [WAHA](https://waha.devlike.pro/docs/overview/introduction/) — self-hosted WhatsApp REST API. Covers session lifecycle, outbound chat (text, media, polls, locations, contacts), chat history, contacts, and webhook verification helpers.

**Upstream:** [devlikeapro/waha](https://github.com/devlikeapro/waha)

## Installation

```bash
pip install requests pydantic
# or from repo root
uv sync
```

Run WAHA with this repo’s compose file (API on host port **3001**):

```bash
docker compose -f composefiles/waha-compose.yml up -d
```

## Quick Start

```python
from dotenv import load_dotenv
load_dotenv(".env.waha")

from modules.wa_util import WhatsAppUtil, WAHAWebhook, WhatsAppClientConfig

# From env (WAHA_API_URL, WAHA_API_KEY, WAHA_SESSION, WAHA_DEFAULT_COUNTRY_CODE)
wa = WhatsAppUtil()

# Or explicit / Pydantic config
wa = WhatsAppUtil(config=WhatsAppClientConfig(
    base_url="http://localhost:3001",
    api_key="your-api-key",
    default_country_code="91",
))

wa.send_text("9876543210", "Hello from Python!")       # 10-digit → +91
wa.send_text("+919876543210", "Same chat")
wa.send_image("9876543210", "https://picsum.photos/800/600", caption="Photo")
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `WAHA_API_URL` | — | WAHA base URL (used by `.env.waha` / compose) |
| `WAHA_BASE_URL` | — | Alternate base URL env name |
| `WAHA_URL` | `http://localhost:3000` | Fallback if the above are unset |
| `WAHA_API_KEY` | — | Sent as `X-Api-Key` when set |
| `WAHA_SESSION` | `default` | Default session name |
| `WAHA_DEFAULT_COUNTRY_CODE` | `91` | Prepended to 10-digit local numbers |

Copy `.env.sample.waha` → `.env.waha`, then load it (see `composefiles/waha-compose.yml` — host port **3001** maps to container `3000`; data in `waha/sessions` and `waha/media`):

```python
from dotenv import load_dotenv
load_dotenv(".env.waha")

from modules.wa_util import WhatsAppUtil
wa = WhatsAppUtil()  # picks up WAHA_API_URL, WAHA_API_KEY, WAHA_SESSION
```

## Constructor

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `base_url` | `str` | env / `localhost:3000` | WAHA server URL |
| `api_key` | `Optional[str]` | env | API key header |
| `session` | `str` | `default` | Session used for chat APIs |
| `default_country_code` | `str` | `91` | Prepended to 10-digit local numbers |
| `timeout` | `int` | `30` | HTTP timeout (seconds) |
| `max_retries` | `int` | `2` | Retries on connection errors / 429 |
| `retry_delay` | `float` | `1.0` | Backoff base delay |
| `config` | `WhatsAppClientConfig` | `None` | Optional Pydantic settings object |

## Sessions & Pairing

```python
wa.list_sessions()
wa.create_session(name="default", start=True)
wa.get_session()
wa.start_session()
wa.stop_session()
wa.restart_session()
wa.logout_session()

# QR for linking WhatsApp on your phone
qr_json = wa.get_qr_code(format="raw")
png_bytes = wa.get_qr_code(format="image")

# Screenshot (WEBJS/WPP engines)
jpeg = wa.get_screenshot()
```

## Pydantic models

Structured types live in `wa_util.py` (validated with [Pydantic v2](https://docs.pydantic.dev/)):

| Model | Purpose |
|-------|---------|
| `WhatsAppClientConfig` | Client settings; `from_env()` reads `WAHA_*` vars |
| `WebhookConfig` | Webhook entry for session create/update (`to_api_dict()`) |
| `WebhookRetryConfig`, `WebhookHmacConfig` | Nested webhook options |
| `WahaWebhookEvent` | Parsed inbound webhook; `.message`, `.is_incoming_message` |
| `WahaMessagePayload` | Message fields (`body`, `fromMe`, `chatId`, …) |
| `PhoneNormalizeParams` | Phone normalization rules (used internally) |

```python
from modules.wa_util import (
    WebhookConfig,
    WebhookHmacConfig,
    WebhookRetryConfig,
    WahaWebhookEvent,
    WhatsAppClientConfig,
)

cfg = WebhookConfig(
    url="https://example.com/hooks/waha",
    events=["message"],
    hmac=WebhookHmacConfig(key="secret"),
    retries=WebhookRetryConfig(policy="linear", delay_seconds=2, attempts=5),
)
wa.create_session(webhooks=[cfg.to_api_dict()])

event = WahaWebhookEvent.from_api_payload(request.json())
if event.is_incoming_message:
    print(event.message.body)
```

## Webhook events (what to pass as `events`)

Official list (and which engine supports each):
[WAHA Events docs](https://waha.devlike.pro/docs/how-to/events/#events)

In code, use the enum (autocomplete + presets) or typed strings:

```python
from modules.wa_util import WahaWebhookEvents, WAHAWebhook

# Minimal: new messages + session online/offline
WahaWebhookEvents.default_subscription()   # ["message", "session.status"]

# Chat bot (acks, reactions, edits, …)
WahaWebhookEvents.chat_focused()           # same idea as WAHAWebhook.COMMON_EVENTS

# Every name we document in the enum (not every engine fires every event)
WahaWebhookEvents.all_documented()

# Single event with autocomplete
WahaWebhookEvents.MESSAGE
WahaWebhookEvents.SESSION_STATUS
```

`configure_webhooks(..., events=...)` accepts enum members, strings, or lists of either.

## Webhooks & HMAC

**HMAC is not WAHA’s API key.** It is a **shared secret you invent** when registering the webhook. WAHA signs each POST body; your server proves the request really came from your WAHA instance.

1. Generate or choose a secret: `secret = WAHAWebhook.generate_hmac_key()`
2. Register on WAHA: `wa.configure_webhooks(url, events=..., hmac_key=secret)`  
   Or `generate_hmac_key=True` to create one (logged once — copy to `.env`).
3. Store the same value on your receiver: e.g. `WAHA_WEBHOOK_HMAC_KEY=...`
4. On each POST, verify **raw body bytes** before trusting JSON.

Configure on the session:

```python
secret = WAHAWebhook.generate_hmac_key()
wa.configure_webhooks(
    url="https://your-app.com/webhooks/waha",
    events=WahaWebhookEvents.chat_focused(),
    hmac_key=secret,
)

hook = WAHAWebhook.build_config(
    "https://your-app.com/webhooks/waha",
    events=[WahaWebhookEvents.MESSAGE, WahaWebhookEvents.SESSION_STATUS],
    hmac_key=secret,
)
```

Verify incoming HTTP callbacks (Flask/FastAPI example):

```python
from modules.wa_util import WAHAWebhook

raw_body = request.get_data()
secret = os.environ["WAHA_WEBHOOK_HMAC_KEY"]

if not WAHAWebhook.verify_request(raw_body, request.headers, secret):
    return "invalid signature", 401

# Or raise
WAHAWebhook.require_valid_request(raw_body, request.headers, secret)

# Typed (recommended)
event = WAHAWebhook.parse_event_model(request.json)
if event.is_incoming_message:
    text = event.message.body or ""

# Dict compatibility
legacy = WAHAWebhook.parse_event(request.json)
if WAHAWebhook.is_incoming_message(legacy):
    text = (legacy["payload"] or {}).get("body", "")
```

WAHA sends `X-Webhook-Hmac` (HMAC-SHA512 of the raw body). See [Events docs](https://waha.devlike.pro/docs/how-to/events/).

## Sending Messages

| Method | Endpoint | Notes |
|--------|----------|-------|
| `send_text` | `/api/sendText` | Formatting: `*bold*`, `_italic_`, `` `code` `` |
| `send_link_preview` | `/api/send/link-custom-preview` | Custom OG preview |
| `send_image` | `/api/sendImage` | Image with preview (probes URL `Content-Type`, sets `image/*` MIME) |
| `send_file` | `/api/sendFile` | Generic document (PDF, etc.) — not for image preview |
| `send_voice` | `/api/sendVoice` | Voice notes |
| `send_video` | `/api/sendVideo` | Video |
| `send_sticker` | `/api/sendSticker` | WebP only |
| `send_location` | `/api/sendLocation` | Map pin |
| `send_contact` | `/api/sendContactVcard` | vCard / contact list |
| `send_poll` | `/api/sendPoll` | Poll with options |
| `send_list` | `/api/sendList` | Interactive list |
| `forward_message` | `/api/forwardMessage` | Forward by message id |

**Reply threading:** pass `reply_to="<messageId>"` on send methods (preferred over deprecated `/api/reply`).

**Media inputs (`file` / `FileInput`):**

`FileInput` = `str` path, `str` URL, `pathlib.Path`, or a WAHA `file` dict.  
`send_*` calls `_resolve_file()` — you rarely need the dict unless you want full control.

```python
from pathlib import Path
from modules.wa_util import WhatsAppUtil

# 1) Local file — str or Path (reads + base64 in file_from_path)
wa.send_file("9876543210", "/tmp/report.pdf", caption="Monthly report")
wa.send_image("9876543210", Path("photos/scan.jpg"))

# 2) Public URL — use send_image for photos (MIME + preview), send_file for documents
wa.send_image("9876543210", "https://picsum.photos/400")
wa.send_file("9876543210", "https://example.com/report.pdf")

# 3) Explicit WAHA file dict (remote or base64)
wa.send_image("9876543210", WhatsAppUtil.file_from_url(
    "https://picsum.photos/200", mimetype="image/jpeg",
))
wa.send_file("9876543210", WhatsAppUtil.file_from_path(Path("/tmp/a.pdf")))

# 4) Manual dict (same shape WAHA OpenAPI expects)
wa.send_image("9876543210", {
    "url": "https://picsum.photos/800/600",
    "mimetype": "image/jpeg",
})
```

## Chat Actions

```python
wa.send_seen(chat_id)
wa.start_typing(chat_id)
wa.stop_typing(chat_id)
wa.set_reaction("false_111@c.us_AAA...", "👍")
wa.mark_chat_read(chat_id, messages=30)
```

## Chats & History

```python
wa.list_chats(limit=50, sort_by="conversationTimestamp", sort_order="desc")
wa.get_chats_overview(limit=20)

msgs = wa.get_messages(chat_id, limit=25, download_media=False)
one = wa.get_message(chat_id, message_id)
wa.edit_message(chat_id, message_id, "Updated text")
wa.delete_message(chat_id, message_id)
wa.delete_chat(chat_id)
```

## Contacts

```python
wa.check_phone_exists("12132132130")
wa.get_contact("12132132130@c.us")
```

## Phone & chat id normalization

Bare numbers are normalized before every send:

| Input | Result (default country `91`) |
|-------|-------------------------------|
| `9876543210` | `919876543210@c.us` |
| `+919876543210` | `919876543210@c.us` |
| `919876543210` | `919876543210@c.us` (country code kept) |
| `09876543210` | `919876543210@c.us` (trunk `0` stripped) |

```python
# Default India (+91) on the client
wa = WhatsAppUtil(default_country_code="91")
wa.send_text("9876543210", "hi")

# One-off US number
wa.send_text("4155551234", "hi", country_code="1")

# Or env: WAHA_DEFAULT_COUNTRY_CODE=91
WhatsAppUtil.normalize_phone_digits("9876543210")  # static, default 91
wa.normalize_chat_id("+91 98765 43210")
```

## Error Handling

```python
from modules.wa_util import WAHAAPIError

try:
    wa.send_text("invalid", "hi")
except WAHAAPIError as e:
    print(e.status_code, e.response)
```

## Context Manager

```python
with WhatsAppUtil() as wa:
    wa.send_text(chat, "Hello")
```

## Use Cases

| Scenario | Approach |
|----------|----------|
| Notify users via WhatsApp | `send_text` / `send_template`-style formatted text |
| Send invoices / PDFs | `send_file` with local path or URL |
| Customer support replies | `get_messages` + `send_text(..., reply_to=...)` |
| Inbound automation | `configure_webhooks` + `WAHAWebhook` verification |
| OTP / alerts | `send_text` to `phone_to_chat_id(mobile)` |
| Polls / menus | `send_poll`, `send_list` |

## Production Notes

- Set `WAHA_API_KEY` in production when the WAHA container uses API key auth (`WAHA_API_KEY` env on the server).
- Prefer session webhooks over global `WHATSAPP_HOOK_*` env vars when you need per-app configuration visible in `GET /api/sessions`.
- Use `reply_to` on send endpoints instead of deprecated `/api/reply`.
- `get_screenshot` and some media features depend on the WAHA engine (WEBJS, GOWS, NOWEB, WPP).
- Retries cover transient network failures and HTTP 429 only — 4xx/5xx (except rate limit) raise `WAHAAPIError` immediately.
- Pair a running WAHA instance before sending; session status should reach `WORKING` after QR scan.

## Related Docs

- [WAHA introduction](https://waha.devlike.pro/docs/overview/introduction/)
- [Send messages](https://waha.devlike.pro/docs/how-to/send-messages/)
- [Receive messages / events](https://waha.devlike.pro/docs/how-to/events/)
- [OpenAPI / Swagger](https://waha.devlike.pro/swagger/)
