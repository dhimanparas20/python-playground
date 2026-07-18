# DateTimeParser — Production-Ready Datetime Parsing & Conversion

A robust Python module for parsing datetime strings in virtually any human-readable format and converting them across 100+ timezone aliases worldwide. Uses `dateparser` for flexible input parsing and `zoneinfo` for accurate timezone handling.

## Installation

```bash
pip install dateparser
```

## Quick Start

```python
from modules.datetime_parser import convert_datetime

# Parse any datetime string and get UTC
result = convert_datetime("August 13, 2026 1:30 PM", parse_to_utc=True)
print(result["utc"])        # "2026-08-13T08:00:00+0000"
print(result["utc_timestamp"])  # 1786645200

# Convert to system timezone
result = convert_datetime("13/08/2026 13:30", parse_to_system=True)
print(result["system"])     # "2026-08-13 01:30:00 PM IST"
```

## Public API

### `convert_datetime(datetime_string, parse_to=None, parse_to_system=False, parse_to_utc=False)`

The main entry point. Parses a datetime string and optionally converts to multiple timezones.

**Args:**

| Arg | Type | Default | Description |
|-----|------|---------|-------------|
| `datetime_string` | `str` | *required* | Datetime string in any human-readable format |
| `parse_to` | `list[str] \| None` | `None` | List of timezone aliases/IANA names to convert to |
| `parse_to_system` | `bool` | `False` | Convert to system local timezone |
| `parse_to_utc` | `bool` | `False` | Convert to UTC |

**Returns:** Dict with keys:

| Key | Type | Description |
|-----|------|-------------|
| `original_string` | `str` | Original input string |
| `is_parsed` | `bool` | Whether parsing succeeded |
| `parsed_datetime` | `str \| None` | Parsed datetime in ISO 8601 |
| `parsed_timestamp` | `int \| None` | Unix timestamp of parsed datetime |
| `utc` | `str \| None` | UTC datetime ISO string |
| `utc_timestamp` | `int \| None` | UTC Unix timestamp |
| `system` | `str \| None` | System timezone formatted string |
| `system_timezone` | `str \| None` | System timezone name |
| `custom_timezones` | `dict` | Converted datetimes for each requested timezone |
| `error` | `str \| None` | Error message if parsing failed |

**Input formats accepted:** `"2026-08-13 13:30:00"`, `"August 13, 2026 1:30 PM"`, `"13/08/2026 13:30"`, `"Tomorrow at 10 AM"`, `"2 hours ago"`, `"13th August 2026 1:30 PM"`, and hundreds more.

**Examples:**

```python
# UTC only
result = convert_datetime("August 13, 2026 1:30 PM", parse_to_utc=True)

# System timezone
result = convert_datetime("13/08/2026 13:30", parse_to_system=True)

# Custom timezones
result = convert_datetime("2026-08-13 13:30:00", parse_to=["IST", "PST", "EST", "JST", "GMT"])

# Full — all options
result = convert_datetime(
    "Tomorrow at 10 AM",
    parse_to=["Asia/Kolkata", "America/New_York"],
    parse_to_system=True,
    parse_to_utc=True,
)
```

### `timestamp_to_string(timestamp, output_timezone="Asia/Kolkata", output_format="%Y-%m-%d %I:%M:%S %p %Z")`

Convert a Unix timestamp to a human-readable datetime string.

**Args:**

| Arg | Type | Default | Description |
|-----|------|---------|-------------|
| `timestamp` | `int \| float` | *required* | Unix timestamp (seconds or ms — auto-detected) |
| `output_timezone` | `str` | `"Asia/Kolkata"` | Timezone for the output |
| `output_format` | `str` | `"%Y-%m-%d %I:%M:%S %p %Z"` | strftime format string |

**Returns:** Dict with `original_timestamp`, `is_milliseconds`, `utc`, `utc_iso`, `formatted`, `timezone`, `error`.

```python
# Seconds
result = timestamp_to_string(1786645200, output_timezone="IST")
# formatted: "2026-08-13 01:30:00 PM IST"

# Milliseconds (auto-detected)
result = timestamp_to_string(1786645200000, output_timezone="PST")
# formatted: "2026-08-13 01:00:00 AM PDT"
```

### `string_to_timestamp(datetime_string, input_timezone="Asia/Kolkata")`

Convert a human-readable datetime string to a Unix timestamp.

| Arg | Type | Default | Description |
|-----|------|---------|-------------|
| `datetime_string` | `str` | *required* | Datetime string to parse |
| `input_timezone` | `str` | `"Asia/Kolkata"` | Timezone for naive datetimes |

```python
result = string_to_timestamp("August 13, 2026 1:30 PM")
# timestamp_seconds: 1786680000
# timestamp_milliseconds: 1786680000000
```

### `format_iso(datetime_string, output_format="%Y-%m-%d %I:%M:%S %p %Z", output_timezone="Asia/Kolkata")`

Parse a datetime string and reformat it to a custom output format.

| Arg | Type | Default | Description |
|-----|------|---------|-------------|
| `datetime_string` | `str` | *required* | Input datetime string |
| `output_format` | `str` | `"%Y-%m-%d %I:%M:%S %p %Z"` | Desired strftime output format |
| `output_timezone` | `str` | `"Asia/Kolkata"` | Timezone for the output |

```python
# Default format
result = format_iso("13/08/2026 13:30")
# formatted: "2026-08-13 01:30:00 PM IST"

# Custom format in UTC
result = format_iso(
    "August 13, 2026 1:30 PM",
    output_format="%d/%m/%Y %H:%M",
    output_timezone="UTC",
)
# formatted: "13/08/2026 08:00"

# Common format examples
format_iso("August 13, 2026 1:30 PM", output_format="%Y-%m-%d")
# "2026-08-13"

format_iso("August 13, 2026 1:30 PM", output_format="%B %d, %Y at %I:%M %p")
# "August 13, 2026 at 01:30 PM"
```

### `resolve_timezone(tz_string)`

Resolve a timezone string (alias or IANA name) to a `ZoneInfo` object.

```python
tz = resolve_timezone("IST")          # ZoneInfo("Asia/Kolkata")
tz = resolve_timezone("PST")          # ZoneInfo("America/Los_Angeles")
tz = resolve_timezone("Asia/Kolkata") # ZoneInfo("Asia/Kolkata")
```

### `get_system_timezone()`

Get the current system timezone name and its IANA equivalent.

```python
name, iana = get_system_timezone()
# name: "IST", iana: "Asia/Kolkata"
```

### `list_supported_timezones()`

List all supported timezone aliases and their IANA equivalents (100+ entries).

```python
timezones = list_supported_timezones()
for tz in timezones[:5]:
    print(f"{tz['alias']} -> {tz['iana']}")
# IST -> Asia/Kolkata
# PST -> America/Los_Angeles
# EST -> America/New_York
# JST -> Asia/Tokyo
# GMT -> Europe/London
```

## Supported Timezone Aliases (100+)

The module includes a comprehensive mapping of common timezone abbreviations to IANA names, covering:

| Region | Examples |
|--------|----------|
| **South Asia** | IST, SLST, BST_BD, NPT, PKT, AFT |
| **Southeast Asia** | ICT, SGT, MYT, WIB, WITA, WIT, PHT |
| **East Asia** | JST, KST, CST_CN, HKT, CST_TW, ULAT |
| **Middle East** | AST_SA, AST_AE, IRST, TRT, AZT, GET |
| **Europe (West)** | GMT, BST, WET, WEST, CET, CEST |
| **Europe (East)** | EET, EEST, MSK, FET |
| **Russia** | YEKT, OMST, KRAS, IRKT, YAKT, VLAT |
| **North America** | EST, EDT, CST, CDT, MST, MDT, PST, PDT, AKST, HST |
| **Canada** | PST_CA, MST_CA, CST_CA, EST_CA, NST_CA |
| **Mexico & Central America** | CST_MX, CST_CR, CST_GT, EST_CU |
| **South America** | BRT, ART, CLT, PET, VET, BOT |
| **Africa** | WAT, CAT, EAT, SAST |
| **Oceania** | AEST, AEDT, ACST, AWST, NZST, NZDT, FJT |
| **Atlantic & Special** | WET_FO, WET_IS, WET_MA, EET_EG |

All aliases can be used interchangeably with IANA names in any function that accepts a timezone string.

## Use Cases

| Use Case | Solution |
|----------|----------|
| API returns Unix timestamp, show as readable date | `timestamp_to_string(ts, output_timezone="IST")` |
| User enters "Tomorrow at 3 PM", store as timestamp | `string_to_timestamp("Tomorrow at 3 PM")` |
| Database has ISO string, display in user's timezone | `format_iso(db_date, output_timezone="America/New_York")` |
| Convert European format to US format | `format_iso("13/08/2026", output_format="%m/%d/%Y")` |
| Get current time in multiple timezones | `convert_datetime("now", parse_to=["IST", "PST", "GMT"])` |
| Need UTC timestamp for logging | `convert_datetime(event_time, parse_to_utc=True)["utc_timestamp"]` |
| Round-trip: string → timestamp → string | Chain `string_to_timestamp` → `timestamp_to_string` |

## Production Notes

- Never raises exceptions — all errors captured in response dict with descriptive messages
- Handles both seconds and millisecond timestamps (auto-detected via magnitude > 1e12)
- 100+ timezone aliases covering every major country/region
- Naive datetimes (no timezone info) default to `Asia/Kolkata`
- `list_supported_timezones()` includes both `alias` and reverse lookup via `get_all_aliases_for_iana()`
- Full type hints on all public and internal functions
- Logging via Python's `logging` module (debug/error tracing)
