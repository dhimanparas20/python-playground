from modules.datetime_parser import convert_datetime, list_supported_timezones

# ─────────────────────────────────────────────
# Example 1: Convert to UTC
# ─────────────────────────────────────────────
result = convert_datetime(
    datetime_string="August 13, 2026 1:30 PM",
    parse_to_utc=True
)

print("Original String:", result["original_string"])
print("Is Parsed:", result["is_parsed"])
print("UTC Time:", result["utc"])
print("UTC Timestamp:", result["utc_timestamp"])
print("-" * 50)

# ─────────────────────────────────────────────
# Example 2: Convert to System Time
# ─────────────────────────────────────────────
result = convert_datetime(
    datetime_string="13/08/2026 13:30",
    parse_to_system=True
)

print("Original String:", result["original_string"])
print("System Timezone:", result["system_timezone"])
print("System Time:", result["system"])
print("System Timestamp:", result["system_timestamp"])
print("-" * 50)

# ─────────────────────────────────────────────
# Example 3: Convert to Multiple Timezones
# ─────────────────────────────────────────────
result = convert_datetime(
    datetime_string="2026-08-13 13:30:00",
    parse_to=["IST", "PST", "EST", "JST", "GMT"],
    parse_to_utc=True,
)

print("Original String:", result["original_string"])
print("UTC Time:", result["utc"])
print()
print("Custom Timezone Conversions:")
for tz, data in result["custom_timezones"].items():
    if data.get("error"):
        print(f"  {tz}: ERROR - {data['error']}")
    else:
        print(f"  {tz:8} -> {data['datetime']} (ts: {data['timestamp']})")
print("-" * 50)

# ─────────────────────────────────────────────
# Example 4: Full conversion (all options)
# ─────────────────────────────────────────────
result = convert_datetime(
    datetime_string="Tomorrow at 10 AM",
    parse_to=["Asia/Kolkata", "America/New_York"],
    parse_to_system=True,
    parse_to_utc=True,
)

import json
print(json.dumps(result, indent=2))
print("-" * 50)

# ─────────────────────────────────────────────
# Example 5: List all supported timezones
# ─────────────────────────────────────────────
timezones = list_supported_timezones()
print(f"Total supported aliases: {len(timezones)}")
print()
for tz in timezones[:10]:
    print(f"  {tz['alias']:12} -> {tz['iana']}")

print(f"  ... and {len(timezones) - 10} more")
