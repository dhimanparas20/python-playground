"""
Complete usage examples for the datetime_parser module.
Run this file directly to see all examples in action.
"""

from datetime_parser import (
    convert_datetime,
    format_iso,
    list_supported_timezones,
    resolve_timezone,
    string_to_timestamp,
    timestamp_to_string,
)


def example_1_timestamp_to_string():
    """Convert Unix timestamp to human-readable string."""
    print("=" * 70)
    print("EXAMPLE 1: Timestamp to String")
    print("=" * 70)

    # Seconds
    result = timestamp_to_string(1786645200, output_timezone="IST")
    print(f"Timestamp (seconds) : {result['original_timestamp']}")
    print(f"Is Milliseconds     : {result['is_milliseconds']}")
    print(f"UTC                 : {result['utc']}")
    print(f"UTC ISO             : {result['utc_iso']}")
    print(f"Formatted (IST)     : {result['formatted']}")
    print()

    # Milliseconds
    result = timestamp_to_string(
        1786645200000, output_timezone="PST"
    )
    print(f"Timestamp (ms)      : {result['original_timestamp']}")
    print(f"Is Milliseconds     : {result['is_milliseconds']}")
    print(f"Formatted (PST)     : {result['formatted']}")
    print()


def example_2_string_to_timestamp():
    """Convert human-readable string to Unix timestamp."""
    print("=" * 70)
    print("EXAMPLE 2: String to Timestamp")
    print("=" * 70)

    result = string_to_timestamp("August 13, 2026 1:30 PM")
    print(f"Original    : {result['original_string']}")
    print(f"Is Parsed   : {result['is_parsed']}")
    print(f"Timestamp   : {result['timestamp_seconds']}")
    print(f"Timestamp ms: {result['timestamp_milliseconds']}")
    print(f"ISO 8601    : {result['iso_8601']}")
    print()

    result = string_to_timestamp("13/08/2026 13:30")
    print(f"Original    : {result['original_string']}")
    print(f"Timestamp   : {result['timestamp_seconds']}")
    print(f"ISO 8601    : {result['iso_8601']}")
    print()


def example_3_format_iso():
    """Parse a datetime and reformat it to a custom format."""
    print("=" * 70)
    print("EXAMPLE 3: Format / Reformat Datetime")
    print("=" * 70)

    result = format_iso(
        "13/08/2026 13:30",
        output_format="%Y-%m-%d %I:%M:%S %p %Z",
        output_timezone="IST",
    )
    print(f"Input   : {result['original_string']}")
    print(f"Output  : {result['formatted']}")
    print(f"ISO 8601: {result['iso_8601']}")
    print()

    result = format_iso(
        "August 13, 2026 1:30 PM",
        output_format="%d/%m/%Y %H:%M",
        output_timezone="UTC",
    )
    print(f"Input   : {result['original_string']}")
    print(f"Output  : {result['formatted']}")
    print(f"ISO 8601: {result['iso_8601']}")
    print()

    # Common formats
    formats = {
        "Default": "%Y-%m-%d %I:%M:%S %p %Z",
        "Date Only": "%Y-%m-%d",
        "Time Only": "%I:%M:%S %p",
        "European": "%d/%m/%Y %H:%M",
        "US Format": "%m/%d/%Y %I:%M %p",
        "File Safe": "%Y-%m-%d_%H-%M-%S",
        "Short": "%b %d, %Y %I:%M %p",
    }

    print("Common Format Examples:")
    for name, fmt in formats.items():
        result = format_iso(
            "August 13, 2026 1:30 PM",
            output_format=fmt,
            output_timezone="IST",
        )
        print(f"  {name:15} -> {result['formatted']}")
    print()


def example_4_bidirectional():
    """Demonstrate full round-trip: String -> Timestamp -> String."""
    print("=" * 70)
    print("EXAMPLE 4: Round-Trip Conversion (String -> TS -> String)")
    print("=" * 70)

    original = "August 13, 2026 1:30 PM"

    # Step 1: String to Timestamp
    to_ts = string_to_timestamp(original)
    print(f"Original String     : {original}")
    print(f"Timestamp (seconds) : {to_ts['timestamp_seconds']}")
    print()

    # Step 2: Timestamp back to String
    to_str = timestamp_to_string(
        to_ts["timestamp_seconds"], output_timezone="IST"
    )
    print(f"Timestamp           : {to_str['original_timestamp']}")
    print(f"Back to String (IST): {to_str['formatted']}")
    print()

    # Step 3: Verify
    print(f"Match: {original} -> timestamp -> {to_str['formatted']}")
    print()


def example_5_common_use_cases():
    """Show common real-world use cases."""
    print("=" * 70)
    print("EXAMPLE 5: Common Use Cases")
    print("=" * 70)

    # Use case 1: API response has timestamp, need string
    print("Use Case 1: API timestamp to readable string")
    api_timestamp = 1786645200
    result = timestamp_to_string(api_timestamp, output_timezone="IST")
    print(f"  API gave: {api_timestamp}")
    print(f"  You see : {result['formatted']}")
    print()

    # Use case 2: User input needs to be stored as timestamp
    print("Use Case 2: User input to timestamp for database")
    user_input = "Next Friday at 3 PM"
    result = string_to_timestamp(user_input)
    print(f"  User typed : {user_input}")
    print(f"  Store in DB: {result['timestamp_seconds']}")
    print(f"  ISO 8601   : {result['iso_8601']}")
    print()

    # Use case 3: Convert between formats
    print("Use Case 3: Reformat for display")
    db_date = "2026-08-13T13:30:00+05:30"
    result = format_iso(
        db_date,
        output_format="%B %d, %Y at %I:%M %p",
        output_timezone="America/New_York",
    )
    print(f"  Database has : {db_date}")
    print(f"  Display to US: {result['formatted']}")
    print()


# ======================================================================
# Run all examples
# ======================================================================

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print(" DATETIME_PARSER MODULE - COMPLETE USAGE EXAMPLES")
    print("=" * 70 + "\n")

    example_1_timestamp_to_string()
    example_2_string_to_timestamp()
    example_3_format_iso()
    example_4_bidirectional()
    example_5_common_use_cases()

    print("=" * 70)
    print(" ALL EXAMPLES COMPLETED SUCCESSFULLY")
    print("=" * 70)
