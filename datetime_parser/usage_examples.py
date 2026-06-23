"""
Complete usage examples for the datetime_parser module.

Run this file directly to see all examples in action:
    python usage_examples.py
"""

from datetime_parser import (
    convert_datetime,
    resolve_timezone,
    get_system_timezone,
    list_supported_timezones,
)


def example_1_basic_utc():
    """Parse a datetime string and convert to UTC only."""
    print("=" * 70)
    print("EXAMPLE 1: Basic UTC Conversion")
    print("=" * 70)

    result = convert_datetime(
        datetime_string="August 13, 2026 1:30 PM",
        parse_to_utc=True,
    )

    print(f"Original : {result['original_string']}")
    print(f"Parsed   : {result['parsed_datetime']}")
    print(f"UTC      : {result['utc']}")
    print(f"Timestamp: {result['utc_timestamp']}")
    print()


def example_2_system_time():
    """Parse and convert to system timezone."""
    print("=" * 70)
    print("EXAMPLE 2: System Timezone Conversion")
    print("=" * 70)

    result = convert_datetime(
        datetime_string="13/08/2026 13:30",
        parse_to_system=True,
    )

    print(f"Original  : {result['original_string']}")
    print(f"Parsed    : {result['parsed_datetime']}")
    print(f"System Tz : {result['system_timezone']}")
    print(f"System    : {result['system']}")
    print(f"Timestamp : {result['system_timestamp']}")
    print()


def example_3_custom_timezones():
    """Parse and convert to multiple custom timezones."""
    print("=" * 70)
    print("EXAMPLE 3: Custom Timezone Conversions")
    print("=" * 70)

    result = convert_datetime(
        datetime_string="2026-08-13T13:30:00",
        parse_to=["IST", "PST", "EST", "JST"],
        parse_to_system=False,
        parse_to_utc=False,
    )

    print(f"Original: {result['original_string']}")
    print(f"Parsed  : {result['parsed_datetime']}")
    print()
    for tz, data in result["custom_timezones"].items():
        if data.get("error"):
            print(f"  {tz}: ERROR - {data['error']}")
        else:
            print(f"  {tz:20} -> {data['datetime']:30} (ts: {data['timestamp']})")
    print()


def example_4_full_conversion():
    """Parse with all options enabled."""
    print("=" * 70)
    print("EXAMPLE 4: Full Conversion (UTC + System + Custom)")
    print("=" * 70)

    result = convert_datetime(
        datetime_string="August 1, 2026 10:00 AM",
        parse_to=["Asia/Kolkata", "America/New_York"],
        parse_to_system=True,
        parse_to_utc=True,
    )

    print(f"Original    : {result['original_string']}")
    print(f"Parsed      : {result['parsed_datetime']}")
    print(f"Timestamp   : {result['parsed_timestamp']}")
    print(f"UTC         : {result['utc']}")
    print(f"UTC ts      : {result['utc_timestamp']}")
    print(f"System Tz   : {result['system_timezone']}")
    print(f"System      : {result['system']}")
    print(f"System ts   : {result['system_timestamp']}")
    print(f"Custom Tzs  : (skipped when parse_to_system=True)")
    print()


def example_5_relative_dates():
    """Parse relative datetime strings like 'tomorrow', '2 hours ago'."""
    print("=" * 70)
    print("EXAMPLE 5: Relative Date Strings")
    print("=" * 70)

    relative_strings = [
        "Tomorrow at 10 AM",
        "Next Friday at 3:30 PM",
        "2 hours ago",
        "Last Monday at 9 AM",
    ]

    for date_str in relative_strings:
        result = convert_datetime(
            datetime_string=date_str,
            parse_to_system=True,
            parse_to_utc=True,
        )
        print(f"Input    : {date_str}")
        print(f"UTC      : {result['utc']}")
        print(f"System   : {result['system']}")
        print()


def example_6_various_formats():
    """Parse datetime in various input formats."""
    print("=" * 70)
    print("EXAMPLE 6: Various Input Formats")
    print("=" * 70)

    formats = [
        "2026-08-13 13:30:00",
        "August 13, 2026 1:30 PM",
        "13/08/2026 13:30",
        "13th August 2026 1:30 PM",
        "Aug 13, 2026 at 1:30 PM",
        "13.08.2026 13:30",
    ]

    for fmt in formats:
        result = convert_datetime(
            datetime_string=fmt,
            parse_to_utc=True,
        )
        status = "OK" if result["is_parsed"] else "FAIL"
        print(f"  [{status}] {fmt:45} -> UTC: {result['utc']}")
    print()


def example_7_error_handling():
    """Demonstrate graceful error handling."""
    print("=" * 70)
    print("EXAMPLE 7: Error Handling")
    print("=" * 70)

    result = convert_datetime(
        datetime_string="not a real date at all",
        parse_to_utc=True,
    )

    print(f"Original : {result['original_string']}")
    print(f"Parsed   : {result['is_parsed']}")
    print(f"Error    : {result['error']}")
    print()


def example_8_helper_functions():
    """Demonstrate helper functions."""
    print("=" * 70)
    print("EXAMPLE 8: Helper Functions")
    print("=" * 70)

    # Resolve timezone
    tz = resolve_timezone("IST")
    print(f"resolve_timezone('IST') -> {tz}")

    # Get system timezone
    name, iana = get_system_timezone()
    print(f"get_system_timezone()  -> {name} ({iana})")

    # List supported timezones
    all_tzs = list_supported_timezones()
    print(f"list_supported_timezones() -> {len(all_tzs)} timezones")
    for tz in all_tzs[:5]:
        print(f"  {tz['alias']:8} -> {tz['iana']}")
    print()


# ======================================================================
# Run all examples
# ======================================================================

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print(" DATETIME_PARSER MODULE - COMPLETE USAGE EXAMPLES")
    print("=" * 70 + "\n")

    example_1_basic_utc()
    example_2_system_time()
    example_3_custom_timezones()
    example_4_full_conversion()
    example_5_relative_dates()
    example_6_various_formats()
    example_7_error_handling()
    example_8_helper_functions()

    print("=" * 70)
    print(" ALL EXAMPLES COMPLETED SUCCESSFULLY")
    print("=" * 70)
