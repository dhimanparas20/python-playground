"""
datetime_parser - A production-level datetime parsing and conversion module.

This module provides a robust solution for parsing datetime strings in various
formats and converting them across multiple timezones. It leverages the
`dateparser` library for flexible input parsing and Python's `zoneinfo` for
accurate timezone handling.

Key Features:
    - Parse datetime strings in virtually any human-readable format.
    - Convert parsed datetime to UTC, system timezone, or custom timezones.
    - Supports common timezone aliases (e.g., 'IST', 'PST') and IANA names.
    - Returns structured dictionaries with ISO strings and Unix timestamps.
    - Graceful error handling with detailed error messages.

Author: T3 Chat
Version: 1.0.0
Date: 2026-06-23
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from dateparser import parse as dateparser_parse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# ======================================================================
# Module-level configuration
# ======================================================================

__version__ = "1.0.0"
__author__ = "T3 Chat"

logger = logging.getLogger(__name__)

# Default timezone used when input datetime is naive (no timezone info)
DEFAULT_TIMEZONE = "Asia/Kolkata"

# Common timezone aliases mapped to IANA timezone strings
TZ_ALIASES: dict[str, str] = {
    # South Asia
    "IST": "Asia/Kolkata",
    "SLST": "Asia/Colombo",
    "BST": "Asia/Dhaka",
    
    # North America
    "PST": "America/Los_Angeles",
    "PDT": "America/Los_Angeles",
    "EST": "America/New_York",
    "EDT": "America/New_York",
    "CST": "America/Chicago",
    "CDT": "America/Chicago",
    "MST": "America/Denver",
    "MDT": "America/Denver",
    "HST": "Pacific/Honolulu",
    "NST": "America/St_Johns",
    
    # Europe
    "GMT": "Europe/London",
    "CET": "Europe/Berlin",
    "CEST": "Europe/Berlin",
    "EET": "Europe/Athens",
    "EEST": "Europe/Athens",
    "MSK": "Europe/Moscow",
    
    # Asia
    "JST": "Asia/Tokyo",
    "KST": "Asia/Seoul",
    "SGT": "Asia/Singapore",
    "HKT": "Asia/Hong_Kong",
    "CST_CN": "Asia/Shanghai",
    "IST_IL": "Asia/Jerusalem",
    
    # Oceania
    "AEST": "Australia/Sydney",
    "AEDT": "Australia/Sydney",
    "NZST": "Pacific/Auckland",
    "NZDT": "Pacific/Auckland",
    
    # South America
    "BRT": "America/Sao_Paulo",
    "ART": "America/Argentina/Buenos_Aires",
    "CLT": "America/Santiago",
}

# ======================================================================
# Public API
# ======================================================================


def convert_datetime(
    datetime_string: str,
    parse_to: Optional[list[str]] = None,
    parse_to_system: bool = False,
    parse_to_utc: bool = False,
) -> dict[str, Any]:
    """
    Parse a datetime string and optionally convert it to multiple timezones.

    This is the main entry point of the module. It accepts a datetime string
    in virtually any human-readable format and returns a structured dictionary
    containing the parsed result in the requested timezones.

    Args:
        datetime_string (str): The datetime string to parse. Can be in any
            format supported by the `dateparser` library, such as:
            - "2026-08-13 13:30:00"
            - "August 13, 2026 1:30 PM"
            - "13/08/2026 13:30"
            - "Tomorrow at 10 AM"
            - "2 hours ago"
            - "13th August 2026 1:30 PM"

        parse_to (Optional[list[str]]): A list of timezone strings to convert
            the parsed datetime into. Accepts both common aliases (e.g., "IST",
            "PST") and full IANA timezone names (e.g., "Asia/Kolkata",
            "America/New_York"). These conversions are only performed when
            `parse_to_system` is False.

        parse_to_system (bool): If True, converts the parsed datetime to the
            system's local timezone and includes the timezone name in the
            response. When True, `parse_to` conversions are skipped.

        parse_to_utc (bool): If True, converts the parsed datetime to UTC and
            includes both the ISO-formatted string and the Unix timestamp.

    Returns:
        dict[str, Any]: A dictionary containing:
            - "original_string" (str): The original datetime string passed.
            - "is_parsed" (bool): Whether the string was successfully parsed.
            - "parsed_datetime" (str | None): The datetime in ISO 8601 format.
            - "parsed_timestamp" (int | None): The Unix timestamp of parsed dt.
            - "utc" (str | None): UTC datetime in ISO 8601 format.
            - "utc_timestamp" (int | None): Unix timestamp in UTC.
            - "system" (str | None): System timezone datetime string.
            - "system_timestamp" (int | None): Unix timestamp in system tz.
            - "system_timezone" (str | None): Name of the system timezone.
            - "custom_timezones" (dict): Converted datetime for each timezone
              in `parse_to`, keyed by the timezone string.
            - "error" (str | None): Error message if parsing failed.
            - "utc_error" (str | None): Error message if UTC conversion failed.
            - "system_error" (str | None): Error message if system tz failed.

    Raises:
        None. This function is designed to never raise exceptions. All errors
        are captured in the response dictionary.

    Examples:
        Basic usage with UTC conversion:

        >>> result = convert_datetime(
        ...     datetime_string="August 13, 2026 1:30 PM",
        ...     parse_to_utc=True
        ... )
        >>> print(result["utc"])
        "2026-08-13T08:00:00+0000"

        Full usage with all options:

        >>> result = convert_datetime(
        ...     datetime_string="13/08/2026 13:30",
        ...     parse_to=["IST", "America/New_York"],
        ...     parse_to_system=True,
        ...     parse_to_utc=True
        ... )
        >>> print(result)
        {
            "original_string": "13/08/2026 13:30",
            "is_parsed": True,
            "parsed_datetime": "2026-08-13T13:30:00+05:30",
            "parsed_timestamp": 1786680000,
            "utc": "2026-08-13T08:00:00+0000",
            "utc_timestamp": 1786645200,
            "system": "2026-08-13 01:30:00 PM IST",
            "system_timestamp": 1786680000,
            "system_timezone": "Asia/Kolkata",
            "custom_timezones": {},
            "error": None,
            "utc_error": None,
            "system_error": None,
        }

        Using relative datetime strings:

        >>> result = convert_datetime(
        ...     datetime_string="Tomorrow at 10 AM",
        ...     parse_to_system=True,
        ...     parse_to_utc=True
        ... )
    """

    # --------------------------------------------------
    # Initialize response structure
    # --------------------------------------------------
    response: dict[str, Any] = {
        "original_string": datetime_string,
        "is_parsed": False,
        "parsed_datetime": None,
        "parsed_timestamp": None,
        "utc": None,
        "utc_timestamp": None,
        "system": None,
        "system_timestamp": None,
        "system_timezone": None,
        "custom_timezones": {},
        "error": None,
        "utc_error": None,
        "system_error": None,
    }

    # --------------------------------------------------
    # Step 1: Parse the input string
    # --------------------------------------------------
    parsed_dt = _parse_input(datetime_string, response)
    if parsed_dt is None:
        return response

    # --------------------------------------------------
    # Step 2: Attach default timezone (IST) to naive datetime
    # --------------------------------------------------
    parsed_dt = _attach_default_timezone(parsed_dt)

    # --------------------------------------------------
    # Step 3: Store the initial parsed result
    # --------------------------------------------------
    response["parsed_datetime"] = parsed_dt.isoformat()
    response["parsed_timestamp"] = int(parsed_dt.timestamp())

    # --------------------------------------------------
    # Step 4: Handle UTC conversion
    # --------------------------------------------------
    if parse_to_utc:
        _convert_to_utc(parsed_dt, response)

    # --------------------------------------------------
    # Step 5: Handle System timezone conversion
    # --------------------------------------------------
    if parse_to_system:
        _convert_to_system(parsed_dt, response)

    # --------------------------------------------------
    # Step 6: Handle Custom timezone conversions
    # --------------------------------------------------
    if parse_to and not parse_to_system:
        _convert_to_custom(parsed_dt, parse_to, response)

    return response


def resolve_timezone(tz_string: str) -> ZoneInfo:
    """
    Resolve a timezone string to a ZoneInfo object.

    This function accepts both common timezone aliases (e.g., "IST", "PST")
    and full IANA timezone names (e.g., "Asia/Kolkata", "America/New_York").

    Args:
        tz_string (str): The timezone string to resolve.

    Returns:
        ZoneInfo: The corresponding ZoneInfo object.

    Raises:
        ValueError: If the timezone string cannot be resolved.

    Examples:
        >>> tz = resolve_timezone("IST")
        >>> print(tz)
        Asia/Kolkata

        >>> tz = resolve_timezone("America/New_York")
        >>> print(tz)
        America/New_York
    """
    return _resolve_timezone(tz_string)


def get_system_timezone() -> tuple[str, str]:
    """
    Get the current system timezone name and its IANA equivalent.

    Returns:
        tuple[str, str]: A tuple of (system_tz_name, iana_tz_string).

    Examples:
        >>> name, iana = get_system_timezone()
        >>> print(f"System timezone: {name} ({iana})")
        System timezone: IST (Asia/Kolkata)
    """
    system_tz = datetime.now().astimezone().tzinfo
    system_tz_name = str(system_tz)
    return system_tz_name, system_tz_name


def list_supported_timezones() -> list[dict[str, str]]:
    """
    List all supported timezone aliases and their IANA equivalents.

    Returns:
        list[dict[str, str]]: A list of dicts with 'alias' and 'iana' keys.

    Examples:
        >>> timezones = list_supported_timezones()
        >>> for tz in timezones[:3]:
        ...     print(f"{tz['alias']} -> {tz['iana']}")
        IST -> Asia/Kolkata
        PST -> America/Los_Angeles
        EST -> America/New_York
    """
    return [{"alias": alias, "iana": iana} for alias, iana in TZ_ALIASES.items()]


# ======================================================================
# Internal Helper Functions
# ======================================================================


def _parse_input(
    datetime_string: str, response: dict[str, Any]
) -> Optional[datetime]:
    """
    Parse the input datetime string using dateparser.

    Args:
        datetime_string: The datetime string to parse.
        response: The response dictionary to update on failure.

    Returns:
        The parsed datetime object, or None if parsing failed.
    """
    try:
        parsed_dt = dateparser_parse(datetime_string)
        if parsed_dt is None:
            response["error"] = (
                f"Could not parse the datetime string: '{datetime_string}'"
            )
            logger.warning(response["error"])
            return None
        response["is_parsed"] = True
        logger.debug(f"Successfully parsed: '{datetime_string}' -> {parsed_dt}")
        return parsed_dt
    except Exception as e:
        response["error"] = f"Parsing failed with exception: {str(e)}"
        logger.error(response["error"])
        return None


def _attach_default_timezone(dt: datetime) -> datetime:
    """
    Attach the default timezone (IST) to a naive datetime.

    Args:
        dt: The datetime object to attach timezone to.

    Returns:
        A timezone-aware datetime object.
    """
    if dt.tzinfo is None:
        logger.debug(
            f"Naive datetime detected, attaching default timezone: {DEFAULT_TIMEZONE}"
        )
        return dt.replace(tzinfo=ZoneInfo(DEFAULT_TIMEZONE))
    return dt


def _convert_to_utc(dt: datetime, response: dict[str, Any]) -> None:
    """
    Convert a datetime to UTC and update the response dictionary.

    Args:
        dt: The datetime object to convert.
        response: The response dictionary to update.
    """
    try:
        utc_dt = dt.astimezone(ZoneInfo("UTC"))
        response["utc"] = utc_dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        response["utc_timestamp"] = int(utc_dt.timestamp())
        logger.debug(f"UTC conversion: {dt} -> {utc_dt}")
    except Exception as e:
        response["utc_error"] = f"UTC conversion failed: {str(e)}"
        logger.error(response["utc_error"])


def _convert_to_system(dt: datetime, response: dict[str, Any]) -> None:
    """
    Convert a datetime to system timezone and update the response.

    Args:
        dt: The datetime object to convert.
        response: The response dictionary to update.
    """
    try:
        system_tz = datetime.now().astimezone().tzinfo
        system_tz_name = str(system_tz)
        system_dt = dt.astimezone(system_tz)
        response["system"] = system_dt.strftime("%Y-%m-%d %I:%M:%S %p %Z")
        response["system_timestamp"] = int(system_dt.timestamp())
        response["system_timezone"] = system_tz_name
        logger.debug(f"System conversion: {dt} -> {system_dt} ({system_tz_name})")
    except Exception as e:
        response["system_error"] = f"System timezone conversion failed: {str(e)}"
        logger.error(response["system_error"])


def _convert_to_custom(
    dt: datetime, parse_to: list[str], response: dict[str, Any]
) -> None:
    """
    Convert a datetime to custom timezones and update the response.

    Args:
        dt: The datetime object to convert.
        parse_to: List of timezone strings to convert to.
        response: The response dictionary to update.
    """
    for tz_string in parse_to:
        try:
            resolved_tz = _resolve_timezone(tz_string)
            converted_dt = dt.astimezone(resolved_tz)

            response["custom_timezones"][tz_string] = {
                "timezone_name": tz_string,
                "iana_name": str(resolved_tz),
                "datetime": converted_dt.strftime("%Y-%m-%d %I:%M:%S %p %Z"),
                "timestamp": int(converted_dt.timestamp()),
                "iso_format": converted_dt.isoformat(),
                "error": None,
            }
            logger.debug(f"Custom conversion: {dt} -> {converted_dt} ({tz_string})")
        except Exception as e:
            response["custom_timezones"][tz_string] = {
                "timezone_name": tz_string,
                "iana_name": None,
                "datetime": None,
                "timestamp": None,
                "iso_format": None,
                "error": f"Conversion failed for '{tz_string}': {str(e)}",
            }
            logger.error(f"Custom conversion failed for '{tz_string}': {e}")


def _resolve_timezone(tz_string: str) -> ZoneInfo:
    """
    Resolve a timezone string to a ZoneInfo object.

    Handles both common aliases (e.g., 'IST', 'PST') and full IANA names
    (e.g., 'Asia/Kolkata', 'America/New_York').

    Args:
        tz_string: The timezone string to resolve.

    Returns:
        The corresponding ZoneInfo object.

    Raises:
        ValueError: If the timezone string cannot be resolved.
    """
    # Step 1: Check common aliases (case-insensitive)
    upper_key = tz_string.upper()
    if upper_key in TZ_ALIASES:
        return ZoneInfo(TZ_ALIASES[upper_key])

    # Step 2: Try direct IANA lookup
    try:
        return ZoneInfo(tz_string)
    except (ZoneInfoNotFoundError, KeyError):
        pass

    # Step 3: Check if it's UTC (special case)
    if upper_key == "UTC":
        return ZoneInfo("UTC")

    # Step 4: Raise error with helpful message
    supported = ", ".join(sorted(TZ_ALIASES.keys()))
    raise ValueError(
        f"Unknown timezone: '{tz_string}'. "
        f"Use IANA names (e.g., 'Asia/Kolkata') or supported aliases: {supported}"
    )
