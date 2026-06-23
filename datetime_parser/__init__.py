"""
datetime_parser package.

Usage:
    from datetime_parser import convert_datetime, resolve_timezone, get_system_timezone

    result = convert_datetime(
        datetime_string="August 13, 2026 1:30 PM",
        parse_to=["IST", "PST"],
        parse_to_system=True,
        parse_to_utc=True
    )
    print(result)
"""

from .datetime_parser import (
    __author__,
    __version__,
    convert_datetime,
    list_supported_timezones,
    resolve_timezone,
    get_system_timezone,
)

__all__ = [
    "__version__",
    "__author__",
    "convert_datetime",
    "resolve_timezone",
    "get_system_timezone",
    "list_supported_timezones",
]
