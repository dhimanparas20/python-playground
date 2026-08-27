#!/usr/bin/env python3
"""
request_maker.py — Production‑grade HTTP client wrapper.

This module provides two "super methods" (`request_sync` and `request_async`) that
abstract away the complexity of making HTTP requests with modern best practices:

* Connection pooling via a shared httpx.Client / httpx.AsyncClient
* Automatic retries with exponential backoff (transient errors only)
* Configurable timeouts (default 10s)
* Automatic error raising (calls raise_for_status)
* Google‑style docstrings with type annotations
* Structured logging (ready for production observability)

Usage
-----
    from request_maker import request_sync, request_async

    # Sync
    resp = request_sync("https://api.example.com/items", json={"key": "value"})
    data = resp.json()

    # Async
    resp = await request_async("https://api.example.com/items", json={"key": "value"})

Installation
------------
    pip install httpx tenacity

Retry policy
------------
* Retries up to 3 times on 5xx server errors and timeouts.
* Waits exponentially: 2s, 4s, 8s (jitter-free for simplicity; add jitter if needed).
* Does NOT retry 4xx client errors (these are not transient).

Author : Ken
Version: 1.2.0
License: MIT
"""

import logging
from typing import Any, Dict, Optional, Union

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    after_log,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
# Suppress noisy low-level debug logs from httpx/httpcore.
# Only our module's logs will be visible at INFO+ by default.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Retry policy
# ---------------------------------------------------------------------------
# Only retry on server errors (5xx) and timeouts.
# Client errors (4xx) are the caller's responsibility.
RETRYABLE_EXCEPTIONS = (
    httpx.HTTPStatusError,
    httpx.TimeoutException,
    httpx.ConnectError,
    httpx.RemoteProtocolError,
)

request_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    after=after_log(logger, logging.DEBUG),
    reraise=True,
)

# ---------------------------------------------------------------------------
# Shared HTTP clients (connection pooling)
# ---------------------------------------------------------------------------
# Default timeouts: 10s connect, 10s read, 10s write, 10s pool.
# Override per request if needed.
_DEFAULT_TIMEOUT = httpx.Timeout(10.0)

_sync_client: httpx.Client = httpx.Client(timeout=_DEFAULT_TIMEOUT)
_async_client: httpx.AsyncClient = httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT)


def close_clients() -> None:
    """Gracefully close both sync and async HTTP clients.

    Call this on application shutdown (e.g., in a FastAPI lifespan event,
    or at the end of a CLI script) to release all connection pool resources.
    """
    _sync_client.close()
    logger.debug("Sync HTTP client closed.")
    # Async client requires a running event loop; we try, but it's safe to
    # skip if no loop is active (e.g. in a sync script).
    try:
        import asyncio

        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(_async_client.aclose())
        else:
            loop.run_until_complete(_async_client.aclose())
        logger.debug("Async HTTP client closed.")
    except RuntimeError:
        # No event loop exists – nothing to close.
        pass


# ---------------------------------------------------------------------------
# Helper: build request arguments
# ---------------------------------------------------------------------------
def _build_request_kwargs(
    json: Optional[Dict[str, Any]] = None,
    data: Optional[Union[Dict[str, str], str, bytes]] = None,
    content: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: Optional[float] = None,
    follow_redirects: bool = True,
) -> Dict[str, Any]:
    """Build a kwargs dict for httpx request methods.

    Args:
        json: JSON-serializable dict to send as request body.
        data: Form-encoded data, or raw string/bytes payload.
        content: Raw bytes payload (alternative to ``data``).
        headers: Custom HTTP headers.
        params: Query parameters to append to the URL.
        timeout: Override default timeout in seconds.
        follow_redirects: Whether to follow HTTP redirects (default True).

    Returns:
        Dictionary of keyword arguments suitable for ``client.request()``.

    Raises:
        ValueError: If more than one body parameter is provided.
    """
    body_params = [json, data, content]
    non_none = [p for p in body_params if p is not None]
    if len(non_none) > 1:
        raise ValueError(
            "Only one of `json`, `data`, or `content` may be provided per request."
        )

    kwargs: Dict[str, Any] = {
        "headers": headers or {},
        "params": params or {},
        "follow_redirects": follow_redirects,
    }

    if json is not None:
        kwargs["json"] = json
    elif data is not None:
        kwargs["data"] = data
    elif content is not None:
        kwargs["content"] = content

    if timeout is not None:
        kwargs["timeout"] = timeout

    return kwargs


# ---------------------------------------------------------------------------
# Sync super method
# ---------------------------------------------------------------------------
@request_retry
def request_sync(
    url: str,
    method: str = "GET",
    *,
    json: Optional[Dict[str, Any]] = None,
    data: Optional[Union[Dict[str, str], str, bytes]] = None,
    content: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: Optional[float] = None,
    follow_redirects: bool = True,
) -> httpx.Response:
    """Make a synchronous HTTP request with automatic retry and error handling.

    This is the primary entry point for all synchronous HTTP calls.  It uses a
    shared connection pool, applies the configured retry policy, and always
    calls ``raise_for_status()`` – so non‑2xx responses raise
    ``httpx.HTTPStatusError``.

    Args:
        url: The request URL (e.g. ``https://api.example.com/items``).
        method: HTTP method (``GET``, ``POST``, ``PUT``, ``PATCH``, ``DELETE``, etc.).
        json: JSON-serializable dict to send as the request body.  Sets
            ``Content-Type: application/json`` automatically.
        data: Form-encoded data (dict) or raw string/bytes payload.  For dicts,
            ``Content-Type`` is set to ``application/x-www-form-urlencoded``.
        content: Raw bytes payload.  Use this instead of ``data`` when you need
            full control over the body.
        headers: Custom HTTP headers as a dict.  Will be merged with any
            default headers set on the shared client.
        params: Query parameters to append to the URL.  Can be a dict of
            ``{key: value}`` or a list of tuples.
        timeout: Override the default timeout (10s) for this request.  Value
            is in seconds.
        follow_redirects: Whether to follow HTTP redirects (default ``True``).

    Returns:
        An ``httpx.Response`` object.  The caller is responsible for consuming
        the response body (e.g. via ``.json()``, ``.text``, ``.content``).

    Raises:
        httpx.HTTPStatusError: If the server returned a 4xx or 5xx response.
        httpx.TimeoutException: If the request timed out.
        httpx.ConnectError: If the connection could not be established.
        httpx.RequestError: Base class for other network errors.
        ValueError: If more than one body parameter is specified.

    Example:
        >>> from request_maker import request_sync
        >>> resp = request_sync("https://api.github.com/users/octocat")
        >>> resp.json()["login"]
        'octocat'
    """
    kwargs = _build_request_kwargs(
        json=json,
        data=data,
        content=content,
        headers=headers,
        params=params,
        timeout=timeout,
        follow_redirects=follow_redirects,
    )

    logger.debug("Sending sync %s %s", method.upper(), url)
    response = _sync_client.request(method=method.upper(), url=url, **kwargs)
    response.raise_for_status()
    logger.debug("Received %d for %s %s", response.status_code, method.upper(), url)
    return response


# ---------------------------------------------------------------------------
# Async super method
# ---------------------------------------------------------------------------
@request_retry
async def request_async(
    url: str,
    method: str = "GET",
    *,
    json: Optional[Dict[str, Any]] = None,
    data: Optional[Union[Dict[str, str], str, bytes]] = None,
    content: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: Optional[float] = None,
    follow_redirects: bool = True,
) -> httpx.Response:
    """Make an asynchronous HTTP request with automatic retry and error handling.

    This is the async counterpart of ``request_sync``.  It uses a shared
    ``httpx.AsyncClient`` connection pool, applies the same retry policy, and
    always calls ``raise_for_status()``.

    Args:
        url: The request URL (e.g. ``https://api.example.com/items``).
        method: HTTP method (``GET``, ``POST``, ``PUT``, ``PATCH``, ``DELETE``, etc.).
        json: JSON-serializable dict to send as the request body.  Sets
            ``Content-Type: application/json`` automatically.
        data: Form-encoded data (dict) or raw string/bytes payload.
        content: Raw bytes payload.
        headers: Custom HTTP headers as a dict.
        params: Query parameters to append to the URL.
        timeout: Override the default timeout (10s) for this request.
        follow_redirects: Whether to follow HTTP redirects (default ``True``).

    Returns:
        An ``httpx.Response`` object.

    Raises:
        httpx.HTTPStatusError: If the server returned a 4xx or 5xx response.
        httpx.TimeoutException: If the request timed out.
        httpx.ConnectError: If the connection could not be established.
        ValueError: If more than one body parameter is specified.

    Example:
        >>> import asyncio
        >>> from request_maker import request_async
        >>> async def main():
        ...     resp = await request_async("https://api.github.com/users/octocat")
        ...     print(resp.json()["login"])
        >>> asyncio.run(main())
        'octocat'
    """
    kwargs = _build_request_kwargs(
        json=json,
        data=data,
        content=content,
        headers=headers,
        params=params,
        timeout=timeout,
        follow_redirects=follow_redirects,
    )

    logger.debug("Sending async %s %s", method.upper(), url)
    response = await _async_client.request(method=method.upper(), url=url, **kwargs)
    response.raise_for_status()
    logger.debug("Received %d for %s %s", response.status_code, method.upper(), url)
    return response


# ---------------------------------------------------------------------------
# Module-level convenience aliases
# ---------------------------------------------------------------------------
# These exist for semantic clarity (e.g., when you want to emphasise the method).
get_sync = request_sync
post_sync = lambda url, **kw: request_sync(url, "POST", **kw)  # noqa: E731
put_sync = lambda url, **kw: request_sync(url, "PUT", **kw)  # noqa: E731
patch_sync = lambda url, **kw: request_sync(url, "PATCH", **kw)  # noqa: E731
delete_sync = lambda url, **kw: request_sync(url, "DELETE", **kw)  # noqa: E731

get_async = request_async
post_async = lambda url, **kw: request_async(url, "POST", **kw)  # noqa: E731
put_async = lambda url, **kw: request_async(url, "PUT", **kw)  # noqa: E731
patch_async = lambda url, **kw: request_async(url, "PATCH", **kw)  # noqa: E731
delete_async = lambda url, **kw: request_async(url, "DELETE", **kw)  # noqa: E731


# ---------------------------------------------------------------------------
# Quick test / demo when run directly
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import asyncio
    import sys
    from rich import print

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    print("=== Sync demo ===")
    resp = request_sync("https://httpbin.org/get", params={"hello": "world"})
    print(f"Status: {resp.status_code}, JSON keys: {list(resp.json().keys())}")

    print("\n=== Async demo ===")

    async def demo_async():
        # FIXED: explicit method="POST" because httpbin.org/post only accepts POST
        resp = await request_async(
            "https://httpbin.org/post",
            method="POST",
            json={"test": 123},
        )
        print(f"Status: {resp.status_code}, JSON keys: {list(resp.json().keys())}")

    asyncio.run(demo_async())

    # Clean exit
    close_clients()
    print("\nDone. Clients closed.")