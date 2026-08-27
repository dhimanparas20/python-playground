#!/usr/bin/env python3
"""request_maker.py — Production-grade HTTP client wrapper around httpx.

This module provides a small, opinionated façade over ``httpx`` that encodes
production best practices so callers don't have to remember them:

* **Bounded connection pooling** — explicit ``httpx.Limits``, shared per client.
* **Correct retry semantics** — only transient failures (429, 5xx, network
  errors) are retried; 4xx client errors fail fast.
* **Idempotency safety** — non-idempotent methods (POST/PATCH) are *not*
  retried by default, preventing duplicate writes on ambiguous timeouts.
* **Exponential backoff with jitter** — avoids the thundering-herd problem.
* **Total time budget** — retries are capped by both attempt count and wall
  clock, so a call can never block a worker indefinitely.
* **Safe redirect handling** — redirects are *not* followed by default (opt-in),
  reducing SSRF amplification and duplicate-write risk.
* **Correct async lifecycle** — the ``AsyncClient`` is created lazily so it
  binds to the caller's running event loop, not import-time state.
* **Observability** — structured logging including server error bodies, plus
  optional correlation-ID propagation.
* **Logging hygiene** — this module never mutates another logger's level.

Architecture
------------
``HttpClient`` is the real implementation. It is a context manager (sync and
async) and should be *injected* into your application for testability::

    client = HttpClient(base_url="https://api.example.com")

For scripts and simple call sites, module-level functions (``request_sync``,
``request_async``, ``get_sync``, ``post_async``, ...) delegate to a lazily
created process-wide default client.

Quick start
-----------
Synchronous, explicit lifecycle (recommended for scripts)::

    from request_maker import HttpClient

    with HttpClient(base_url="https://api.github.com") as client:
        resp = client.request("/users/octocat")
        print(resp.json()["login"])

Asynchronous, explicit lifecycle::

    async with HttpClient(base_url="https://api.github.com") as client:
        resp = await client.arequest("/users/octocat")

Module-level convenience (uses the shared default client)::

    from request_maker import get_sync, post_async, close_default_client

    resp = get_sync("https://api.example.com/items", params={"page": 1})
    resp = await post_async("https://api.example.com/items", json={"a": 1})

    close_default_client()  # at process shutdown

FastAPI integration
-------------------
Bind the client to the app lifespan so the pool is opened once and closed
cleanly on shutdown::

    from contextlib import asynccontextmanager
    from fastapi import FastAPI, Request
    from request_maker import HttpClient

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with HttpClient(base_url="https://upstream.internal") as client:
            app.state.http = client
            yield

    app = FastAPI(lifespan=lifespan)

    def get_http(request: Request) -> HttpClient:
        return request.app.state.http

Body parameters — which one do I use?
-------------------------------------
============  ==========================  ==================================
Parameter     Sends                       Content-Type set by httpx
============  ==========================  ==================================
``json``      dict/list -> JSON           ``application/json``
``data``      dict -> URL-encoded form    ``application/x-www-form-urlencoded``
``data``      str/bytes -> raw body       *(none — set it yourself)*
``content``   bytes/str/iterator          *(none — set it yourself)*
``files``     dict -> multipart           ``multipart/form-data``
============  ==========================  ==================================

Exactly one of ``json`` / ``data`` / ``content`` may be supplied.
``files`` may be combined with ``data`` (form fields alongside uploads).

Retry policy
------------
Retried:      429, 500, 502, 503, 504, connect errors, timeouts, protocol errors.
Not retried:  all other 4xx (fail fast — retrying won't help).
Methods:      idempotent methods only (GET/HEAD/OPTIONS/PUT/DELETE/TRACE) unless
              ``retry_non_idempotent=True`` is passed explicitly.
Backoff:      exponential with full jitter, capped at ``retry_max_wait``.
Stop:         whichever comes first — ``max_attempts`` or ``retry_max_delay``.
``Retry-After``: honoured when the server sends it on 429/503.

Installation
------------
    pip install httpx tenacity

Author  : Ken
Version : 2.0.0
License : MIT
"""

from __future__ import annotations

import asyncio
import logging
import random
import threading
import uuid
from types import TracebackType
from typing import (
    Any,
    Callable,
    Final,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    Type,
    Union,
)

import httpx
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential_jitter,
)

__all__ = [
    "HttpClient",
    "RequestError",
    "__version__",
    "aclose_default_client",
    "close_default_client",
    "delete_async",
    "delete_sync",
    "get_async",
    "get_default_client",
    "get_sync",
    "head_async",
    "head_sync",
    "patch_async",
    "patch_sync",
    "post_async",
    "post_sync",
    "put_async",
    "put_sync",
    "request_async",
    "request_sync",
    "set_default_client",
]

__version__: Final[str] = "2.0.0"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
# A library must never configure logging or mutate other loggers' levels.
# We attach a NullHandler so that "No handlers could be found" warnings are
# suppressed when the consuming application has not configured logging.
logger: Final[logging.Logger] = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------
#: Query parameters: mapping, sequence of pairs, raw query string, or httpx type.
QueryParams = Union[
    Mapping[str, Any],
    Sequence[tuple[str, Any]],
    str,
    bytes,
    httpx.QueryParams,
    None,
]

#: Headers: mapping or sequence of byte/str pairs.
HeaderTypes = Union[
    Mapping[str, str],
    Sequence[tuple[str, str]],
    httpx.Headers,
    None,
]

#: Form data (dict) or a raw string/bytes body.
FormData = Union[Mapping[str, Any], str, bytes, None]

#: Raw request content.
RawContent = Union[str, bytes, Iterable[bytes], None]

#: Multipart file uploads, e.g. ``{"file": ("name.txt", b"data", "text/plain")}``.
FileTypes = Union[Mapping[str, Any], Sequence[tuple[str, Any]], None]

#: httpx auth object or a ``(username, password)`` tuple.
AuthTypes = Union[httpx.Auth, tuple[str, str], None]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
#: HTTP methods that are safe to retry — repeating them has no extra effect.
IDEMPOTENT_METHODS: Final[frozenset[str]] = frozenset(
    {"GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE"}
)

#: Status codes that indicate a transient server-side condition.
RETRYABLE_STATUS_CODES: Final[frozenset[int]] = frozenset(
    {
        408,  # Request Timeout
        425,  # Too Early
        429,  # Too Many Requests
        500,  # Internal Server Error
        502,  # Bad Gateway
        503,  # Service Unavailable
        504,  # Gateway Timeout
    }
)

#: Network-level exceptions that are always worth retrying.
RETRYABLE_NETWORK_EXCEPTIONS: Final[tuple[type[Exception], ...]] = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    httpx.RemoteProtocolError,
    httpx.ReadError,
    httpx.WriteError,
)

#: Header names whose values must never appear in logs.
_SENSITIVE_HEADERS: Final[frozenset[str]] = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "x-api-key",
        "x-auth-token",
        "api-key",
    }
)

#: Maximum number of characters of an error response body to log.
_ERROR_BODY_LOG_LIMIT: Final[int] = 2048

# Sensible production defaults.
_DEFAULT_TIMEOUT: Final[httpx.Timeout] = httpx.Timeout(
    connect=5.0,  # TCP + TLS handshake
    read=15.0,  # waiting for response bytes
    write=10.0,  # sending the request body
    pool=5.0,  # waiting for a free connection from the pool
)

_DEFAULT_LIMITS: Final[httpx.Limits] = httpx.Limits(
    max_connections=100,
    max_keepalive_connections=20,
    keepalive_expiry=30.0,
)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------
class RequestError(RuntimeError):
    """Raised for wrapper-level misuse (not for HTTP or network failures).

    HTTP and transport failures are surfaced as the original ``httpx``
    exceptions (``httpx.HTTPStatusError``, ``httpx.TimeoutException``, ...) so
    that callers can handle them with standard ``httpx`` error handling.
    """


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _redact_headers(headers: HeaderTypes) -> dict[str, str]:
    """Return a copy of ``headers`` with sensitive values replaced.

    Args:
        headers: Headers to redact. May be ``None``.

    Returns:
        A plain dict where any sensitive header value is replaced with
        ``"<redacted>"``. Returns an empty dict when ``headers`` is ``None``.
    """
    if not headers:
        return {}
    items = headers.items() if hasattr(headers, "items") else headers
    return {
        str(k): ("<redacted>" if str(k).lower() in _SENSITIVE_HEADERS else str(v))
        for k, v in items  # type: ignore[union-attr]
    }


def _truncate(text: str, limit: int = _ERROR_BODY_LOG_LIMIT) -> str:
    """Truncate ``text`` to ``limit`` characters with an explicit marker.

    Args:
        text: The string to truncate.
        limit: Maximum number of characters to keep.

    Returns:
        The original string, or a truncated version annotated with the number
        of omitted characters.
    """
    if len(text) <= limit:
        return text
    return f"{text[:limit]}... [{len(text) - limit} more chars truncated]"


def _safe_error_body(response: httpx.Response) -> str:
    """Extract a loggable error body from a response without raising.

    Reading the body of a streaming response that has not been consumed raises
    ``httpx.ResponseNotRead``; this helper degrades gracefully instead.

    Args:
        response: The response whose body should be summarised.

    Returns:
        A truncated body string, or a placeholder if the body is unavailable.
    """
    try:
        return _truncate(response.text)
    except Exception:  # noqa: BLE001 - logging must never raise
        return "<body unavailable (streamed or undecodable)>"


def _parse_retry_after(response: httpx.Response) -> Optional[float]:
    """Parse the ``Retry-After`` header into seconds.

    Only the delta-seconds form is honoured. HTTP-date values are ignored to
    avoid clock-skew issues between client and server.

    Args:
        response: Response that may carry a ``Retry-After`` header.

    Returns:
        Number of seconds to wait, or ``None`` if absent/unparseable.
    """
    raw = response.headers.get("retry-after")
    if not raw:
        return None
    try:
        seconds = float(raw.strip())
    except (TypeError, ValueError):
        return None
    return seconds if seconds >= 0 else None


def _is_retryable_exception(exc: BaseException) -> bool:
    """Decide whether an exception represents a transient failure.

    Args:
        exc: The exception raised by the request attempt.

    Returns:
        ``True`` if the failure is transient and worth retrying, else ``False``.
        Notably, 4xx responses other than 408/425/429 return ``False`` so that
        client errors fail fast.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in RETRYABLE_STATUS_CODES
    return isinstance(exc, RETRYABLE_NETWORK_EXCEPTIONS)


def _log_retry(retry_state: RetryCallState) -> None:
    """Log a structured warning before each retry sleep.

    Args:
        retry_state: Tenacity state describing the failed attempt.
    """
    outcome = retry_state.outcome
    if outcome is None:
        return
    exc = outcome.exception()
    sleep_for = getattr(retry_state.next_action, "sleep", 0.0)

    if isinstance(exc, httpx.HTTPStatusError):
        reason = (
            f"HTTP {exc.response.status_code} "
            f"body={_safe_error_body(exc.response)!r}"
        )
    else:
        reason = f"{type(exc).__name__}: {exc}"

    logger.warning(
        "Retrying request (attempt %d) in %.2fs — %s",
        retry_state.attempt_number,
        sleep_for,
        reason,
    )


def _build_retry_wait(
    initial: float, maximum: float
) -> Callable[[RetryCallState], float]:
    """Build a wait strategy that prefers ``Retry-After`` over backoff.

    Args:
        initial: Base delay in seconds for exponential backoff.
        maximum: Upper bound on any computed delay, in seconds.

    Returns:
        A tenacity-compatible callable returning the seconds to sleep.
    """
    backoff = wait_exponential_jitter(initial=initial, max=maximum, jitter=initial)

    def _wait(retry_state: RetryCallState) -> float:
        outcome = retry_state.outcome
        if outcome is not None:
            exc = outcome.exception()
            if isinstance(exc, httpx.HTTPStatusError):
                retry_after = _parse_retry_after(exc.response)
                if retry_after is not None:
                    # Honour the server's instruction, but never exceed our cap
                    # and add a little jitter to de-synchronise clients.
                    return min(retry_after, maximum) + random.uniform(0, 0.5)
        return float(backoff(retry_state))

    return _wait


# ---------------------------------------------------------------------------
# HttpClient
# ---------------------------------------------------------------------------
class HttpClient:
    """A production-ready HTTP client with retries, pooling and observability.

    A single instance owns one sync connection pool and (lazily) one async
    connection pool. Instances are thread-safe for concurrent sync requests and
    coroutine-safe for concurrent async requests.

    The async transport is created on first async use so that it binds to the
    caller's running event loop. This makes the class safe to instantiate at
    import time, inside tests that create fresh loops, and inside frameworks
    that manage their own loop lifecycle.

    Attributes:
        base_url: Base URL prefixed to relative request paths.
        max_attempts: Maximum number of attempts per request (1 disables retries).

    Example:
        >>> with HttpClient(base_url="https://api.github.com") as client:
        ...     resp = client.request("/users/octocat")
        ...     resp.json()["login"]
        'octocat'
    """

    def __init__(
        self,
        *,
        base_url: str = "",
        headers: HeaderTypes = None,
        auth: AuthTypes = None,
        timeout: Union[httpx.Timeout, float, None] = None,
        limits: Optional[httpx.Limits] = None,
        max_attempts: int = 3,
        retry_initial_wait: float = 0.5,
        retry_max_wait: float = 10.0,
        retry_max_delay: float = 60.0,
        follow_redirects: bool = False,
        max_redirects: int = 5,
        http2: bool = False,
        verify: Union[bool, str] = True,
        trust_env: bool = True,
        user_agent: Optional[str] = None,
        correlation_header: Optional[str] = "X-Request-ID",
    ) -> None:
        """Initialise the client and its sync connection pool.

        Args:
            base_url: Base URL joined with relative request URLs. Leave empty to
                require absolute URLs at every call site.
            headers: Default headers applied to every request. Per-request
                headers are merged on top of these.
            auth: Default authentication — an ``httpx.Auth`` instance or a
                ``(username, password)`` tuple for HTTP Basic.
            timeout: Timeout applied to all requests. Accepts an
                ``httpx.Timeout`` for fine-grained control or a float for a
                uniform timeout. Defaults to 5s connect / 15s read / 10s write
                / 5s pool.
            limits: Connection pool limits. Defaults to 100 max connections and
                20 keepalive connections with a 30s keepalive expiry.
            max_attempts: Total attempts per request, including the first.
                Must be >= 1; a value of 1 disables retries entirely.
            retry_initial_wait: Base backoff delay in seconds.
            retry_max_wait: Maximum delay between two attempts, in seconds.
            retry_max_delay: Total wall-clock budget for all retries of a single
                request, in seconds. Prevents unbounded worker blocking.
            follow_redirects: Whether to follow redirects by default. ``False``
                is the secure default; enable per request when needed.
            max_redirects: Maximum redirect chain length when following.
            http2: Enable HTTP/2. Requires ``pip install httpx[http2]``.
            verify: TLS verification — ``True``, ``False`` (never in
                production), or a path to a CA bundle.
            trust_env: Honour ``HTTP_PROXY``/``HTTPS_PROXY``/``NO_PROXY`` and
                ``SSL_CERT_FILE`` environment variables.
            user_agent: Value for the ``User-Agent`` header. Defaults to
                ``request_maker/<version>``.
            correlation_header: Header used to propagate a correlation ID. A
                UUID4 is generated per request when the caller does not supply
                one. Pass ``None`` to disable.

        Raises:
            ValueError: If ``max_attempts`` is less than 1, or if any retry
                timing parameter is negative.
        """
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if min(retry_initial_wait, retry_max_wait, retry_max_delay) < 0:
            raise ValueError("retry timing parameters must be non-negative")

        self.base_url = base_url
        self.max_attempts = max_attempts

        self._retry_initial_wait = retry_initial_wait
        self._retry_max_wait = retry_max_wait
        self._retry_max_delay = retry_max_delay
        self._default_follow_redirects = follow_redirects
        self._correlation_header = correlation_header

        resolved_timeout = (
            _DEFAULT_TIMEOUT
            if timeout is None
            else (timeout if isinstance(timeout, httpx.Timeout) else httpx.Timeout(timeout))
        )

        default_headers = httpx.Headers({"Accept": "application/json"})
        default_headers["User-Agent"] = user_agent or f"request_maker/{__version__}"
        if headers:
            default_headers.update(headers)

        # Shared kwargs guarantee identical behaviour across sync and async.
        self._client_kwargs: dict[str, Any] = {
            "base_url": base_url,
            "headers": default_headers,
            "auth": auth,
            "timeout": resolved_timeout,
            "limits": limits or _DEFAULT_LIMITS,
            "follow_redirects": follow_redirects,
            "max_redirects": max_redirects,
            "http2": http2,
            "verify": verify,
            "trust_env": trust_env,
        }

        self._sync_client: httpx.Client = httpx.Client(**self._client_kwargs)

        # Created lazily so it binds to the caller's running event loop.
        self._async_client: Optional[httpx.AsyncClient] = None
        self._async_lock = asyncio.Lock()
        self._sync_lock = threading.Lock()
        self._closed = False

    # -- properties --------------------------------------------------------
    @property
    def is_closed(self) -> bool:
        """Whether :meth:`close` or :meth:`aclose` has been called."""
        return self._closed

    @property
    def sync_client(self) -> httpx.Client:
        """The underlying ``httpx.Client``, for advanced/streaming use.

        Returns:
            The live sync client instance.

        Raises:
            RequestError: If this client has been closed.
        """
        if self._closed:
            raise RequestError("HttpClient has been closed")
        return self._sync_client

    async def async_client(self) -> httpx.AsyncClient:
        """Return the underlying ``httpx.AsyncClient``, creating it if needed.

        The client is created on first call so that it binds to the currently
        running event loop. Creation is guarded by a lock to remain safe under
        concurrent coroutines.

        Returns:
            The live async client instance.

        Raises:
            RequestError: If this client has been closed.
        """
        if self._closed:
            raise RequestError("HttpClient has been closed")
        if self._async_client is None or self._async_client.is_closed:
            async with self._async_lock:
                if self._async_client is None or self._async_client.is_closed:
                    self._async_client = httpx.AsyncClient(**self._client_kwargs)
        return self._async_client

    # -- request building --------------------------------------------------
    def _prepare(
        self,
        method: str,
        url: str,
        *,
        json: Any,
        data: FormData,
        content: RawContent,
        files: FileTypes,
        headers: HeaderTypes,
        params: QueryParams,
        auth: AuthTypes,
        timeout: Union[httpx.Timeout, float, None],
        follow_redirects: Optional[bool],
        correlation_id: Optional[str],
    ) -> tuple[str, dict[str, Any], str]:
        """Validate inputs and assemble kwargs for ``httpx.Client.request``.

        Args:
            method: HTTP method, case-insensitive.
            url: Absolute URL, or a path relative to ``base_url``.
            json: JSON-serialisable body.
            data: Form fields or a raw string/bytes body.
            content: Raw request content.
            files: Multipart file uploads.
            headers: Per-request headers.
            params: Query parameters.
            auth: Per-request authentication override.
            timeout: Per-request timeout override.
            follow_redirects: Per-request redirect override.
            correlation_id: Explicit correlation ID; generated when ``None``.

        Returns:
            A ``(method, kwargs, correlation_id)`` tuple where ``method`` is
            upper-cased and ``kwargs`` is ready to splat into httpx.

        Raises:
            RequestError: If the client is closed, the method is empty, or more
                than one mutually exclusive body parameter is supplied.
        """
        if self._closed:
            raise RequestError("HttpClient has been closed")

        method_upper = method.strip().upper()
        if not method_upper:
            raise RequestError("HTTP method must be a non-empty string")

        supplied_bodies = [
            name
            for name, value in (("json", json), ("data", data), ("content", content))
            if value is not None
        ]
        if len(supplied_bodies) > 1:
            raise RequestError(
                "Only one of 'json', 'data' or 'content' may be supplied; "
                f"got {supplied_bodies}. Use 'files' with 'data' for multipart."
            )

        request_headers = httpx.Headers(headers) if headers else httpx.Headers()

        cid = correlation_id
        if self._correlation_header:
            existing = request_headers.get(self._correlation_header)
            cid = cid or existing or str(uuid.uuid4())
            request_headers[self._correlation_header] = cid
        cid = cid or "-"

        kwargs: dict[str, Any] = {
            "url": url,
            "follow_redirects": (
                self._default_follow_redirects
                if follow_redirects is None
                else follow_redirects
            ),
        }
        if request_headers:
            kwargs["headers"] = request_headers
        if params is not None:
            kwargs["params"] = params
        if json is not None:
            kwargs["json"] = json
        elif data is not None:
            kwargs["data"] = data
        elif content is not None:
            kwargs["content"] = content
        if files is not None:
            kwargs["files"] = files
        if auth is not None:
            kwargs["auth"] = auth
        if timeout is not None:
            kwargs["timeout"] = timeout

        return method_upper, kwargs, cid

    def _retry_controller(
        self,
        method: str,
        *,
        max_attempts: Optional[int],
        retry_non_idempotent: bool,
        retry_on_status: Optional[Iterable[int]],
    ) -> tuple[int, Callable[[BaseException], bool]]:
        """Resolve the effective attempt count and retry predicate.

        Non-idempotent methods are not retried unless explicitly allowed,
        because a timed-out POST may already have been applied server-side.

        Args:
            method: Upper-cased HTTP method.
            max_attempts: Per-request override of the client default.
            retry_non_idempotent: Allow retrying POST/PATCH.
            retry_on_status: Override the retryable status-code set.

        Returns:
            A ``(attempts, predicate)`` tuple. ``attempts`` is 1 when retries
            are disabled for this call.
        """
        attempts = self.max_attempts if max_attempts is None else max(1, max_attempts)

        if method not in IDEMPOTENT_METHODS and not retry_non_idempotent:
            attempts = 1

        if retry_on_status is None:
            return attempts, _is_retryable_exception

        codes = frozenset(retry_on_status)

        def predicate(exc: BaseException) -> bool:
            if isinstance(exc, httpx.HTTPStatusError):
                return exc.response.status_code in codes
            return isinstance(exc, RETRYABLE_NETWORK_EXCEPTIONS)

        return attempts, predicate

    def _log_failure(
        self, exc: BaseException, method: str, url: str, cid: str
    ) -> None:
        """Emit a final, structured log entry for a failed request.

        Args:
            exc: The exception that terminated the request.
            method: Upper-cased HTTP method.
            url: Request URL.
            cid: Correlation ID for cross-service tracing.
        """
        if isinstance(exc, httpx.HTTPStatusError):
            response = exc.response
            level = logging.ERROR if response.status_code >= 500 else logging.WARNING
            logger.log(
                level,
                "%s %s failed: HTTP %d (cid=%s) body=%s",
                method,
                url,
                response.status_code,
                cid,
                _safe_error_body(response),
            )
        elif isinstance(exc, httpx.TimeoutException):
            logger.error("%s %s timed out (cid=%s): %s", method, url, cid, exc)
        else:
            logger.error(
                "%s %s failed (cid=%s): %s: %s",
                method,
                url,
                cid,
                type(exc).__name__,
                exc,
            )

    # -- sync API ----------------------------------------------------------
    def request(
        self,
        url: str,
        method: str = "GET",
        *,
        json: Any = None,
        data: FormData = None,
        content: RawContent = None,
        files: FileTypes = None,
        headers: HeaderTypes = None,
        params: QueryParams = None,
        auth: AuthTypes = None,
        timeout: Union[httpx.Timeout, float, None] = None,
        follow_redirects: Optional[bool] = None,
        raise_for_status: bool = True,
        max_attempts: Optional[int] = None,
        retry_non_idempotent: bool = False,
        retry_on_status: Optional[Iterable[int]] = None,
        correlation_id: Optional[str] = None,
    ) -> httpx.Response:
        """Perform a synchronous HTTP request with retries and error handling.

        Args:
            url: Absolute URL, or a path relative to the client ``base_url``.
            method: HTTP method — ``GET``, ``POST``, ``PUT``, ``PATCH``,
                ``DELETE``, ``HEAD``, ``OPTIONS``. Case-insensitive.
            json: JSON-serialisable body. Sets ``Content-Type: application/json``.
            data: Form fields (mapping) or a raw ``str``/``bytes`` body.
            content: Raw bytes, string, or byte-iterator body.
            files: Multipart uploads, e.g.
                ``{"file": ("report.csv", handle, "text/csv")}``. May be
                combined with ``data`` for additional form fields.
            headers: Per-request headers, merged over the client defaults.
            params: Query parameters as a mapping, sequence of pairs, or raw
                query string.
            auth: Per-request authentication override.
            timeout: Per-request timeout override in seconds or as an
                ``httpx.Timeout``.
            follow_redirects: Per-request redirect override. Defaults to the
                client setting (``False`` unless configured otherwise).
            raise_for_status: Raise ``httpx.HTTPStatusError`` on 4xx/5xx. Set to
                ``False`` to inspect error responses yourself — note that
                retries are driven by this exception, so disabling it also
                disables status-based retries.
            max_attempts: Per-request override of the client attempt count.
            retry_non_idempotent: Permit retrying POST/PATCH. Only enable when
                the endpoint is idempotent or protected by an idempotency key.
            retry_on_status: Override which status codes are retryable.
            correlation_id: Explicit correlation ID to propagate; generated
                automatically when omitted.

        Returns:
            The ``httpx.Response``. For non-streaming requests the body is
            already read and may be accessed via ``.json()``, ``.text`` or
            ``.content``.

        Raises:
            RequestError: On invalid arguments or use of a closed client.
            httpx.HTTPStatusError: On a 4xx/5xx response when
                ``raise_for_status`` is ``True``.
            httpx.TimeoutException: If the request exceeded its timeout budget.
            httpx.ConnectError: If the connection could not be established.
            httpx.RequestError: For any other transport-level failure.

        Example:
            >>> client = HttpClient()
            >>> resp = client.request(
            ...     "https://httpbin.org/post",
            ...     method="POST",
            ...     json={"hello": "world"},
            ... )
            >>> resp.status_code
            200
        """
        method_upper, kwargs, cid = self._prepare(
            method,
            url,
            json=json,
            data=data,
            content=content,
            files=files,
            headers=headers,
            params=params,
            auth=auth,
            timeout=timeout,
            follow_redirects=follow_redirects,
            correlation_id=correlation_id,
        )
        attempts, predicate = self._retry_controller(
            method_upper,
            max_attempts=max_attempts,
            retry_non_idempotent=retry_non_idempotent,
            retry_on_status=retry_on_status,
        )

        retrying = Retrying(
            stop=(stop_after_attempt(attempts) | stop_after_delay(self._retry_max_delay)),
            wait=_build_retry_wait(self._retry_initial_wait, self._retry_max_wait),
            retry=retry_if_exception(predicate),
            before_sleep=_log_retry,
            reraise=True,
        )

        try:
            for attempt in retrying:
                with attempt:
                    logger.debug(
                        "-> %s %s (cid=%s, attempt=%d, headers=%s)",
                        method_upper,
                        url,
                        cid,
                        attempt.retry_state.attempt_number,
                        _redact_headers(kwargs.get("headers")),
                    )
                    response = self._sync_client.request(method_upper, **kwargs)
                    if raise_for_status:
                        response.raise_for_status()
                    logger.debug(
                        "<- %s %s %d in %.3fs (cid=%s)",
                        method_upper,
                        url,
                        response.status_code,
                        response.elapsed.total_seconds(),
                        cid,
                    )
                    return response
        except BaseException as exc:  # noqa: BLE001 - log, then re-raise as-is
            self._log_failure(exc, method_upper, url, cid)
            raise

        # Unreachable: tenacity either returns a value or raises.
        raise RequestError("retry loop exited without a result")  # pragma: no cover

    # -- async API ---------------------------------------------------------
    async def arequest(
        self,
        url: str,
        method: str = "GET",
        *,
        json: Any = None,
        data: FormData = None,
        content: RawContent = None,
        files: FileTypes = None,
        headers: HeaderTypes = None,
        params: QueryParams = None,
        auth: AuthTypes = None,
        timeout: Union[httpx.Timeout, float, None] = None,
        follow_redirects: Optional[bool] = None,
        raise_for_status: bool = True,
        max_attempts: Optional[int] = None,
        retry_non_idempotent: bool = False,
        retry_on_status: Optional[Iterable[int]] = None,
        correlation_id: Optional[str] = None,
    ) -> httpx.Response:
        """Perform an asynchronous HTTP request with retries and error handling.

        The async counterpart of :meth:`request`, with identical parameters and
        semantics. Backoff sleeps use ``asyncio.sleep``, so the event loop is
        never blocked.

        Args:
            url: Absolute URL, or a path relative to the client ``base_url``.
            method: HTTP method. Case-insensitive.
            json: JSON-serialisable body.
            data: Form fields (mapping) or a raw ``str``/``bytes`` body.
            content: Raw bytes, string, or byte-iterator body.
            files: Multipart uploads; may be combined with ``data``.
            headers: Per-request headers, merged over the client defaults.
            params: Query parameters.
            auth: Per-request authentication override.
            timeout: Per-request timeout override.
            follow_redirects: Per-request redirect override.
            raise_for_status: Raise ``httpx.HTTPStatusError`` on 4xx/5xx.
            max_attempts: Per-request override of the client attempt count.
            retry_non_idempotent: Permit retrying POST/PATCH.
            retry_on_status: Override which status codes are retryable.
            correlation_id: Explicit correlation ID to propagate.

        Returns:
            The ``httpx.Response``.

        Raises:
            RequestError: On invalid arguments or use of a closed client.
            httpx.HTTPStatusError: On a 4xx/5xx response when
                ``raise_for_status`` is ``True``.
            httpx.TimeoutException: If the request exceeded its timeout budget.
            httpx.ConnectError: If the connection could not be established.
            httpx.RequestError: For any other transport-level failure.

        Example:
            >>> import asyncio
            >>> async def main() -> int:
            ...     async with HttpClient() as client:
            ...         resp = await client.arequest("https://httpbin.org/get")
            ...         return resp.status_code
            >>> asyncio.run(main())
            200
        """
        method_upper, kwargs, cid = self._prepare(
            method,
            url,
            json=json,
            data=data,
            content=content,
            files=files,
            headers=headers,
            params=params,
            auth=auth,
            timeout=timeout,
            follow_redirects=follow_redirects,
            correlation_id=correlation_id,
        )
        attempts, predicate = self._retry_controller(
            method_upper,
            max_attempts=max_attempts,
            retry_non_idempotent=retry_non_idempotent,
            retry_on_status=retry_on_status,
        )
        client = await self.async_client()

        retrying = AsyncRetrying(
            stop=(stop_after_attempt(attempts) | stop_after_delay(self._retry_max_delay)),
            wait=_build_retry_wait(self._retry_initial_wait, self._retry_max_wait),
            retry=retry_if_exception(predicate),
            before_sleep=_log_retry,
            reraise=True,
        )

        try:
            async for attempt in retrying:
                with attempt:
                    logger.debug(
                        "-> %s %s (cid=%s, attempt=%d, headers=%s)",
                        method_upper,
                        url,
                        cid,
                        attempt.retry_state.attempt_number,
                        _redact_headers(kwargs.get("headers")),
                    )
                    response = await client.request(method_upper, **kwargs)
                    if raise_for_status:
                        response.raise_for_status()
                    logger.debug(
                        "<- %s %s %d in %.3fs (cid=%s)",
                        method_upper,
                        url,
                        response.status_code,
                        response.elapsed.total_seconds(),
                        cid,
                    )
                    return response
        except BaseException as exc:  # noqa: BLE001 - log, then re-raise as-is
            self._log_failure(exc, method_upper, url, cid)
            raise

        raise RequestError("retry loop exited without a result")  # pragma: no cover

    # -- verb shortcuts (sync) --------------------------------------------
    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        """Issue a ``GET`` request. See :meth:`request` for keyword arguments."""
        return self.request(url, "GET", **kwargs)

    def head(self, url: str, **kwargs: Any) -> httpx.Response:
        """Issue a ``HEAD`` request. See :meth:`request` for keyword arguments."""
        return self.request(url, "HEAD", **kwargs)

    def post(self, url: str, **kwargs: Any) -> httpx.Response:
        """Issue a ``POST`` request. Not retried unless ``retry_non_idempotent``."""
        return self.request(url, "POST", **kwargs)

    def put(self, url: str, **kwargs: Any) -> httpx.Response:
        """Issue a ``PUT`` request. See :meth:`request` for keyword arguments."""
        return self.request(url, "PUT", **kwargs)

    def patch(self, url: str, **kwargs: Any) -> httpx.Response:
        """Issue a ``PATCH`` request. Not retried unless ``retry_non_idempotent``."""
        return self.request(url, "PATCH", **kwargs)

    def delete(self, url: str, **kwargs: Any) -> httpx.Response:
        """Issue a ``DELETE`` request. See :meth:`request` for keyword arguments."""
        return self.request(url, "DELETE", **kwargs)

    # -- verb shortcuts (async) -------------------------------------------
    async def aget(self, url: str, **kwargs: Any) -> httpx.Response:
        """Issue an async ``GET``. See :meth:`arequest` for keyword arguments."""
        return await self.arequest(url, "GET", **kwargs)

    async def ahead(self, url: str, **kwargs: Any) -> httpx.Response:
        """Issue an async ``HEAD``. See :meth:`arequest` for keyword arguments."""
        return await self.arequest(url, "HEAD", **kwargs)

    async def apost(self, url: str, **kwargs: Any) -> httpx.Response:
        """Issue an async ``POST``. Not retried unless ``retry_non_idempotent``."""
        return await self.arequest(url, "POST", **kwargs)

    async def aput(self, url: str, **kwargs: Any) -> httpx.Response:
        """Issue an async ``PUT``. See :meth:`arequest` for keyword arguments."""
        return await self.arequest(url, "PUT", **kwargs)

    async def apatch(self, url: str, **kwargs: Any) -> httpx.Response:
        """Issue an async ``PATCH``. Not retried unless ``retry_non_idempotent``."""
        return await self.arequest(url, "PATCH", **kwargs)

    async def adelete(self, url: str, **kwargs: Any) -> httpx.Response:
        """Issue an async ``DELETE``. See :meth:`arequest` for keyword arguments."""
        return await self.arequest(url, "DELETE", **kwargs)

    # -- lifecycle ---------------------------------------------------------
    async def aclose(self) -> None:
        """Close both the async and sync connection pools. Idempotent.

        Prefer this over :meth:`close` in async applications — call it from a
        FastAPI/Starlette lifespan shutdown hook or the tail of your coroutine.
        """
        if self._async_client is not None and not self._async_client.is_closed:
            await self._async_client.aclose()
        self._async_client = None

        if not self._sync_client.is_closed:
            self._sync_client.close()

        self._closed = True
        logger.debug("HttpClient sync + async pools closed")

    # -- context managers --------------------------------------------------
    def __enter__(self) -> "HttpClient":
        """Enter a synchronous context manager.

        Returns:
            This client instance.
        """
        return self

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc: Optional[BaseException],
            tb: Optional[TracebackType],
    ) -> None:
        """Exit the synchronous context manager, closing the sync pool."""
        self.aclose()

    async def __aenter__(self) -> "HttpClient":
        """Enter an asynchronous context manager, warming the async pool.

        Returns:
            This client instance.
        """
        await self.async_client()
        return self

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc: Optional[BaseException],
            tb: Optional[TracebackType],
    ) -> None:
        """Exit the asynchronous context manager, closing both pools."""
        await self.aclose()

    def __repr__(self) -> str:
        """Return a debug representation of the client."""
        return (
            f"HttpClient(base_url={self.base_url!r}, "
            f"max_attempts={self.max_attempts}, closed={self._closed})"
        )

# ---------------------------------------------------------------------------
# Process-wide default client
# ---------------------------------------------------------------------------
# Convenience for scripts and simple call sites. Long-lived services should
# instantiate and inject their own HttpClient instead of relying on this.
_default_client: Optional[HttpClient] = None
_default_lock: Final[threading.Lock] = threading.Lock()

def get_default_client() -> HttpClient:
    """Return the process-wide default client, creating it on first use.

    Thread-safe and lazy: no connection pool exists until the first request.

    Returns:
        The shared :class:`HttpClient` instance.
    """
    global _default_client
    if _default_client is None or _default_client.is_closed:
        with _default_lock:
            if _default_client is None or _default_client.is_closed:
                _default_client = HttpClient()
    return _default_client

def set_default_client(client: HttpClient) -> None:
    """Replace the process-wide default client.

    Useful for injecting a pre-configured or mocked client in tests.

    Args:
        client: The client to install as the new default.

    Raises:
        TypeError: If ``client`` is not an :class:`HttpClient`.
    """
    global _default_client
    if not isinstance(client, HttpClient):
        raise TypeError(f"expected HttpClient, got {type(client).__name__}")
    with _default_lock:
        _default_client = client

def close_default_client() -> None:
    """Close the default client's sync pool. Safe to call when none exists."""
    global _default_client
    if _default_client is not None:
        _default_client.close()
        _default_client = None

async def aclose_default_client() -> None:
    """Close the default client's async and sync pools. Await at shutdown."""
    global _default_client
    if _default_client is not None:
        await _default_client.aclose()
        _default_client = None

# ---------------------------------------------------------------------------
# Module-level convenience functions
# ---------------------------------------------------------------------------
def request_sync(url: str, method: str = "GET", **kwargs: Any) -> httpx.Response:
    """Perform a sync request using the default client.

    Args:
        url: Absolute request URL.
        method: HTTP method. Case-insensitive.
        **kwargs: Forwarded to :meth:`HttpClient.request`.

    Returns:
        The ``httpx.Response``.
    """
    return get_default_client().request(url, method, **kwargs)

async def request_async(url: str, method: str = "GET", **kwargs: Any) -> httpx.Response:
    """Perform an async request using the default client.

    Args:
        url: Absolute request URL.
        method: HTTP method. Case-insensitive.
        **kwargs: Forwarded to :meth:`HttpClient.arequest`.

    Returns:
        The ``httpx.Response``.
    """
    return await get_default_client().arequest(url, method, **kwargs)

def get_sync(url: str, **kwargs: Any) -> httpx.Response:
    """Sync ``GET`` via the default client. See :meth:`HttpClient.request`."""
    return request_sync(url, "GET", **kwargs)

def head_sync(url: str, **kwargs: Any) -> httpx.Response:
    """Sync ``HEAD`` via the default client. See :meth:`HttpClient.request`."""
    return request_sync(url, "HEAD", **kwargs)

def post_sync(url: str, **kwargs: Any) -> httpx.Response:
    """Sync ``POST`` via the default client. Not retried by default."""
    return request_sync(url, "POST", **kwargs)

def put_sync(url: str, **kwargs: Any) -> httpx.Response:
    """Sync ``PUT`` via the default client. See :meth:`HttpClient.request`."""
    return request_sync(url, "PUT", **kwargs)

def patch_sync(url: str, **kwargs: Any) -> httpx.Response:
    """Sync ``PATCH`` via the default client. Not retried by default."""
    return request_sync(url, "PATCH", **kwargs)

def delete_sync(url: str, **kwargs: Any) -> httpx.Response:
    """Sync ``DELETE`` via the default client. See :meth:`HttpClient.request`."""
    return request_sync(url, "DELETE", **kwargs)

async def get_async(url: str, **kwargs: Any) -> httpx.Response:
    """Async ``GET`` via the default client. See :meth:`HttpClient.arequest`."""
    return await request_async(url, "GET", **kwargs)

async def head_async(url: str, **kwargs: Any) -> httpx.Response:
    """Async ``HEAD`` via the default client. See :meth:`HttpClient.arequest`."""
    return await request_async(url, "HEAD", **kwargs)

async def post_async(url: str, **kwargs: Any) -> httpx.Response:
    """Async ``POST`` via the default client. Not retried by default."""
    return await request_async(url, "POST", **kwargs)

async def put_async(url: str, **kwargs: Any) -> httpx.Response:
    """Async ``PUT`` via the default client. See :meth:`HttpClient.arequest`."""
    return await request_async(url, "PUT", **kwargs)

async def patch_async(url: str, **kwargs: Any) -> httpx.Response:
    """Async ``PATCH`` via the default client. Not retried by default."""
    return await request_async(url, "PATCH", **kwargs)

async def delete_async(url: str, **kwargs: Any) -> httpx.Response:
    """Async ``DELETE`` via the default client. See :meth:`HttpClient.arequest`."""
    return await request_async(url, "DELETE", **kwargs)

# ---------------------------------------------------------------------------
# Self-test / live demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    from rich import print

    # Application-level logging config. Note this lives INSIDE __main__ —
    # importing this module never touches your logging setup.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    # Silence third-party transport chatter for this demo only.
    for noisy in ("httpx", "httpcore", "hpack", "h2", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    BASE = "https://httpbin.org"

    def hr(title: str) -> None:
        """Print a section header.

        Args:
            title: Section title to display.
        """
        print(f"\n{'=' * 68}\n  {title}\n{'=' * 68}")

    # -- 1. Sync: GET with query params ---------------------------------
    hr("1. Sync GET with query params")
    with HttpClient(base_url=BASE) as client:
        resp = client.get("/get", params={"hello": "world", "page": 2})
        print(f"status : {resp.status_code}")
        print(f"args   : {resp.json()['args']}")

        # -- 2. Sync: POST with JSON body -------------------------------
        hr("2. Sync POST with JSON body")
        resp = client.post("/post", json={"name": "Ken", "role": "devops"})
        print(f"status : {resp.status_code}")
        print(f"echoed : {resp.json()['json']}")

        # -- 3. Sync: POST with form data -------------------------------
        hr("3. Sync POST with form-encoded data")
        resp = client.post("/post", data={"username": "ken", "team": "platform"})
        print(f"status : {resp.status_code}")
        print(f"form   : {resp.json()['form']}")

        # -- 4. Sync: PUT / PATCH / DELETE ------------------------------
        hr("4. Sync PUT / PATCH / DELETE")
        print(f"PUT    : {client.put('/put', json={'v': 1}).status_code}")
        print(f"PATCH  : {client.patch('/patch', json={'v': 2}).status_code}")
        print(f"DELETE : {client.delete('/delete').status_code}")

        # -- 5. Custom headers + correlation ID -------------------------
        hr("5. Custom headers and correlation ID")
        resp = client.get(
            "/headers",
            headers={"Authorization": "Bearer super-secret", "X-Tenant": "acme"},
            correlation_id="demo-cid-12345",
        )
        echoed = resp.json()["headers"]
        print(f"X-Request-Id : {echoed.get('X-Request-Id')}")
        print(f"X-Tenant     : {echoed.get('X-Tenant')}")
        print("(the Authorization value was redacted in debug logs)")

        # -- 6. Multipart file upload -----------------------------------
        hr("6. Multipart file upload")
        resp = client.post(
            "/post",
            files={"report": ("metrics.csv", b"cpu,mem\n0.8,0.6\n", "text/csv")},
            data={"env": "production"},
        )
        payload = resp.json()
        print(f"status : {resp.status_code}")
        print(f"files  : {list(payload['files'].keys())}")
        print(f"form   : {payload['form']}")

        # -- 7. 4xx fails fast — NO retries -----------------------------
        hr("7. 404 fails fast (no wasted retries)")
        try:
            client.get("/status/404")
        except httpx.HTTPStatusError as exc:
            print(f"caught : HTTP {exc.response.status_code} — returned immediately")

        # -- 8. 5xx IS retried, then raises -----------------------------
        hr("8. 503 is retried with jittered backoff, then raises")
        try:
            client.get("/status/503", max_attempts=3)
        except httpx.HTTPStatusError as exc:
            print(f"caught : HTTP {exc.response.status_code} after 3 attempts")

        # -- 9. POST is NOT retried (idempotency safety) ----------------
        hr("9. POST on 503 is NOT retried (prevents duplicate writes)")
        try:
            client.post("/status/503", json={"charge": 100})
        except httpx.HTTPStatusError as exc:
            print(f"caught : HTTP {exc.response.status_code} — single attempt only")
        print("opt in with retry_non_idempotent=True when it is safe")

        # -- 10. Inspect an error body without raising ------------------
        hr("10. raise_for_status=False to inspect errors manually")
        resp = client.get("/status/418", raise_for_status=False)
        print(f"status    : {resp.status_code}")
        print(f"is_error  : {resp.is_error}")

        # -- 11. Timeout handling ---------------------------------------
        hr("11. Per-request timeout override")
        try:
            client.get("/delay/5", timeout=1.0)
        except httpx.TimeoutException as exc:
            print(f"caught : {type(exc).__name__} — budget respected")

        # -- 12. Mutually exclusive body guard --------------------------
        hr("12. Guard against conflicting body parameters")
        try:
            client.post("/post", json={"a": 1}, content=b"raw")
        except RequestError as exc:
            print(f"caught : RequestError — {exc}")

        # -- 13. Streaming a large response -----------------------------
        hr("13. Streaming via the escape hatch (sync_client)")
        total = 0
        with client.sync_client.stream("GET", "/bytes/16384") as stream:
            for chunk in stream.iter_bytes(chunk_size=4096):
                total += len(chunk)
        print(f"streamed {total} bytes without buffering the whole body")

    # -- 14. Async: concurrent requests ---------------------------------
    async def async_demo() -> None:
        """Exercise the async API: concurrency, POST, and error handling."""
        hr("14. Async concurrent GETs on one pooled connection set")
        async with HttpClient(base_url=BASE) as client:
            urls = ["/get?i=1", "/get?i=2", "/get?i=3", "/uuid", "/user-agent"]
            responses = await asyncio.gather(
                *(client.aget(u) for u in urls),
                return_exceptions=True,
            )
            for u, r in zip(urls, responses):
                if isinstance(r, BaseException):
                    print(f"  {u:<14} -> FAILED {type(r).__name__}")
                else:
                    print(f"  {u:<14} -> {r.status_code}")

            hr("15. Async POST with JSON")
            resp = await client.apost("/post", json={"async": True, "n": 42})
            print(f"status : {resp.status_code}")
            print(f"echoed : {resp.json()['json']}")

            hr("16. Async error handling (non-blocking backoff)")
            try:
                await client.aget("/status/502", max_attempts=2)
            except httpx.HTTPStatusError as exc:
                print(f"caught : HTTP {exc.response.status_code} — loop never blocked")

    asyncio.run(async_demo())

    # -- 17. Module-level convenience functions -------------------------
    hr("17. Module-level helpers (shared default client)")
    resp = get_sync("https://httpbin.org/get", params={"via": "module-helper"})
    print(f"get_sync status : {resp.status_code}")

    async def module_helper_demo() -> None:
        """Demonstrate the async module-level helpers."""
        resp = await post_async("https://httpbin.org/post", json={"via": "helper"})
        print(f"post_async status: {resp.status_code}")
        await aclose_default_client()

    asyncio.run(module_helper_demo())

    close_default_client()
    hr("All demos complete — pools closed cleanly")