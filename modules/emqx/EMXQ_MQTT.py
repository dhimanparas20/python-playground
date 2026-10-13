"""
EMQXClient — Production-ready EMQX v5 REST API client.

Provides comprehensive management of EMQX broker: users, clients, subscriptions,
topics, messages, API keys, rules, data bridges, listeners, nodes, and health.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional, Union

try:
    import requests
    from requests.auth import HTTPBasicAuth
except ImportError:
    raise ImportError("Install requests: pip install requests")

logger = logging.getLogger(__name__)


class EMQXAPIError(Exception):
    """Custom exception for EMQX API errors."""

    def __init__(self, message: str, status_code: Optional[int] = None, response: Optional[Any] = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class EMQXClient:
    """
    Production-ready EMQX v5 REST API client.

    Attributes:
        base_url (str): Base URL of the EMQX REST API.
        api_key (str): API key for authentication.
        api_secret (str): API secret for authentication.
        timeout (int): Request timeout in seconds.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        api_secret: str,
        timeout: int = 10,
        max_retries: int = 0,
        retry_delay: float = 1.0,
    ) -> None:
        """
        Initialize the EMQXClient.

        Args:
            base_url: Base URL (e.g., "http://localhost:18083").
            api_key: API key for authentication.
            api_secret: API secret for authentication.
            timeout: Request timeout in seconds.
            max_retries: Number of retries on transient failures (0 to disable).
            retry_delay: Seconds between retries.
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.api_secret = api_secret
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(api_key, api_secret)
        self.session.headers.update({"Content-Type": "application/json"})

    # ──────────────────────────────────────────────
    # INTERNAL
    # ──────────────────────────────────────────────

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Send an HTTP request to the EMQX API with optional retry.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE).
            endpoint: API endpoint path (e.g., "/api/v5/clients").
            params: Query parameters.
            json_data: JSON body.

        Returns:
            Parsed JSON response.

        Raises:
            EMQXAPIError: On request failure.
        """
        url = f"{self.base_url}{endpoint}"
        last_error: Optional[Exception] = None
        attempts = max(1, self.max_retries + 1)

        for attempt in range(attempts):
            try:
                response = self.session.request(
                    method, url, params=params, json=json_data, timeout=self.timeout, **kwargs
                )
                if response.status_code == 429 and attempt < attempts - 1:
                    retry_after = int(response.headers.get("Retry-After", self.retry_delay))
                    time.sleep(retry_after)
                    continue
                response.raise_for_status()
                if response.content:
                    return response.json()
                return None
            except requests.HTTPError as e:
                status = e.response.status_code if e.response is not None else None
                detail = ""
                try:
                    detail = e.response.json() if e.response is not None and e.response.content else ""
                except Exception:
                    detail = e.response.text if e.response is not None else ""
                raise EMQXAPIError(
                    f"HTTP {status}: {detail or str(e)}",
                    status_code=status,
                    response=e.response,
                ) from e
            except (requests.ConnectionError, requests.Timeout) as e:
                last_error = e
                if attempt < attempts - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue
                raise EMQXAPIError(f"Connection error after {attempts} attempts: {e}") from e
            except requests.RequestException as e:
                raise EMQXAPIError(f"HTTP error: {e}") from e

        raise EMQXAPIError(f"Request failed after {attempts} attempts: {last_error}")

    def _list_all(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_param: str = "page",
        limit_param: str = "limit",
        page_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all items across pages for list endpoints that support pagination.

        Args:
            endpoint: API endpoint.
            params: Additional query parameters.
            page_param: Query param name for page number.
            limit_param: Query param name for page size.
            page_size: Number of items per page.

        Returns:
            Combined list of all items.
        """
        all_items: List[Dict[str, Any]] = []
        page = 1
        p = dict(params or {})
        p[limit_param] = page_size

        while True:
            p[page_param] = page
            result = self._request("GET", endpoint, params=p)
            data = result.get("data", []) if isinstance(result, dict) else (result or [])
            if not data:
                break
            all_items.extend(data)
            if isinstance(result, dict) and "meta" in result:
                meta = result["meta"]
                total_pages = meta.get("page_count") or meta.get("total_pages", 0)
                if page >= total_pages:
                    break
            elif len(data) < page_size:
                break
            page += 1

        return all_items

    # ──────────────────────────────────────────────
    # BROKER STATUS & NODES
    # ──────────────────────────────────────────────

    def get_status(self) -> Dict[str, Any]:
        """Get broker status (running/stopped)."""
        return self._request("GET", "/api/v5/status")

    def get_nodes(self) -> List[Dict[str, Any]]:
        """Get information about all nodes in the cluster."""
        result = self._request("GET", "/api/v5/nodes")
        return result.get("data", []) if isinstance(result, dict) else []

    def get_node(self, node_name: str) -> Dict[str, Any]:
        """Get information about a specific node."""
        return self._request("GET", f"/api/v5/nodes/{node_name}")

    def get_license(self) -> Dict[str, Any]:
        """Get license information."""
        return self._request("GET", "/api/v5/license")

    def get_stats(self) -> Dict[str, Any]:
        """Get broker statistics (connections, topics, subscriptions, etc.)."""
        return self._request("GET", "/api/v5/stats")

    # ──────────────────────────────────────────────
    # USER MANAGEMENT (built-in database)
    # ──────────────────────────────────────────────

    def list_users(self) -> List[Dict[str, Any]]:
        """
        List all users in the built-in authentication database.

        Returns:
            List of user objects.
        """
        result = self._request("GET", "/api/v5/authentication/password_based:built_in_database/users")
        return result.get("data", []) if isinstance(result, dict) else []

    def create_user(self, user_id: str, password: str, is_superuser: bool = False) -> Dict[str, Any]:
        """
        Create a new user in the built-in database.

        Args:
            user_id: Username.
            password: Password.
            is_superuser: Whether the user is a superuser.

        Returns:
            Created user object.
        """
        endpoint = "/api/v5/authentication/password_based:built_in_database/users"
        payload: Dict[str, Any] = {"user_id": user_id, "password": password, "is_superuser": is_superuser}
        return self._request("POST", endpoint, json_data=payload)

    def get_user(self, user_id: str) -> Dict[str, Any]:
        """Get details of a specific user."""
        endpoint = f"/api/v5/authentication/password_based:built_in_database/users/{user_id}"
        return self._request("GET", endpoint)

    def update_user(
        self,
        user_id: str,
        password: Optional[str] = None,
        is_superuser: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Update a user.

        Args:
            user_id: Username.
            password: New password (if changing).
            is_superuser: New superuser status (if changing).

        Returns:
            Updated user object.
        """
        endpoint = f"/api/v5/authentication/password_based:built_in_database/users/{user_id}"
        payload: Dict[str, Any] = {}
        if password is not None:
            payload["password"] = password
        if is_superuser is not None:
            payload["is_superuser"] = is_superuser
        return self._request("PUT", endpoint, json_data=payload)

    def delete_user(self, user_id: str) -> None:
        """Delete a user."""
        endpoint = f"/api/v5/authentication/password_based:built_in_database/users/{user_id}"
        self._request("DELETE", endpoint)

    # ──────────────────────────────────────────────
    # CLIENT MANAGEMENT
    # ──────────────────────────────────────────────

    def list_clients(self, page_size: int = 100) -> List[Dict[str, Any]]:
        """
        List all connected clients (auto-paginated).

        Args:
            page_size: Items per page.

        Returns:
            List of client objects.
        """
        return self._list_all("/api/v5/clients", page_size=page_size)

    def get_client(self, client_id: str) -> Dict[str, Any]:
        """Get details of a specific connected client."""
        return self._request("GET", f"/api/v5/clients/{client_id}")

    def disconnect_client(self, client_id: str) -> None:
        """Disconnect a client by client ID."""
        self._request("DELETE", f"/api/v5/clients/{client_id}")

    def ban_client(self, client_id: str, reason: str = "", until: Optional[int] = None) -> Dict[str, Any]:
        """
        Ban a client ID from connecting.

        Args:
            client_id: Client ID to ban.
            reason: Reason for the ban.
            until: Unix timestamp when the ban expires. None = permanent.

        Returns:
            Ban result.
        """
        payload: Dict[str, Any] = {"clientid": client_id, "reason": reason}
        if until is not None:
            payload["until"] = until
        return self._request("POST", "/api/v5/banned", json_data=payload)

    def list_banned_clients(self) -> List[Dict[str, Any]]:
        """List all banned clients."""
        result = self._request("GET", "/api/v5/banned")
        return result.get("data", []) if isinstance(result, dict) else []

    def unban_client(self, client_id: str) -> None:
        """Remove a ban by client ID."""
        self._request("DELETE", f"/api/v5/banned/{client_id}")

    # ──────────────────────────────────────────────
    # SUBSCRIPTION MANAGEMENT
    # ──────────────────────────────────────────────

    def list_subscriptions(self, page_size: int = 100) -> List[Dict[str, Any]]:
        """
        List all subscriptions (auto-paginated).

        Args:
            page_size: Items per page.

        Returns:
            List of subscription objects.
        """
        return self._list_all("/api/v5/subscriptions", page_size=page_size)

    def list_client_subscriptions(self, client_id: str) -> List[Dict[str, Any]]:
        """List all subscriptions for a specific client."""
        result = self._request("GET", f"/api/v5/clients/{client_id}/subscriptions")
        return result.get("data", []) if isinstance(result, dict) else []

    def get_subscription(self, topic: str, client_id: str) -> Dict[str, Any]:
        """Get details of a specific subscription."""
        encoded_topic = requests.utils.quote(topic, safe="")
        return self._request("GET", f"/api/v5/subscriptions/{encoded_topic}/{client_id}")

    # ──────────────────────────────────────────────
    # TOPIC MANAGEMENT
    # ──────────────────────────────────────────────

    def list_topics(self, page_size: int = 100) -> List[Dict[str, Any]]:
        """
        List all topics with subscriptions (auto-paginated).

        Args:
            page_size: Items per page.

        Returns:
            List of topic objects.
        """
        return self._list_all("/api/v5/topics", page_size=page_size)

    def get_topic_metrics(self, topic: str) -> Dict[str, Any]:
        """Get metrics for a specific topic."""
        encoded_topic = requests.utils.quote(topic, safe="")
        return self._request("GET", f"/api/v5/topics/{encoded_topic}/metrics")

    def get_topic_subscriptions(self, topic: str) -> List[Dict[str, Any]]:
        """List all subscriptions for a specific topic."""
        encoded_topic = requests.utils.quote(topic, safe="")
        result = self._request("GET", f"/api/v5/topics/{encoded_topic}/subscriptions")
        return result.get("data", []) if isinstance(result, dict) else []

    def list_topic_alias(self) -> List[Dict[str, Any]]:
        """List all topic aliases."""
        result = self._request("GET", "/api/v5/topic_alias")
        return result.get("data", []) if isinstance(result, dict) else []

    # ──────────────────────────────────────────────
    # MESSAGE PUBLISHING
    # ──────────────────────────────────────────────

    def publish_message(
        self,
        topic: str,
        payload: Union[str, Dict[str, Any], List[Any]],
        qos: int = 0,
        retain: bool = False,
        content_type: str = "plain",
        encoding: str = "plain",
        properties: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Publish a message to a topic via the REST API.

        Args:
            topic: Topic to publish to.
            payload: Message payload (string or JSON-serializable dict/list).
            qos: QoS level (0, 1, or 2).
            retain: Whether to retain the message.
            content_type: Content type indicator: "plain", "json", "text", etc.
            encoding: Encoding mode: "plain" or "base64".
            properties: Optional MQTT5 properties dict.

        Returns:
            Publish result.
        """
        endpoint = "/api/v5/mqtt/publish"
        data: Dict[str, Any] = {
            "topic": topic,
            "payload": json.dumps(payload, default=str) if isinstance(payload, (dict, list)) else payload,
            "qos": qos,
            "retain": retain,
            "content_type": content_type,
            "encoding": encoding,
        }
        if properties:
            data["properties"] = properties
        return self._request("POST", endpoint, json_data=data)

    # ──────────────────────────────────────────────
    # API KEY MANAGEMENT
    # ──────────────────────────────────────────────

    def list_api_keys(self) -> List[Dict[str, Any]]:
        """List all API keys."""
        result = self._request("GET", "/api/v5/api_keys")
        return result.get("data", []) if isinstance(result, dict) else []

    def create_api_key(
        self,
        description: str = "",
        permissions: Optional[List[str]] = None,
        expire_at: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Create a new API key.

        Args:
            description: Description for the API key.
            permissions: List of permissions (default: ["all"]).
            expire_at: Optional expiration Unix timestamp.

        Returns:
            Created API key object.
        """
        payload: Dict[str, Any] = {
            "description": description,
            "permissions": permissions or ["all"],
        }
        if expire_at is not None:
            payload["expire_at"] = expire_at
        return self._request("POST", "/api/v5/api_keys", json_data=payload)

    def delete_api_key(self, key_id: str) -> None:
        """Delete an API key."""
        self._request("DELETE", f"/api/v5/api_keys/{key_id}")

    # ──────────────────────────────────────────────
    # LISTENER MANAGEMENT
    # ──────────────────────────────────────────────

    def list_listeners(self) -> List[Dict[str, Any]]:
        """List all listeners."""
        result = self._request("GET", "/api/v5/listeners")
        return result.get("data", []) if isinstance(result, dict) else []

    def get_listener(self, listener_id: str) -> Dict[str, Any]:
        """Get details of a specific listener."""
        return self._request("GET", f"/api/v5/listeners/{listener_id}")

    # ──────────────────────────────────────────────
    # RULE ENGINE
    # ──────────────────────────────────────────────

    def list_rules(self) -> List[Dict[str, Any]]:
        """List all rules in the rule engine."""
        result = self._request("GET", "/api/v5/rules")
        return result.get("data", []) if isinstance(result, dict) else []

    def create_rule(
        self,
        rawsql: str,
        description: str = "",
        actions: Optional[List[Dict[str, Any]]] = None,
        enable: bool = True,
    ) -> Dict[str, Any]:
        """
        Create a new rule.

        Args:
            rawsql: SQL-like rule statement.
            description: Human-readable description.
            actions: List of action configs to trigger on match.
            enable: Whether the rule is enabled on creation.

        Returns:
            Created rule object.
        """
        payload: Dict[str, Any] = {
            "rawsql": rawsql,
            "description": description,
            "actions": actions or [],
            "enable": enable,
        }
        return self._request("POST", "/api/v5/rules", json_data=payload)

    def get_rule(self, rule_id: str) -> Dict[str, Any]:
        """Get details of a specific rule."""
        return self._request("GET", f"/api/v5/rules/{rule_id}")

    def update_rule(
        self,
        rule_id: str,
        rawsql: Optional[str] = None,
        description: Optional[str] = None,
        actions: Optional[List[Dict[str, Any]]] = None,
        enable: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Update a rule.

        Args:
            rule_id: Rule ID.
            rawsql: New SQL statement.
            description: New description.
            actions: New actions list.
            enable: Whether the rule is enabled.

        Returns:
            Updated rule object.
        """
        payload: Dict[str, Any] = {}
        if rawsql is not None:
            payload["rawsql"] = rawsql
        if description is not None:
            payload["description"] = description
        if actions is not None:
            payload["actions"] = actions
        if enable is not None:
            payload["enable"] = enable
        return self._request("PUT", f"/api/v5/rules/{rule_id}", json_data=payload)

    def delete_rule(self, rule_id: str) -> None:
        """Delete a rule."""
        self._request("DELETE", f"/api/v5/rules/{rule_id}")

    # ──────────────────────────────────────────────
    # DATA BRIDGES
    # ──────────────────────────────────────────────

    def list_bridges(self) -> List[Dict[str, Any]]:
        """List all data bridges."""
        result = self._request("GET", "/api/v5/bridges")
        return result.get("data", []) if isinstance(result, dict) else []

    def create_bridge(
        self,
        name: str,
        bridge_type: str,
        config: Dict[str, Any],
        enable: bool = True,
    ) -> Dict[str, Any]:
        """
        Create a new data bridge.

        Args:
            name: Bridge name.
            bridge_type: Bridge type (e.g., "mqtt", "kafka", "http", "mysql").
            config: Bridge-specific configuration dict.
            enable: Whether the bridge is enabled.

        Returns:
            Created bridge object.
        """
        payload: Dict[str, Any] = {
            "name": name,
            "type": bridge_type,
            "config": config,
            "enable": enable,
        }
        return self._request("POST", "/api/v5/bridges", json_data=payload)

    def get_bridge(self, bridge_id: str) -> Dict[str, Any]:
        """Get details of a specific data bridge."""
        return self._request("GET", f"/api/v5/bridges/{bridge_id}")

    def update_bridge(
        self,
        bridge_id: str,
        config: Optional[Dict[str, Any]] = None,
        enable: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Update a data bridge.

        Args:
            bridge_id: Bridge ID (url-encoded name).
            config: New bridge configuration.
            enable: Whether the bridge is enabled.

        Returns:
            Updated bridge object.
        """
        payload: Dict[str, Any] = {}
        if config is not None:
            payload["config"] = config
        if enable is not None:
            payload["enable"] = enable
        return self._request("PUT", f"/api/v5/bridges/{bridge_id}", json_data=payload)

    def delete_bridge(self, bridge_id: str) -> None:
        """Delete a data bridge."""
        self._request("DELETE", f"/api/v5/bridges/{bridge_id}")

    # ──────────────────────────────────────────────
    # ALERTS
    # ──────────────────────────────────────────────

    def list_alerts(self) -> List[Dict[str, Any]]:
        """List all alerts."""
        result = self._request("GET", "/api/v5/alerts")
        return result.get("data", []) if isinstance(result, dict) else []

    def deactivate_alert(self, alert_name: str) -> None:
        """Deactivate an alert by name."""
        self._request("PUT", f"/api/v5/alerts/{alert_name}/deactivate")

    # ──────────────────────────────────────────────
    # AUTHENTICATION / AUTHORIZATION
    # ──────────────────────────────────────────────

    def list_authenticators(self) -> List[Dict[str, Any]]:
        """List all authentication backends."""
        result = self._request("GET", "/api/v5/authentication")
        return result.get("data", []) if isinstance(result, dict) else []

    def list_authorization_sources(self) -> List[Dict[str, Any]]:
        """List all authorization (ACL) sources."""
        result = self._request("GET", "/api/v5/authorization/sources")
        return result.get("data", []) if isinstance(result, dict) else []

    # ──────────────────────────────────────────────
    # LIFECYCLE
    # ──────────────────────────────────────────────

    def close(self) -> None:
        """Close the HTTP session."""
        self.session.close()

    def __repr__(self) -> str:
        return f"EMQXClient(base_url='{self.base_url}', api_key='{self.api_key[:4]}...')"

    def __enter__(self) -> "EMQXClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


# ──────────────────────────────────────────────
# USAGE EXAMPLES
# ──────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    import os

    client = EMQXClient(
        base_url=os.getenv("EMQX_URL", "http://localhost:18083"),
        api_key=os.getenv("EMQX_API_KEY", ""),
        api_secret=os.getenv("EMQX_API_SECRET", ""),
        max_retries=2,
    )

    print("=" * 60)
    print("EMQXClient - Usage Examples")
    print("=" * 60)

    with client:
        print(f"\nStatus:   {client.get_status()}")
        print(f"Nodes:    {len(client.get_nodes())}")
        print(f"Clients:  {len(client.list_clients())}")
        print(f"Topics:   {len(client.list_topics())}")
        print(f"Rules:    {len(client.list_rules())}")
        print(f"Bridges:  {len(client.list_bridges())}")
        print(f"Alerts:   {len(client.list_alerts())}")
        print(f"API Keys: {len(client.list_api_keys())}")

    print("\nDone!")
