import requests
from typing import Optional, Dict, Any, List, Union
from requests.auth import HTTPBasicAuth


class EMQXAPIError(Exception):
    """Custom exception for EMQX API errors."""
    pass

class EMQXClient:

    def __init__(self, base_url: str, api_key: str, api_secret: str, timeout: int = 10) -> None:
        """
        Initialize the EMQXClient.

        Args:
            base_url (str): Base URL of the EMQX REST API (e.g., "http://localhost:80").
            api_key (str): API key for authentication.
            api_secret (str): API secret for authentication.
            timeout (int): Request timeout in seconds.
        """
        self.base_url = base_url.rstrip("/")
        self.auth = HTTPBasicAuth(api_key, api_secret)
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({"Content-Type": "application/json"})

    def _request(self, method: str, endpoint: str, **kwargs) -> Any:
        """
        Internal method to send HTTP requests to the EMQX API.

        Args:
            method (str): HTTP method (GET, POST, etc.).
            endpoint (str): API endpoint path.

        Returns:
            Any: Parsed JSON response.

        Raises:
            EMQXAPIError: If the request fails or returns an error.
        """
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.request(method, url, timeout=self.timeout, **kwargs)
            response.raise_for_status()
            if response.content:
                return response.json()
            return None
        except requests.RequestException as e:
            raise EMQXAPIError(f"HTTP error: {e}") from e
        except ValueError:
            raise EMQXAPIError("Invalid JSON response from EMQX API.")

    # User Management

    def list_users(self) -> List[Dict[str, Any]]:
        """
        List all users in the built-in database.

        Returns:
            List[Dict[str, Any]]: List of user objects.
        """
        endpoint = "/api/v5/authentication/password_based:built_in_database/users"
        return self._request("GET", endpoint).get("data", [])

    def create_user(self, user_id: str, password: str, is_superuser: bool = False) -> Dict[str, Any]:
        """
        Create a new user.

        Args:
            user_id (str): The user ID (username).
            password (str): The user's password.
            is_superuser (bool): Whether the user is a superuser.

        Returns:
            Dict[str, Any]: The created user object.
        """
        endpoint = "/api/v5/authentication/password_based:built_in_database/users"
        payload = {
            "user_id": user_id,
            "password": password,
            "is_superuser": is_superuser
        }
        return self._request("POST", endpoint, json=payload)

    def get_user(self, user_id: str) -> Dict[str, Any]:
        """
        Get details of a specific user.

        Args:
            user_id (str): The user ID.

        Returns:
            Dict[str, Any]: The user object.
        """
        endpoint = f"/api/v5/authentication/password_based:built_in_database/users/{user_id}"
        return self._request("GET", endpoint)

    def update_user(self, user_id: str, password: Optional[str] = None, is_superuser: Optional[bool] = None) -> Dict[str, Any]:
        """
        Update a user's password and/or superuser status.

        Args:
            user_id (str): The user ID.
            password (Optional[str]): New password.
            is_superuser (Optional[bool]): New superuser status.

        Returns:
            Dict[str, Any]: The updated user object.
        """
        endpoint = f"/api/v5/authentication/password_based:built_in_database/users/{user_id}"
        payload: Dict[str, Union[str, bool]] = {}
        if password is not None:
            payload["password"] = password
        if is_superuser is not None:
            payload["is_superuser"] = is_superuser
        return self._request("PUT", endpoint, json=payload)

    def delete_user(self, user_id: str) -> None:
        """
        Delete a user.

        Args:
            user_id (str): The user ID.

        Returns:
            None
        """
        endpoint = f"/api/v5/authentication/password_based:built_in_database/users/{user_id}"
        self._request("DELETE", endpoint)

    # Client Management

    def list_clients(self) -> List[Dict[str, Any]]:
        """
        List all connected clients.

        Returns:
            List[Dict[str, Any]]: List of client objects.
        """
        endpoint = "/api/v5/clients"
        return self._request("GET", endpoint).get("data", [])

    def disconnect_client(self, client_id: str) -> None:
        """
        Disconnect a client by client ID.

        Args:
            client_id (str): The client ID.

        Returns:
            None
        """
        endpoint = f"/api/v5/clients/{client_id}"
        self._request("DELETE", endpoint)

    # Subscription Management

    def list_subscriptions(self) -> List[Dict[str, Any]]:
        """
        List all subscriptions.

        Returns:
            List[Dict[str, Any]]: List of subscription objects.
        """
        endpoint = "/api/v5/subscriptions"
        return self._request("GET", endpoint).get("data", [])

    def list_client_subscriptions(self, client_id: str) -> List[Dict[str, Any]]:
        """
        List all subscriptions for a specific client.

        Args:
            client_id (str): The client ID.

        Returns:
            List[Dict[str, Any]]: List of subscription objects.
        """
        endpoint = f"/api/v5/clients/{client_id}/subscriptions"
        return self._request("GET", endpoint).get("data", [])

    # Message Publishing

    def publish_message(self, topic: str, payload: str, qos: int = 0, retain: bool = False) -> Dict[str, Any]:
        """
        Publish a message to a topic.

        Args:
            topic (str): The topic to publish to.
            payload (str): The message payload.
            qos (int): Quality of Service level (0, 1, or 2).
            retain (bool): Whether to retain the message.

        Returns:
            Dict[str, Any]: The publish result.
        """
        endpoint = "/api/v5/mqtt/publish"
        data = {
            "topic": topic,
            "payload": payload,
            "qos": qos,
            "retain": retain
        }
        return self._request("POST", endpoint, json=data)

    # API Key Management

    def list_api_keys(self) -> List[Dict[str, Any]]:
        """
        List all API keys.

        Returns:
            List[Dict[str, Any]]: List of API key objects.
        """
        endpoint = "/api/v5/api_keys"
        return self._request("GET", endpoint).get("data", [])

    def create_api_key(self, description: str = "", permissions: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Create a new API key.

        Args:
            description (str): Description for the API key.
            permissions (Optional[List[str]]): List of permissions.

        Returns:
            Dict[str, Any]: The created API key object.
        """
        endpoint = "/api/v5/api_keys"
        payload = {
            "description": description,
            "permissions": permissions or ["all"]
        }
        return self._request("POST", endpoint, json=payload)

    def delete_api_key(self, key_id: str) -> None:
        """
        Delete an API key.

        Args:
            key_id (str): The API key ID.

        Returns:
            None
        """
        endpoint = f"/api/v5/api_keys/{key_id}"
        self._request("DELETE", endpoint)

    # Health and Status

    def get_status(self) -> Dict[str, Any]:
        """
        Get broker status.

        Returns:
            Dict[str, Any]: Status information.
        """
        endpoint = "/api/v5/status"
        return self._request("GET", endpoint)

    def get_nodes(self) -> List[Dict[str, Any]]:
        """
        Get information about all nodes.

        Returns:
            List[Dict[str, Any]]: List of node objects.
        """
        endpoint = "/api/v5/nodes"
        return self._request("GET", endpoint).get("data", [])

if __name__ == "__main__":
    client = EMQXClient(base_url="http://192.168.1.69:80", api_key="", api_secret="")
    print(client.list_users())
    print(client.list_clients())
    print(client.list_subscriptions())
