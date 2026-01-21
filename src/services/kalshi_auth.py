"""
Kalshi API Authentication using RSA-PSS signatures.

Kalshi requires RSA key-based authentication for API access. Each request must include:
- KALSHI-ACCESS-KEY: Your API key ID
- KALSHI-ACCESS-TIMESTAMP: Request timestamp in milliseconds
- KALSHI-ACCESS-SIGNATURE: RSA-PSS signature of the request

The signature is computed over: timestamp + method + path + body
"""
import base64
import time
from pathlib import Path
from typing import Optional

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend


class KalshiAuthenticator:
    """Handles RSA-PSS signature generation for Kalshi API requests."""

    def __init__(
        self,
        api_key: str,
        private_key_path: Optional[str] = None,
        private_key_pem: Optional[str] = None,
    ):
        """
        Initialize the authenticator.

        Args:
            api_key: The Kalshi API key ID
            private_key_path: Path to the RSA private key PEM file
            private_key_pem: Direct PEM content as a string
        """
        self._api_key = api_key
        self._private_key = self._load_private_key(private_key_path, private_key_pem)

    def _load_private_key(
        self,
        path: Optional[str],
        pem_content: Optional[str],
    ):
        """Load RSA private key from file or direct PEM content."""
        if pem_content:
            # Handle escaped newlines in environment variables
            pem_bytes = pem_content.replace("\\n", "\n").encode("utf-8")
        elif path:
            pem_bytes = Path(path).read_bytes()
        else:
            raise ValueError("Either private_key_path or private_key_pem required")

        return serialization.load_pem_private_key(
            pem_bytes,
            password=None,
            backend=default_backend(),
        )

    def _sign_message(self, message: str) -> str:
        """Sign a message with RSA-PSS and return base64-encoded signature."""
        message_bytes = message.encode("utf-8")

        signature = self._private_key.sign(
            message_bytes,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )

        return base64.b64encode(signature).decode("utf-8")

    def generate_headers(self, method: str, path: str, body: str = "") -> dict:
        """
        Generate authentication headers for Kalshi REST API requests.

        Args:
            method: HTTP method (GET, POST, etc.)
            path: Request path (e.g., "/trade-api/v2/markets")
            body: Request body as string (empty for GET requests)

        Returns:
            Dict with KALSHI-ACCESS-KEY, KALSHI-ACCESS-TIMESTAMP, KALSHI-ACCESS-SIGNATURE
        """
        timestamp_ms = int(time.time() * 1000)
        timestamp_str = str(timestamp_ms)

        # Message format: timestamp + method + path + body
        message = f"{timestamp_str}{method.upper()}{path}{body}"
        signature_b64 = self._sign_message(message)

        return {
            "KALSHI-ACCESS-KEY": self._api_key,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_str,
            "KALSHI-ACCESS-SIGNATURE": signature_b64,
        }

    def generate_ws_headers(self) -> dict:
        """
        Generate authentication headers for WebSocket connection handshake.

        Kalshi requires these headers during the WebSocket upgrade request,
        not as a separate login command after connection.

        Returns:
            Dict with KALSHI-ACCESS-KEY, KALSHI-ACCESS-TIMESTAMP, KALSHI-ACCESS-SIGNATURE
        """
        timestamp_ms = int(time.time() * 1000)
        timestamp_str = str(timestamp_ms)

        # For WebSocket, sign: timestamp + "GET" + "/trade-api/ws/v2"
        message = f"{timestamp_str}GET/trade-api/ws/v2"
        signature_b64 = self._sign_message(message)

        return {
            "KALSHI-ACCESS-KEY": self._api_key,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_str,
            "KALSHI-ACCESS-SIGNATURE": signature_b64,
        }

    def generate_ws_auth_message(self) -> dict:
        """
        Generate WebSocket authentication command (deprecated).

        Note: Kalshi now uses header-based auth during connection.
        This method is kept for backwards compatibility.
        """
        timestamp_ms = int(time.time() * 1000)

        message = f"{timestamp_ms}GET/trade-api/ws/v2"
        signature_b64 = self._sign_message(message)

        return {
            "id": 1,
            "cmd": "login",
            "params": {
                "api_key": self._api_key,
                "timestamp": timestamp_ms,
                "signature": signature_b64,
            },
        }

    @property
    def api_key(self) -> str:
        """Return the API key ID (safe to expose)."""
        return self._api_key
