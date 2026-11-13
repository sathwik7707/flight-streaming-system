"""Small OAuth2-aware client for OpenSky state vectors."""

import time
from typing import Any

import requests

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
STATES_URL = "https://opensky-network.org/api/states/all"

class OpenSkyError(RuntimeError):
    """An API error with an optional server-requested retry delay."""

    def __init__(self, message: str, retry_after_seconds: int | None = None):
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds

class OpenSkyClient:
    """Fetch OpenSky states, using OAuth2 when client credentials are set."""

    def __init__(
        self, client_id: str = "", client_secret: str = "", timeout: int = 25
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout = timeout
        self.session = requests.Session()
        self.access_token = None
        self.token_expires_at = 0

    def _get_token(self, force_refresh: bool = False) -> str | None:
        """Return a cached token or request a new client-credentials token."""
        if not self.client_id or not self.client_secret:
            return None
        if not force_refresh and self.access_token and time.time() < self.token_expires_at - 60:
            return self.access_token

        try:
            response = self.session.post(
                TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise OpenSkyError(f"OpenSky OAuth token request failed: {exc}") from exc
        if response.status_code >= 400:
            raise OpenSkyError(
                f"OpenSky OAuth token request failed: HTTP {response.status_code}: "
                f"{response.text[:300]}"
            )
        try:
            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(payload.get("access_token"), str):
                raise ValueError("access token is missing")
            self.access_token = payload["access_token"]
            self.token_expires_at = time.time() + int(payload.get("expires_in", 1800))
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            raise OpenSkyError("OpenSky OAuth response did not contain a valid access token") from exc
        return self.access_token

    @staticmethod
    def _raise_for_response(response: requests.Response) -> None:
        """Raise a useful error for quota and other unsuccessful responses."""
        if response.status_code == 429:
            retry_value = (
                response.headers.get("X-Rate-Limit-Retry-After-Seconds")
                or response.headers.get("Retry-After")
            )
            try:
                retry_after = max(0, int(retry_value)) if retry_value else None
            except (TypeError, ValueError):
                retry_after = None
            message = "OpenSky rate limit reached (HTTP 429)."
            if retry_after is not None:
                message += f" Retry after {retry_after} seconds."
            raise OpenSkyError(message, retry_after_seconds=retry_after)
        if response.status_code >= 400:
            raise OpenSkyError(
                f"OpenSky state request failed: HTTP {response.status_code}: "
                f"{response.text[:300]}"
            )

    def get_states(
        self, lamin: float, lomin: float, lamax: float, lomax: float
    ) -> tuple[dict[str, Any], requests.structures.CaseInsensitiveDict]:
        """Return one state-vector snapshot and its response headers."""
        params = {
            "lamin": lamin, "lomin": lomin,
            "lamax": lamax, "lomax": lomax,
        }
        headers = {}
        if self.client_id and self.client_secret:
            headers["Authorization"] = f"Bearer {self._get_token()}"

        try:
            response = self.session.get(
                STATES_URL, params=params, headers=headers, timeout=self.timeout
            )
        except requests.RequestException as exc:
            raise OpenSkyError(f"OpenSky state request failed: {exc}") from exc
        if response.status_code == 401 and self.client_id and self.client_secret:
            headers["Authorization"] = f"Bearer {self._get_token(force_refresh=True)}"
            try:
                response = self.session.get(
                    STATES_URL, params=params, headers=headers, timeout=self.timeout
                )
            except requests.RequestException as exc:
                raise OpenSkyError(f"OpenSky state request failed: {exc}") from exc

        self._raise_for_response(response)
        try:
            payload = response.json()
        except ValueError as exc:
            raise OpenSkyError("OpenSky state response was not valid JSON") from exc
        if not isinstance(payload, dict):
            raise OpenSkyError("OpenSky state response had an unexpected JSON structure")
        return payload, response.headers
