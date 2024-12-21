from dagster import ConfigurableResource
import requests

class StravaAPIResource(ConfigurableResource):
    client_id: str
    client_secret: str
    refresh_token: str
    _access_token: str = None

    def get_access_token(self) -> str:
        if not self._access_token:
            self._access_token = self._refresh_access_token()
        return self._access_token

    def _refresh_access_token(self) -> str:
        auth_url = "https://www.strava.com/oauth/token"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
        }
        response = requests.post(auth_url, data=payload, timeout=100)
        response.raise_for_status()
        return response.json()["access_token"]
