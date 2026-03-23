from dotenv import load_dotenv

from unittest.mock import patch, MagicMock

from dagster_proj.resources.configured_resources import StravaAPIResource

load_dotenv()  # load env vars


def test_get_access_token_caches_token():
    """
    Test that get_access_token caches the token after the first call.

    This test verifies that the StravaAPIResource only calls the Strava
    OAuth endpoint once, even when get_access_token is called multiple times.
    """
    resource = StravaAPIResource(
        client_id="test_id",
        client_secret="test_secret",
        refresh_token="test_token",
        athlete_id=12345,
    )

    mock_response = MagicMock()
    mock_response.json.return_value = {"access_token": "abc123"}
    mock_response.raise_for_status = MagicMock()

    with patch(
        "dagster_proj.resources.configured_resources.requests.post",
        return_value=mock_response,
    ) as mock_post:
        token1 = resource.get_access_token()
        token2 = resource.get_access_token()

        assert token1 == "abc123"
        assert token2 == "abc123"
        mock_post.assert_called_once()  # only one HTTP request despite two calls


def test_refresh_access_token_calls_strava_oauth():
    """
    Test that _refresh_access_token sends the correct payload to Strava's OAuth endpoint.

    This test checks that the token refresh request includes the proper
    client credentials and grant type.
    """
    resource = StravaAPIResource(
        client_id="test_id",
        client_secret="test_secret",
        refresh_token="test_token",
        athlete_id=12345,
    )

    mock_response = MagicMock()
    mock_response.json.return_value = {"access_token": "new_token_123"}
    mock_response.raise_for_status = MagicMock()

    with patch(
        "dagster_proj.resources.configured_resources.requests.post",
        return_value=mock_response,
    ) as mock_post:
        result = resource._refresh_access_token()

        assert result == "new_token_123"
        mock_post.assert_called_once_with(
            "https://www.strava.com/oauth/token",
            data={
                "client_id": "test_id",
                "client_secret": "test_secret",
                "refresh_token": "test_token",
                "grant_type": "refresh_token",
            },
            timeout=100,
        )
        mock_response.raise_for_status.assert_called_once()


def test_refresh_access_token_raises_on_http_error():
    """
    Test that _refresh_access_token propagates HTTP errors from the Strava API.

    This test ensures that if the OAuth token refresh fails (e.g., invalid
    credentials), the error is raised rather than silently swallowed.
    """
    resource = StravaAPIResource(
        client_id="bad_id",
        client_secret="bad_secret",
        refresh_token="bad_token",
        athlete_id=12345,
    )

    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Exception("401 Unauthorized")

    with patch(
        "dagster_proj.resources.configured_resources.requests.post",
        return_value=mock_response,
    ):
        try:
            resource._refresh_access_token()
            assert False, "Expected an exception to be raised"
        except Exception as e:
            assert "401 Unauthorized" in str(e)
