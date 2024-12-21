"""
This module uses the RESTAPIConfig and rest_api_resources classes for declarative setup.
A second module, strava_pipeline, is written without these classes to compare and implement
dlt without full declarative configuration.
"""

import dlt
from dlt.sources.helpers import requests
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
import pendulum
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)

CLIENT_ID = dlt.secrets["strava_pipeline.strava.credentials.client_id"]
CLIENT_SECRET = dlt.secrets["strava_pipeline.strava.credentials.client_secret"]
REFRESH_TOEKEN = dlt.secrets["strava_pipeline.strava.credentials.refresh_token"]


def strava_auth(refresh_token, client_id, client_secret):
    """Return the access_token for Authorization bearer"""
    auth_url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    response = requests.post(auth_url, data=payload)
    access_token = response.json()["access_token"]
    return access_token


@dlt.source
def strava_source(client_id, client_secret, refresh_token):
    logger.info("Extracting Strava data source")
    access_token = strava_auth(refresh_token, client_id, client_secret)

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://www.strava.com/api/v3/",
            "auth": {
                "type": "bearer",
                "token": access_token,
            },
            "paginator": {"type": "page_number", "base_page": 1, "total_path": None},
        },
        "resources": [
            {
                "name": "activities",
                "endpoint": {
                    "params": {
                        "after": {
                            "type": "incremental",
                            "cursor_path": "start_date_local",
                            "initial_value": "2010-01-01 00:00:00+00:00",
                            "convert": lambda dt_str: int(
                                pendulum.parse(dt_str).timestamp()
                            ),
                        },
                    }
                },
                "primary_key": "id",
                "write_disposition": "merge",
            }
        ],
    }

    logger.info("RESTAPIConfig set up, starting to yield resources...")

    yield from rest_api_resources(config)


def load_strava_activities():
    pipeline = dlt.pipeline(
        pipeline_name="strava_rest_config",
        destination="duckdb",
        dataset_name="activities_rest_config",
        progress="log",
    )

    source = strava_source(
        client_id=CLIENT_ID, client_secret=CLIENT_SECRET, refresh_token=REFRESH_TOEKEN
    )

    load_info = pipeline.run(source)
    print(load_info)


if __name__ == "__main__":
    load_strava_activities()
