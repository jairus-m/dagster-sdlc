import dlt
from dlt.sources.helpers import requests
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
import pendulum
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

logger = logging.getLogger(__name__)

"https://www.strava.com/api/v3/athlete/activities"

CLIENT_ID = dlt.secrets['strava_pipeline.strava.credentials.client_id']
CLIENT_SECRET = dlt.secrets['strava_pipeline.strava.credentials.client_secret']
REFRESH_TOEKEN = dlt.secrets['strava_pipeline.strava.credentials.refresh_token']

def strava_auth(refresh_token, client_id, client_secret):
    """Return the access_token for Authorization bearer"""
    auth_url = "https://www.strava.com/oauth/token"
    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token'
    }
    response = requests.post(auth_url, data=payload)
    access_token = response.json()['access_token']
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
            "paginator": {
                "type": "page_number",
                "base_page": 1,
                "total_path": None
            },
        },
        "resources": [
            {
                "name": "activities",
                "endpoint": {
                    "path": "athlete/activities",
                    "incremental": {
                        "start_param": "start",
                        "cursor_path": "start_date_local",
                        "initial_value": "2024-11-05 00:00:00+00:00",
                        "convert": lambda dt_str: int(pendulum.parse(dt_str).timestamp()),

                    }
                             
                },
                "primary_key": "id",
                "write_disposition": "merge",
            }
        ]
    }
    
    logger.info("RESTAPIConfig set up, starting to yield resources...")

    yield from rest_api_resources(config)

def load_strava_activities():
    pipeline = dlt.pipeline(
        pipeline_name="strava_activities_dlt", 
        destination="duckdb", 
        dataset_name="strava_data_dlt",
        progress="log")

    source = strava_source(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        refresh_token=REFRESH_TOEKEN
    )

    load_info = pipeline.run(source)
    print(load_info)

if __name__ == "__main__":
    load_strava_activities()


#%%
import random

def number_generator(start, end):
    current = start
    while current <= end:
        print(current)
        yield current
        current += 1

for num in number_generator(1, 5):
    pass
    

#%%
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
help(rest_api_resources)
# %%
