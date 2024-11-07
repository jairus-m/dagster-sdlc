"""
This module creates a specific get_activities() resource to extract 
from the REST API.
"""
import logging
import sys
from typing import Generator
import dlt
from dlt.sources.helpers import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

logger = logging.getLogger(__name__)

STRAVA_ACTIVITES_URL = dlt.config['strava_pipeline.strava.urls.strava_activites_url']
STRAVA_AUTH_URL = dlt.config['strava_pipeline.strava.urls.strava_auth_url']
PAYLOAD = {
    'client_id': dlt.secrets['strava_pipeline.strava.credentials.client_id'],
    'client_secret': dlt.secrets['strava_pipeline.strava.credentials.client_secret'],
    'refresh_token': dlt.secrets['strava_pipeline.strava.credentials.refresh_token'],
    'grant_type': "refresh_token",
    'f': 'json'
}

def strava_auth(strava_auth_url : dlt.config.value, payload: dict) -> str:
    """
    Returns access_token to get the proper 
    authorization header.
    """
    res = requests.post(strava_auth_url, data=payload, verify=False, timeout=100)
    return res.json()['access_token']

@dlt.resource(table_name="strava_activities", primary_key="id", write_disposition="merge")
def get_activities(
        header: dict, 
        strava_activities_url: 
        dlt.config.value,
        full_load: bool = False
        ) -> Generator:
    """
    Generator that returns json containing 200 activities per page. 
    """
    logger.info('Getting activities...')
    page_number = 1
    activities_per_page = 50
    logger.info('activities_per_page=%s, full_load=%s', activities_per_page, full_load)
    while full_load or page_number == 1:
        # get the data from page n
        param = {'per_page': activities_per_page, 'page': page_number}
        response = requests.get(strava_activities_url, headers=header, params=param, timeout=100)
        data = response.json()
        
        # break if no data (end)
        if not data:
            break
        
        logger.info('Yielding Page: %s', page_number)
        yield data

        if not full_load:
            break
        
        page_number += 1

@dlt.source
def strava_source():
    access_token = strava_auth(STRAVA_AUTH_URL, PAYLOAD)
    header = {'Authorization': 'Bearer ' + access_token}
    return get_activities(header, STRAVA_ACTIVITES_URL, full_load=False)

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="strava", 
        destination="duckdb", 
        dataset_name="activities"
    )

    load_info = pipeline.run(strava_source())
    print(load_info)
