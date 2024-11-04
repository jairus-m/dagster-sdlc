import requests
import dlt
import time
from typing import Generator


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
def strava_source(header: dict, strava_activities_url: dlt.config.value) -> Generator:
    """
    Generator that returns json containing 200 activities. 
    """
    page_number = 1
    while True:
        param = {'per_page': 200, 'page': page_number}
        response = requests.get(strava_activities_url, headers=header, params=param, timeout=100)
        
        # Check for rate limiting
        if response.status_code == 429:
            print("Rate limit reached. Waiting before next request.")
            time.sleep(15 * 60)  # Wait for 15 minutes
            continue

        my_dataset = response.json()
        
        if not my_dataset:  # If the response is empty, we've reached the end
            break
        
        print(f'Yielding Page: {page_number}')
        yield my_dataset
        
        page_number += 1



if __name__ == "__main__":
    access_token = strava_auth(STRAVA_AUTH_URL, PAYLOAD)
    header = {'Authorization': 'Bearer ' + access_token}

    pipeline = dlt.pipeline(
        pipeline_name="strava_data", 
        destination="duckdb", 
        dataset_name="strava_activities")

    load_info = pipeline.run(strava_source(header, STRAVA_ACTIVITES_URL))
    print(load_info)
