import os
import dlt
from dlt.sources.rest_api import rest_api_source
import requests
import time


STRAVA_ACTIVITES_URL = dlt.config['strava_pipeline.strava.urls.strava_activites_url']
STRAVA_AUTH_URL = dlt.config['strava_pipeline.strava.urls.strava_auth_url']

PAYLOAD = {
    'client_id': dlt.secrets['strava_pipeline.strava.credentials.client_id'],
    'client_secret': dlt.secrets['strava_pipeline.strava.credentials.client_secret'],
    'refresh_token': dlt.secrets['strava_pipeline.strava.credentials.refresh_token'],
    'grant_type': "refresh_token",
    'f': 'json'
}

def strava_auth(strava_auth_url : dlt.configs.value, payload: dict) -> str: 
    """
    Returns access_token to get the proper 
    authorization header.
    """
    res = requests.post(strava_auth_url, data=payload, verify=False, timeout=100)
    return res.json()['access_token']

@dlt.resource(name='strava_activities', write_disposition='replace')
def strava_source():
    """
    Generator that returns json containing 200 activities. 
    """
    access_token = strava_auth(strava_auth_url=STRAVA_AUTH_URL, payload=PAYLOAD)
    header = {'Authorization': 'Bearer ' + access_token}

    page_number = 1
    while True:
        param = {'per_page': 200, 'page': page_number}
        response = requests.get(STRAVA_ACTIVITES_URL, headers=header, params=param)
        
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

def load_strava_data():
    pipeline = dlt.pipeline(
        pipeline_name="strava_data", 
        destination="duckdb", 
        dataset_name="strava_activities")
    
    load_info = pipeline.run(strava_source())
    print(load_info)

if __name__ == "__main__":
    load_strava_data()







        
print('Finished.')