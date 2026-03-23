from dotenv import load_dotenv

from dagster_proj.assets.dlt.activities import strava_activities, strava_stats

load_dotenv()  # load env vars


def test_strava_activities_config_structure():
    """
    Test the strava_activities dltHub resource config.

    This test validates the structure and values of the activities
    endpoint configuration used in the RESTAPIConfig.
    """
    config = strava_activities()

    assert config["name"] == "activities"
    assert config["primary_key"] == "id"
    assert config["write_disposition"] == "merge"

    # verify incremental loading is configured
    params = config["endpoint"]["params"]
    after_param = params["after"]
    assert after_param["type"] == "incremental"
    assert after_param["cursor_path"] == "start_date_local"
    assert after_param["initial_value"] == "2010-01-01 00:00:00+00:00"


def test_strava_activities_incremental_converter():
    """
    Test the incremental timestamp converter in strava_activities.

    This test ensures the lambda correctly converts a datetime string
    to a Unix timestamp, which is required by the Strava API's 'after' param.
    """
    config = strava_activities()
    convert = config["endpoint"]["params"]["after"]["convert"]

    # 2024-01-01 00:00:00 UTC = 1704067200
    result = convert("2024-01-01 00:00:00+00:00")
    assert result == 1704067200


def test_strava_stats_config_structure():
    """
    Test the strava_stats dltHub resource config.

    This test validates the structure and values of the athlete stats
    endpoint configuration, including the dynamic path construction.
    """
    athlete_id = 12345
    config = strava_stats(athlete_id)

    assert config["name"] == "stats"
    assert config["write_disposition"] == "replace"
    assert config["endpoint"]["path"] == f"athletes/{athlete_id}/stats"


def test_strava_stats_uses_single_page_paginator():
    """
    Test that strava_stats uses a SinglePagePaginator.

    The stats endpoint returns a single JSON object (not a paginated list),
    so a SinglePagePaginator is required to avoid pagination errors.
    """
    from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator

    config = strava_stats(99999)
    paginator = config["endpoint"]["paginator"]

    assert isinstance(paginator, SinglePagePaginator)
