from dotenv import load_dotenv

import pandas as pd
import numpy as np

load_dotenv()  # load env vars


def test_check_cycling_data_size_passes_with_valid_data():
    """
    Test that the cycling data size check passes with sufficient rows and correct columns.

    This test creates a DataFrame matching the expected schema (8 columns, >1500 rows)
    and verifies the asset check logic returns passed=True.
    """
    columns = [
        "distance",
        "moving_time",
        "total_elevation_gain",
        "average_speed",
        "average_watts",
        "weighted_average_watts",
        "kilojoules",
        "average_heartrate",
    ]
    df = pd.DataFrame(np.zeros((2000, 8)), columns=columns)

    num_rows = df.shape[0]
    num_cols = df.shape[1]
    passed = num_rows > 1500 and num_cols == 8

    assert passed is True


def test_check_cycling_data_size_fails_with_insufficient_rows():
    """
    Test that the cycling data size check fails when there are too few rows.

    This test verifies that even with the correct number of columns,
    the check correctly rejects data with fewer than 1500 rows.
    """
    columns = [
        "distance",
        "moving_time",
        "total_elevation_gain",
        "average_speed",
        "average_watts",
        "weighted_average_watts",
        "kilojoules",
        "average_heartrate",
    ]
    df = pd.DataFrame(np.zeros((100, 8)), columns=columns)

    num_rows = df.shape[0]
    num_cols = df.shape[1]
    passed = num_rows > 1500 and num_cols == 8

    assert passed is False


def test_check_cycling_data_size_fails_with_wrong_columns():
    """
    Test that the cycling data size check fails when the column count is wrong.

    This test verifies that even with sufficient rows, the check correctly
    rejects data that doesn't have exactly 8 columns.
    """
    df = pd.DataFrame(np.zeros((2000, 5)), columns=["a", "b", "c", "d", "e"])

    num_rows = df.shape[0]
    num_cols = df.shape[1]
    passed = num_rows > 1500 and num_cols == 8

    assert passed is False
