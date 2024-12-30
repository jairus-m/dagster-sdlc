from dotenv import load_dotenv

from dagster import Definitions
import pytest

load_dotenv()  # load env vars


def test_definitions_import():
    """
    Test to ensure that the Definitions object can be imported without errors.

    This test checks for any failures that would prevent the loading of the
    code location (like syntax errors, missing imports, etc).
    """
    try:
        from dagster_proj import defs

        assert isinstance(defs, Definitions)
    except Exception as e:
        pytest.fail(f"Definitions import failed with error: {e}")


def test_graph_construction():
    """
    Test to verify that the job graph can be constructed without errors.

    This test attempts to retrieve and construct the specified job from the
    Definitions object. It checks for issues such as missing job definitions
    or errors in the graph structure.
    """
    from dagster_proj import defs

    try:
        # attempt to construct the graph or job
        defs.get_job_def("activities_update_job")
    except Exception as e:
        pytest.fail(f"Graph construction failed with error: {e}")
