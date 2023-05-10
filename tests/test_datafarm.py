import os
import pytest
from watobs.datafarm import DatafarmRepository


def requires_DATAFARM_API_KEY():
    api_key = os.environ.get("DATAFARM_API_KEY")
    reason = "Environment variable DATAFARM_API_KEY not present"
    return pytest.mark.skipif(api_key is None, reason=reason)


@pytest.fixture
def repo():
    api_key = os.environ.get("DATAFARM_API_KEY")
    assert api_key is not None
    dfr = DatafarmRepository(api_key)
    dfr.connect()
    return dfr


@requires_DATAFARM_API_KEY()
def test_list_time_series(repo):
    assert repo.access_token is not None
    assert repo.list_time_series() is not None
