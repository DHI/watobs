import os
import pytest
from watobs.datafarm import DatafarmRepository


@pytest.fixture
def repo():
    api_key = os.getenv("DATAFARM_API_KEY")
    assert api_key is not None
    dfr = DatafarmRepository(api_key)
    dfr.connect()
    return dfr


def test_list_time_series(repo):
    assert repo.access_token is not None
    assert repo.list_time_series() is not None
