import json
import os
import pandas as pd
import pytest
from watobs.datafarm import DatafarmRepository, to_pandas_df


def requires_DATAFARM_API_KEY():
    api_key = os.environ.get("DATAFARM_API_KEY")
    reason = "Environment variable DATAFARM_API_KEY not present"
    return pytest.mark.skipif(api_key is None, reason=reason)


@pytest.fixture
def repo() -> DatafarmRepository:
    api_key = os.environ.get("DATAFARM_API_KEY")
    assert api_key is not None
    dfr = DatafarmRepository(api_key)
    dfr.connect()
    return dfr


@pytest.fixture
def json_input() -> str:
    data = {
        "schema": {
            "fields": [
                {"name": "GUID", "type": "string"},
                {"name": "ID", "type": "integer"},
                {"name": "EntityID", "type": "string"},
                {"name": "Touched", "type": "datetime"},
            ],
            "primaryKey": ["GUID"],
            "pandas_version": "0.20.0",
        },
        "data": [
            [
                "{62F60AF2-C34A-11ED-B2F7-1831BF2DC749}",
                2,
                "AKZ_waves_CMEMS_unfiltered_Hm0",
                1679332722000,
            ]
        ],
    }

    return json.dumps(data)


@pytest.fixture
def json_input_empty() -> str:
    data = {
        "schema": {
            "fields": [
                {"name": "GUID", "type": "string"},
                {"name": "ID", "type": "integer"},
                {"name": "EntityID", "type": "string"},
                {"name": "Touched", "type": "datetime"},
            ],
            "primaryKey": ["GUID"],
            "pandas_version": "0.20.0",
        }
    }

    return json.dumps(data)


@requires_DATAFARM_API_KEY()
def test_list_time_series(repo):
    assert repo.access_token is not None
    assert repo.list_time_series() is not None


@requires_DATAFARM_API_KEY()
def test_connection(repo: DatafarmRepository):
    assert repo.access_token is not None
    repo.close()
    assert repo.access_token is None
    assert repo.headers is None
    repo.connect()
    assert repo.access_token is not None
    assert repo.headers is not None


@requires_DATAFARM_API_KEY()
def test_get_data(repo: DatafarmRepository):
    time_series = "TNWB_wind_RVO-FUGRO_unfiltered_WS-130"
    data = repo.get_data(
        time_series_id=[time_series],
        iso8601_timestamp=False,
        start="2015-03-24T10:16:45.034Z",
        end="2023-03-24T10:16:45.034Z",
        limit=10,
    )
    assert data is not None
    assert isinstance(data, pd.DataFrame)
    assert data.shape == (10, 2)
    assert data.columns.tolist() == ["Data", "QualityTxt"]
    assert data.index.name == "RefDateTimeRef"


@requires_DATAFARM_API_KEY()
def test_get_data_5_rows(repo: DatafarmRepository):
    time_series = "TNWB_wind_RVO-FUGRO_unfiltered_WS-130"
    data = repo.get_data(
        time_series_id=[time_series],
        iso8601_timestamp=False,
        start="2015-03-24T10:16:45.034Z",
        end="2023-03-24T10:16:45.034Z",
        limit=5,
    )
    assert data.shape == (5, 2)


@requires_DATAFARM_API_KEY()
def test_no_data(repo: DatafarmRepository):
    time_series_no_data = "Bor1_currents_RVO-FUGRO_derived_CS"
    data = repo.get_data(
        time_series_id=[time_series_no_data],
        iso8601_timestamp=False,
        start="2015-03-24T10:16:45.034Z",
        end="2023-03-24T10:16:45.034Z",
        limit=10,
    )
    assert isinstance(data, pd.DataFrame)
    assert data.empty


def test_to_dataframe(json_input):
    df = to_pandas_df(json_input)
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 3)
    assert df.columns.tolist() == ["ID", "EntityID", "Touched"]
    assert df.index.name == "GUID"
    assert df["Touched"].dtype == "datetime64[ns]"


def test_to_dataframe_empty(json_input_empty):
    df = to_pandas_df(json_input_empty)
    assert isinstance(df, pd.DataFrame)
    assert df.empty
    assert df.shape == (0, 4)
    assert df.columns.tolist() == ["GUID", "ID", "EntityID", "Touched"]


def test_to_dataframe_error():
    with pytest.raises(ValueError):
        to_pandas_df("{}")
