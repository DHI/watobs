from __future__ import annotations

import base64
import datetime
import json
import os
import pandas as pd
import pytest
from watobs.datafarm import DatafarmRepository, _parse_datetime, to_pandas_df


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
def test_list_time_series(repo: DatafarmRepository):
    assert len(repo.list_time_series()) > 2000


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


def test_parse_datetime_valid():
    dt = "2023-05-15T14:30:00"
    result = _parse_datetime(dt)
    expected = "2023-05-15T14:30:00.000Z"
    assert result == expected


def test_parse_datetime_invalid():
    dt = "2023-50-50"
    with pytest.raises(ValueError):
        _parse_datetime(dt)


def test_parse_datetime_other_formate():
    dt = "05/15/2023 14:30:00"
    result = _parse_datetime(dt)
    expected = "2023-05-15T14:30:00.000Z"
    assert result == expected


def test_parse_datetime_object():
    dt = datetime.datetime(2023, 5, 15, 14, 30, 0)
    result = _parse_datetime(dt)
    expected = "2023-05-15T14:30:00.000Z"
    assert result == expected


@requires_DATAFARM_API_KEY()
def test_units(repo: DatafarmRepository):
    units = repo.units
    assert units is not None
    assert isinstance(units, pd.DataFrame)
    assert "IDName" in units.columns.tolist()
    assert "l/min" in units["IDName"].tolist()


@requires_DATAFARM_API_KEY()
def test_time_series_source_descriptions(repo: DatafarmRepository):
    time_series_source_descriptions = repo.time_series_source_descriptions
    assert time_series_source_descriptions is not None
    assert isinstance(time_series_source_descriptions, pd.DataFrame)
    assert "IDName" in time_series_source_descriptions.columns.tolist()
    assert "LEG_wl_RWS" in time_series_source_descriptions["IDName"].tolist()


@requires_DATAFARM_API_KEY()
def test_time_series_types(repo: DatafarmRepository):
    time_series_types = repo.time_series_types
    assert time_series_types is not None
    assert isinstance(time_series_types, pd.DataFrame)
    assert "IDName" in time_series_types.columns.tolist()
    assert "BSH-dat-meteo" in time_series_types["IDName"].tolist()


@requires_DATAFARM_API_KEY()
def test_time_series_status(repo: DatafarmRepository):
    time_series_status = repo.time_series_status
    assert time_series_status is not None
    assert isinstance(time_series_status, pd.DataFrame)
    assert "IDName" in time_series_status.columns.tolist()
    assert "new" in time_series_status["IDName"].tolist()


@requires_DATAFARM_API_KEY()
def test_qualities(repo: DatafarmRepository):
    qualities = repo.qualities
    assert qualities is not None
    assert isinstance(qualities, pd.DataFrame)
    assert "IDName" in qualities.columns.tolist()
    assert "ok" in qualities["IDName"].tolist()


@requires_DATAFARM_API_KEY()
def test_parameters(repo: DatafarmRepository):
    parameters = repo.parameters
    assert parameters is not None
    assert isinstance(parameters, pd.DataFrame)
    assert "IDName" in parameters.columns.tolist()
    assert "Lat" in parameters["IDName"].tolist()
    assert "Lon" in parameters["IDName"].tolist()


@requires_DATAFARM_API_KEY()
def test_medias(repo: DatafarmRepository):
    medias = repo.medias
    assert medias is not None
    assert isinstance(medias, pd.DataFrame)
    assert "IDName" in medias.columns.tolist()
    assert "waves" in medias["IDName"].tolist()


@requires_DATAFARM_API_KEY()
def test_locations(repo: DatafarmRepository):
    assert hasattr(repo, "locations")
    locations = repo.locations
    assert locations is not None
    assert isinstance(locations, pd.DataFrame)
    assert "IDName" in locations.columns.tolist()
    assert "Bor1" in locations["IDName"].tolist()


@requires_DATAFARM_API_KEY()
def test_time_series_insert_data(repo: DatafarmRepository):
    assert hasattr(repo, "insert_data")
    import pandas as pd

    data = pd.DataFrame(
        {
            "TimeStamp": [
                "2020-01-01T00:00:00Z",
                pd.to_datetime("2020-01-01T00:00:00Z"),
                pd.to_datetime("2020-01-01T12:00:00Z"),
            ],
            "QualityTxt": ["ok", "ok", "critical"],
            "Data": [1.23, None, 3],
        }
    )

    body = repo._get_insert_data_body(
        time_series_id="test",
        data=data,
    )
    assert body is not None
    assert isinstance(body, dict)
    assert body["Data"][0] == {"N": 0, "V": 1.23}
    assert body["Data"][1] == {"N": 1, "V": 0}


@requires_DATAFARM_API_KEY()
def test_time_series_insert_data_file(repo: DatafarmRepository):
    data = pd.DataFrame(
        {
            "TimeStamp": [
                "2020-01-01T00:00:00Z",
                pd.to_datetime("2020-01-01T00:00:00Z"),
                pd.to_datetime("2020-01-01T12:00:00Z"),
            ],
            "QualityTxt": ["ok", "ok", "critical"],
            "Data": [1.23, None, 3],
            "FilePath": [
                "tests/data/test_upload.txt",
                "tests/data/test_upload.txt",
                "tests/data/test_upload.txt",
            ],
        }
    )
    body = repo._get_insert_data_body(
        time_series_id="test",
        data=data,
    )
    assert body is not None
    assert isinstance(body, dict)
    assert body["ObjectFileName"] == [
        "test_upload.txt",
        "test_upload.txt",
        "test_upload.txt",
    ]
    assert isinstance(body["ObjectBase64"][0], bytes)
    assert body["ObjectBase64"] != b"Hello world"
    assert base64.b64decode(body["ObjectBase64"][0]) == b"Hello world"


@requires_DATAFARM_API_KEY()
def test_insert_data(repo):
    import random

    random_date = lambda: pd.Timestamp.now() - pd.Timedelta(
        days=3000 + random.randint(1, 1000)
    )
    rows = 2
    data = pd.DataFrame(
        {
            "TimeStamp": [random_date() for _ in range(rows)],
            "Data": [random.random() for _ in range(rows)],
            "Quality": ["ok"] * rows,
        }
    )
    data
    response = repo.insert_data("testapi.insert", data, bulk_insert=True)
    assert response is not None
    assert response.status_code == 200
    assert response.request.method == "POST"
    body = json.loads(response.request.body)
    assert body["TimeSeriesName"] == "testapi.insert"
    assert body["Data"][0]["N"] == 0
    assert body["QualityLevel"][0] == 0


@requires_DATAFARM_API_KEY()
def test_time_series_delete_data(repo: DatafarmRepository):
    assert hasattr(repo, "delete_data")

    timestamp = pd.Timestamp("2100-01-01T00:00:00Z")
    delta = pd.Timedelta(microseconds=1)

    data = pd.DataFrame(
        {
            "TimeStamp": [timestamp],
            "Data": [1],
            "Quality": ["ok"],
        }
    )
    repo.insert_data("testapi.insert", data, bulk_insert=True)
    res = repo.get_data("testapi.insert", start=timestamp, end=timestamp + delta)
    assert len(res) == 1

    repo.delete_data("testapi.insert", timestamps=[timestamp])
    res = repo.get_data("testapi.insert", start=timestamp, end=timestamp + delta)
    assert len(res) == 0


@requires_DATAFARM_API_KEY()
def test_time_series_update_data_quality(repo: DatafarmRepository):
    assert hasattr(repo, "update_data_quality")

    timestamp = pd.Timestamp("2100-01-01T00:00:00Z")
    delta = pd.Timedelta(microseconds=1)

    data = pd.DataFrame(
        {
            "TimeStamp": [timestamp],
            "Data": [1],
            "Quality": ["ok"],
        }
    )
    repo.insert_data("testapi.insert", data, bulk_insert=True)
    res = repo.get_data("testapi.insert", start=timestamp, end=timestamp + delta)
    assert len(res) == 1
    assert res["QualityTxt"][0] == "ok"

    repo.update_data_quality(
        "testapi.insert", timestamps=[timestamp], quality="critical"
    )
    res = repo.get_data("testapi.insert", start=timestamp, end=timestamp + delta)
    assert len(res) == 1
    assert res["QualityTxt"][0] == "critical"
