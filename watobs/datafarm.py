from __future__ import annotations

import base64
import datetime
import json
import logging
import os
from functools import cached_property, wraps


import numpy as np
import pandas as pd
import pandas.io.json as pj
import pandera as pa
import requests

DateTime = str | datetime.datetime


def to_pandas_df(json_input: str) -> pd.DataFrame:
    columns = []
    data = json.loads(json_input)

    if "schema" not in data:
        raise ValueError("No schema in data")

    for f in data["schema"]["fields"]:
        columns.append(f["name"])
    if "data" not in data:
        return pd.DataFrame(columns=columns)

    df = pd.read_json(
        json.dumps(data["data"]), orient="values"
    )  # Create the dataframe from values - index is being created automatically, that should be ignored!
    df.columns = columns

    dict_fields = data["schema"]["fields"]
    df_fields = pj.build_table_schema(df, index=False)["fields"]
    for dict_field, df_field in zip(dict_fields, df_fields):
        if dict_field["type"] != df_field["type"]:
            if dict_field["type"] == "datetime":
                df.loc[:, dict_field["name"]] = pd.to_datetime(
                    df.loc[:, dict_field["name"]], unit="ms"
                )
            if dict_field["type"] == "number":
                df.loc[:, dict_field["name"]] = df.loc[:, dict_field["name"]].astype(
                    float
                )
    df.set_index(
        data["schema"]["primaryKey"][0], inplace=True
    )  # Setting index in actual dataframe, Assume index name from PK
    return df


def _parse_datetime(dt: str) -> str:
    """Return a datetime string in ISO8601 format: E.g. 2015-03-24T10:16:45.034Z"""
    datetime_obj = pd.to_datetime(dt)
    return datetime_obj.isoformat(timespec="milliseconds") + "Z"


def ensure_auth(func):
    """(Re)authenticate if access token is expired / not set"""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except requests.HTTPError as e:
            if e.response.status_code == 401 and self._connected:
                logging.info("Session expired. Reconnecting...")
                self.connect()
                return func(self, *args, **kwargs)
            else:
                raise e

    return wrapper


class DatafarmRepository:
    """Get timeseries data from Datafarm


    Examples
    ========
    >>> with DatafarmRepository(api_key="e11...") as datafarm:
    >>>     all_time_series = datafarm.list_time_series()
    >>>     time_series_data = datafarm.get_data(
    >>>         time_series=[
    >>>             "TNWB_wind_RVO-FUGRO_unfiltered_WS-130",
    >>>         ],
    >>>         range_start="2015-03-24T10:16:45.034Z",
    >>>         range_end="2023-03-24T10:16:45.034Z",
    >>>         limit_row_count=0,
    >>>         ascending=True,
    >>>     )

    """

    API_URL = "https://apidevtest.datafarm.work/api"

    INSERT_SCHEMA = pa.DataFrameSchema(
        {
            "TimeStamp": pa.Column(str),
            "QualityLevel": pa.Column(int),
            "Confidence": pa.Column(pa.Int, nullable=True, required=False),
            "Data": pa.Column(pa.Float, nullable=True, required=False),
            "Duration": pa.Column(pa.Int, nullable=True, required=False),
            "FilePath": pa.Column(pa.String, required=False),
        }
    )

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.access_token = None
        self.headers = None
        self._connected = False

    @ensure_auth
    def list_time_series(self) -> pd.DataFrame:
        url = self.API_URL + "/List/TimeSeries/"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        data = response.json()

        return to_pandas_df(json.dumps(data))

    @ensure_auth
    def get_data(
        self,
        time_series_id: str | list[str],
        start: DateTime = datetime.datetime(1900, 1, 1, 0, 0, 0, 0),
        end: DateTime = datetime.datetime.now(),
        fields: list[str] | None = None,
        qualities: list[str] | None = None,
        limit: int = 0,
        ascending: bool = True,
    ) -> pd.DataFrame:
        """Get data from Datafarm.

        Parameters
        ==========
        time_series_id : str or list of str
            The time series to get data from.
        start : DateTime, optional
            The start of the range to get data from.
        end : DateTime, optional
            The end of the range to get data from.
        fields : list of str, optional
            fields/columns to return
        qualities : list of str, optional
            Filter the data by qualities.
        limit : int, optional
            The maximum number of rows to return.
            Defaults to 0, which means no limit.
        ascending : bool, optional
            Whether to sort the data in ascending order.
            Defaults to True.
        """
        start = _parse_datetime(start)
        end = _parse_datetime(end)
        qualities = qualities or []
        fields = fields or []
        sort_order = "soAscending" if ascending else "soDescending"
        if isinstance(time_series_id, str):
            time_series_id = [time_series_id]

        url = self.API_URL + "/TimeSeries/ExtractData"
        body = {
            "TimeSeries": time_series_id,
            "ISO8601_TimeStamp": False,
            "LimitRowCount": limit,
            "Qualities": qualities,
            "RangeEnd": end,
            "RangeStart": start,
            "SortOrder": sort_order,
            "Fields": fields,
        }
        response = self.session.post(url, json=body, headers=self.headers)
        response.raise_for_status()

        data = response.json()[0]

        return to_pandas_df(json.dumps(data))

    @ensure_auth
    def insert_data(
        self, time_series_id: str, data: pd.DataFrame, bulk_insert: bool = False
    ) -> requests.Response:
        """Insert data into a time series."""
        body = self._get_insert_data_body(time_series_id, data, bulk_insert)
        endpoint = "/TimeSeries/InsertData"
        url = self.API_URL + endpoint
        response = self.session.post(url, json=body, headers=self.headers)
        response.raise_for_status()
        return response

    def _get_insert_data_body(
        self, time_series_id: str, data: pd.DataFrame, bulk_insert: bool = False
    ) -> dict:
        """Insert data into a time series.

        Parameters
        ==========
        time_series_id : str
            The time series to insert data into.
        data : pd.DataFrame
            The data to insert.
        bulk_insert : bool, optional
            Whether to use bulk insert.
            Defaults to False.
        """

        insert_data = self._prepare_insert_data(data)

        insert_data_dict = {col: list(insert_data[col]) for col in insert_data.columns}
        body = {
            "BulkInsert": bulk_insert,
            "TimeSeriesName": time_series_id,
            **insert_data_dict,
        }
        return body

    def _prepare_insert_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Prepare data for insertion. This includes converting the timestamp to ISO8601 format,
        converting the quality level to an integer, and checking that the data is valid.
        """
        insert_data = data.copy()

        if data.empty:
            raise ValueError("No data to insert")

        try:
            quality_column = next(
                col
                for col in insert_data.columns
                if col in ("Quality", "QualityLevel", "QualityTxt")
            )
        except StopIteration:
            raise ValueError("No quality column in data")

        if insert_data[quality_column].dtype in ("object", "string", "str"):
            logging.info("Converting quality names to quality level")
            try:
                insert_data[quality_column] = insert_data[quality_column].apply(
                    lambda x: self.quality_name_to_level[x]
                )
            except KeyError:
                raise KeyError(
                    f"Invalid quality string values. Must be in {self.quality_name_to_level.keys()} or an integer."
                )
        else:
            insert_data[quality_column] = insert_data[quality_column].astype(int)

        insert_data.rename(columns={quality_column: "QualityLevel"}, inplace=True)

        insert_data["Data"] = insert_data["Data"].astype(float)

        columns_schema = self.INSERT_SCHEMA.columns.keys()
        if not set(insert_data.columns).issubset(set(columns_schema)):
            logging.warning(
                f"Columns {set(insert_data.columns) - set(columns_schema)} not allowed to insert."
            )

        try:
            logging.info("Ensuring timestamps are in ISO8601 format")
            insert_data["TimeStamp"] = insert_data["TimeStamp"].apply(_parse_datetime)
        except KeyError:
            raise KeyError("No 'TimeStamp' column in data")
        except ValueError:
            raise ValueError("Invalid 'TimeStamp' column in data")

        self.INSERT_SCHEMA.validate(insert_data)

        if "FilePath" in insert_data.columns:
            logging.info("Converting file to base64")
            insert_data["ObjectFileName"] = insert_data["FilePath"].apply(
                lambda p: os.path.basename(p)
            )
            try:
                insert_data["ObjectBase64"] = insert_data["FilePath"].apply(
                    lambda p: base64.b64encode(open(p, "rb").read())
                )
            except FileNotFoundError as err:
                raise FileNotFoundError(
                    f"File {err.filename} not found. Please check the path."
                )
            del insert_data["FilePath"]

        if "Data" in insert_data.columns:
            insert_data["Data"] = insert_data["Data"].apply(self._format_float)

        if "Confidence" in insert_data.columns:
            insert_data["Confidence"] = insert_data["Confidence"].apply(
                self._format_float
            )

        if "Duration" in insert_data.columns:
            insert_data["Duration"] = insert_data["Duration"].apply(self._format_float)

        return insert_data

    @ensure_auth
    def delete_data(
        self,
        time_series_id: str,
        timestamps: list[DateTime] | None = None,
        start: DateTime | None = None,
        end: DateTime | None = None,
    ) -> requests.Response:
        """Delete data from a time series. Either timestamps or start and end must be provided.

        Parameters
        ----------
        time_series_id : str
            The time series to delete data from.
        timestamps : list of DateTime objects, optional
            The timestamps to delete.
        start : DateTime, optional
            The start of the range to delete data from.
        end : DateTime, optional
            The end of the range to delete data from.
            Note that this is NOT inclusive.
        """
        if timestamps is None and (start is None or end is None):
            raise ValueError("Either timestamps or start and end must be provided.")
        if timestamps is not None and (start is not None or end is not None):
            raise ValueError("Either timestamps or start and end must be provided.")
        if timestamps is not None:
            return self._delete_data_timestamps(time_series_id, timestamps)

        return self._delete_data_range(time_series_id, start, end)

    def _delete_data_timestamps(
        self, time_series_id: str, timestamps: list[DateTime]
    ) -> requests.Response:
        """Delete data from a time series by timestamps.

        Parameters
        ----------
        time_series_id : str
            The time series to delete data from.
        timestamps : list of DateTime objects
            The timestamps to delete.
        """
        endpoint = "/TimeSeries/DeleteData"
        url = self.API_URL + endpoint
        body = {
            "TimeSeriesName": time_series_id,
            "TimeStamp": [_parse_datetime(ts) for ts in timestamps],
        }
        response = self.session.post(url, json=body, headers=self.headers)
        response.raise_for_status()
        return response

    def _delete_data_range(
        self, time_series_id: str, start: DateTime, end: DateTime
    ) -> requests.Response:
        """Delete data from a time series by range [start, end).

        Parameters
        ----------
        time_series_id : str
            The time series to delete data from.
        start : DateTime
            The start of the range to delete data from.
        end : DateTime
            The end of the range to delete data from.
            Note that this is NOT inclusive.
        """
        endpoint = "/TimeSeries/DeleteDataRange"
        url = self.API_URL + endpoint
        body = {
            "TimeSeriesName": time_series_id,
            "RangeStart": _parse_datetime(start),
            "RangeFinish": _parse_datetime(end),
        }
        response = self.session.post(url, json=body, headers=self.headers)
        response.raise_for_status()
        return response

    def update_data_quality(
        self,
        time_series_id: str,
        timestamps: list[DateTime],
        quality: int | str | list[int] | list[str],
    ) -> requests.Response:
        """Update the quality of data in a time series.

        Parameters
        ----------
        time_series_id : str
            The time series to update.
        timestamps : list of str or list of datetime.datetime
            The timestamps to update.
        quality : int or str or list of int or list of str
            The qualities to set.
        """
        if not isinstance(quality, list):
            quality = [quality] * len(timestamps)
        if not len(timestamps) == len(quality):
            raise ValueError("The number of timestamps and qualities must be the same.")

        if type(quality[0]) == str:
            try:
                new_qualities = [int(self.quality_name_to_level[q]) for q in quality]
            except KeyError:
                raise ValueError(
                    "Quality must be one of: {}".format(
                        ", ".join(self.quality_name_to_level.keys())
                    )
                )
        else:
            new_qualities = quality

        endpoint = "/TimeSeries/UpdateDataQuality"
        url = self.API_URL + endpoint
        body = {
            "TimeSeriesName": time_series_id,
            "TimeStamp": [_parse_datetime(ts) for ts in timestamps],
            "QualityLevel": new_qualities,
        }
        response = self.session.post(url, json=body, headers=self.headers)
        response.raise_for_status()
        return response

    @ensure_auth
    def get_statistics(self, time_series_id: str | list[str]) -> pd.DataFrame:
        """Get statistics for a time series or a list of time series.

        Parameters
        ==========
        time_series_id : str or list of str
            The time series to get statistics for.
        """
        endpoint = "/TimeSeries/Statistics"
        url = self.API_URL + endpoint
        if isinstance(time_series_id, str):
            time_series_id = [time_series_id]
        body = {"TimeSeries": list(time_series_id), "ISO8601_Timestamp": True}
        response = self.session.post(url, json=body, headers=self.headers)
        response.raise_for_status()
        data = response.json()

        return to_pandas_df(json.dumps(data))

    @cached_property
    @ensure_auth
    def time_series_metadata(self) -> pd.DataFrame:
        endpoint = "/MetaData/Entity"
        params = {"aClassId": "Timeseries"}
        return self._get_pandas_df(endpoint, params)

    @cached_property
    @ensure_auth
    def time_series_source_descriptions(self) -> pd.DataFrame:
        endpoint = "/List/TimeSeriesSourceDescriptions"
        return self._get_pandas_df(endpoint)

    @cached_property
    @ensure_auth
    def units(self) -> pd.DataFrame:
        endpoint = "/List/Units"
        return self._get_pandas_df(endpoint)

    @cached_property
    @ensure_auth
    def time_series_types(self) -> pd.DataFrame:
        endpoint = "/List/TimeSeriesTypes"
        return self._get_pandas_df(endpoint)

    @cached_property
    @ensure_auth
    def time_series_status(self) -> pd.DataFrame:
        endpoint = "/List/TimeSeriesStatus"
        return self._get_pandas_df(endpoint)

    @cached_property
    @ensure_auth
    def qualities(self) -> pd.DataFrame:
        endpoint = "/List/Qualities"
        return self._get_pandas_df(endpoint)

    @cached_property
    @ensure_auth
    def parameters(self) -> pd.DataFrame:
        endpoint = "/List/Parameters"
        return self._get_pandas_df(endpoint)

    @cached_property
    @ensure_auth
    def medias(self) -> pd.DataFrame:
        endpoint = "/List/Medias"
        return self._get_pandas_df(endpoint)

    @cached_property
    @ensure_auth
    def locations(self) -> pd.DataFrame:
        endpoint = "/List/Locations"
        return self._get_pandas_df(endpoint)

    @cached_property
    @ensure_auth
    def quality_level_to_name(self) -> dict[int, str]:
        df = self.qualities
        return {df["Level"].iloc[i]: df["IDName"].iloc[i] for i in range(len(df))}

    @cached_property
    @ensure_auth
    def quality_name_to_level(self) -> dict[str, int]:
        df = self.qualities
        return {df["IDName"].iloc[i]: df["Level"].iloc[i] for i in range(len(df))}

    def connect(self) -> None:
        """Connect to the Datafarm API."""
        url = self.API_URL + "/Login/Login"
        data = {"Token": self.api_key}
        response = self.session.post(url, json=data)
        response.raise_for_status()
        try:
            self.access_token = response.headers["Access-Token"]
        except KeyError:
            raise KeyError(
                f"Could not get access token. Check that your API key is correct."
            )
        self.headers = {"Access-Token": self.access_token}
        self._connected = True

    def close(self) -> None:
        """Close the connection to the Datafarm API."""
        url = self.API_URL + "/Login/Logoff"
        response = self.session.post(url, headers=self.headers)
        response.raise_for_status()
        self.access_token = None
        self.headers = None
        self._connected = False

    def _get_pandas_df(self, endpoint: str, params: dict | None = None) -> pd.DataFrame:
        url = self.API_URL + endpoint
        r = requests.get(url, headers=self.headers, params=params)
        r.raise_for_status()
        data = r.json()
        return to_pandas_df(json.dumps(data))

    @staticmethod
    def _format_float(x: float | None) -> dict[str, float]:
        """Format a float for JSON serialization."""
        if x is None or np.isnan(x):
            return {"N": 1, "V": 0.0}
        return {"N": 0, "V": float(x)}

    def __enter__(self) -> DatafarmRepository:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
