import base64
import datetime
import json
from functools import cached_property
import logging
import os
from typing import Dict, List, Optional, Union
import numpy as np

import pandas as pd
import pandas.io.json as pj
import requests
import pandera as pa


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
    return datetime_obj.isoformat() + "Z"


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

    def __init__(self, api_key):
        self.api_key = api_key
        self.session = requests.Session()
        self.access_token = None
        self.headers = None

    def list_time_series(self) -> pd.DataFrame:
        url = self.API_URL + "/List/TimeSeries/"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        data = response.json()

        return to_pandas_df(json.dumps(data))

    def get_data(
        self,
        time_series_id,
        start=datetime.datetime(1900, 1, 1, 0, 0, 0, 0),
        end=datetime.datetime.now(),
        fields=None,
        qualities=None,
        limit=0,
        ascending=True,
    ):
        """Get data from Datafarm.

        Parameters
        ==========
        time_series_id : str or list of str
            The time series to get data from.
        start : str | datetime.datetime
            The start of the range to get data from.
        end : str | datetime.datetime, optional
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

    def insert_data(
        self, time_series_id: str, data: pd.DataFrame, bulk_insert: bool = False
    ):
        """Insert data into a time series."""
        body = self._get_insert_data_body(time_series_id, data, bulk_insert)
        endpoint = "/TimeSeries/InsertData"
        url = self.API_URL + endpoint
        response = self.session.post(url, json=body, headers=self.headers)
        response.raise_for_status()
        return response

    def _get_insert_data_body(
        self, time_series_id: str, data: pd.DataFrame, bulk_insert: bool = False
    ):
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

    def _prepare_insert_data(self, data: pd.DataFrame):
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

    def delete_data(self, time_series_id, timestamps=None, start=None, end=None):
        """Delete data from a time series. Either timestamps or start and end must be provided.

        Parameters
        ----------
        time_series_id : str
            The time series to delete data from.
        timestamps : list of str | datetime.datetime objects, optional
            The timestamps to delete.
        start : str | datetime.datetime, optional
            The start of the range to delete data from.
        end : str | datetime.datetime, optional
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

    def _delete_data_timestamps(self, time_series_id, timestamps):
        """Delete data from a time series by timestamps.

        Parameters
        ----------
        time_series_id : str
            The time series to delete data from.
        timestamps : list of str | datetime.datetime objects
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

    def _delete_data_range(self, time_series_id, start, end):
        """Delete data from a time series by range [start, end).

        Parameters
        ----------
        time_series_id : str
            The time series to delete data from.
        start : str | datetime.datetime
            The start of the range to delete data from.
        end : str | datetime.datetime
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

    def get_statistics(self, time_series_id: Union[str, List[str]]) -> pd.DataFrame:
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

    def _get_file_base64(self, file_paths):
        file_exists = [os.path.exists(x) for x in file_paths]
        if not all(file_exists):
            raise ValueError(
                f"File does not exist: {file_paths[~np.array(file_exists)]}"
            )
        file_base64 = [base64.b64encode(open(x, "rb").read()) for x in file_paths]
        file_names = [os.path.basename(x) for x in file_paths]
        return file_base64, file_names

    @cached_property
    def time_series_metadata(self):
        endpoint = "/MetaData/Entity"
        params = {"aClassId": "Timeseries"}
        return self._get_pandas_df(endpoint, params)

    @cached_property
    def time_series_source_descriptions(self):
        endpoint = "/List/TimeSeriesSourceDescriptions"
        return self._get_pandas_df(endpoint)

    @cached_property
    def units(self):
        endpoint = "/List/Units"
        return self._get_pandas_df(endpoint)

    @cached_property
    def time_series_types(self):
        endpoint = "/List/TimeSeriesTypes"
        return self._get_pandas_df(endpoint)

    @cached_property
    def time_series_status(self):
        endpoint = "/List/TimeSeriesStatus"
        return self._get_pandas_df(endpoint)

    @cached_property
    def qualities(self):
        endpoint = "/List/Qualities"
        return self._get_pandas_df(endpoint)

    @cached_property
    def parameters(self):
        endpoint = "/List/Parameters"
        return self._get_pandas_df(endpoint)

    @cached_property
    def medias(self):
        endpoint = "/List/Medias"
        return self._get_pandas_df(endpoint)

    @cached_property
    def locations(self):
        endpoint = "/List/Locations"
        return self._get_pandas_df(endpoint)

    @cached_property
    def quality_level_to_name(self):
        df = self.qualities
        return {df["Level"].iloc[i]: df["IDName"].iloc[i] for i in range(len(df))}

    @cached_property
    def quality_name_to_level(self):
        df = self.qualities
        return {df["IDName"].iloc[i]: df["Level"].iloc[i] for i in range(len(df))}

    def connect(self):
        """Connect to the Datafarm API."""
        url = self.API_URL + "/Login/Login"
        data = {"Token": self.api_key}
        response = self.session.post(url, json=data)
        response.raise_for_status()
        try:
            self.access_token = response.headers["Access-Token"]
        except KeyError:
            raise KeyError(
                f"resopnse.json() = {response.json()} Could not get access token. Check that your API key is correct."
            )
        self.headers = {"Access-Token": self.access_token}

    def close(self):
        """Close the connection to the Datafarm API."""
        url = self.API_URL + "/Login/Logoff"
        response = self.session.post(url, headers=self.headers)
        response.raise_for_status()
        self.access_token = None
        self.headers = None

    def _get_pandas_df(self, endpoint, params=None):
        url = self.API_URL + endpoint
        r = requests.get(url, headers=self.headers, params=params)
        r.raise_for_status()
        data = r.json()
        return to_pandas_df(json.dumps(data))

    @staticmethod
    def _format_float(x: Optional[float]) -> Dict[str, Union[int, float]]:
        """Format a float for JSON serialization."""
        if x is None or np.isnan(x):
            return {"N": 1, "V": 0.0}
        return {"N": 0, "V": x}

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
