import requests
import json
import pandas as pd
import pandas.io.json as pj


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

    def __init__(self, api_key):
        self.api_key = api_key
        self.session = requests.Session()
        self.access_token = None
        self.headers = None

    def list_time_series(self, fields=None):
        url = self.API_URL + "/TimeSeries/List"
        fields = fields or []
        data = {"Fields": fields}
        response = requests.post(url, headers=self.headers)
        response.raise_for_status()
        data = response.json()

        return to_pandas_df(json.dumps(data))

    def get_data(
        self,
        time_series_id,
        start,
        end,
        iso8601_timestamp=True,
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
        start : str
            The start of the range to get data from.
        end : str
            The end of the range to get data from.
        iso8601_timestamp : bool, optional
            Whether to use ISO8601 timestamps.
            Defaults to True.
        fields : list of str, optional
            TODO: add description
        qualities : list of str, optional
            TODO: add description
        limit : int, optional
            The maximum number of rows to return.
            Defaults to 0, which means no limit.
        ascending : bool, optional
            Whether to sort the data in ascending order.
            Defaults to True.
        """
        qualities = qualities or []
        fields = fields or []
        sort_order = "soAscending" if ascending else "soDescending"
        if isinstance(time_series_id, str):
            time_series_id = [time_series_id]

        url = self.API_URL + "/TimeSeries/ExtractData"
        body = {
            "TimeSeries": time_series_id,
            "ISO8601_TimeStamp": iso8601_timestamp,
            "LimitRowCount": limit,
            "Qualities": qualities,
            "RangeEnd": end,
            "RangeStart": start,
            "SortOrder": sort_order,
            # "Fields": fields,   TODO: add fields
        }
        response = self.session.post(url, json=body, headers=self.headers)
        response.raise_for_status()

        data = response.json()[0]

        return to_pandas_df(json.dumps(data))

    @property
    def time_series_metadata(self):
        r = requests.get(
            self.API_URL + "/MetaData/Entity",
            headers=self.headers,
            params={"aClassId": "Timeseries"},
        )
        r.raise_for_status()
        data = r.json()
        return to_pandas_df(json.dumps(data))

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

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


if __name__ == "__main__":
    import os

    import dotenv

    dotenv.load_dotenv()
    api_key = os.getenv("DATAFARM_API_KEY")
    assert api_key is not None

    with DatafarmRepository(api_key) as dfr:
        assert dfr.access_token is not None
        print(dfr.list_time_series())
        print(dfr.time_series_metadata)
        time_series = "TNWB_wind_RVO-FUGRO_unfiltered_WS-130"
        data = dfr.get_data(
            time_series_id=[
                # "Bor1_currents_RVO-FUGRO_derived_CS",
                "TNWB_wind_RVO-FUGRO_unfiltered_WS-130",
            ],
            iso8601_timestamp=False,
            start="2015-03-24T10:16:45.034Z",
            end="2023-03-24T10:16:45.034Z",
            limit=10,
        )
        print(data)
