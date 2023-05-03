import requests
import json
import pandas as pd
import pandas.io.json as pj


def to_pandas_df(json_input):
    ac = []  # Column names
    o = json.loads(json_input)
    for f in o["schema"]["fields"]:
        ac.append(f["name"])
    if "data" not in o:
        return pd.DataFrame(columns=ac)

    df = pd.read_json(
        json.dumps(o["data"]), orient="values"
    )  # Create the dataframe from values - index is being created automatically, that should be ignored!
    df.columns = ac  # Rename columns to correct names
    zipped = zip(
        o["schema"]["fields"], pj.build_table_schema(df, index=False)["fields"]
    )  # ignoring index from the newly created frame
    for m, l in zipped:
        if m["type"] != l["type"]:
            if m["type"] == "datetime":
                df.loc[:, m["name"]] = pd.to_datetime(df.loc[:, m["name"]], unit="ms")
            if m["type"] == "number":
                df.loc[:, m["name"]] = df.loc[:, m["name"]].astype(float)
    df.set_index(
        o["schema"]["primaryKey"][0], inplace=True
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
        time_series,
        range_start,
        range_end,
        iso8601_timestamp=True,  # TODO: Do we need this?
        fields=None,  # TODO: Do we need this?
        qualities=None,  # TODO: Do we need this?
        limit_row_count=0,
        ascending=True,
    ):
        """Get data from Datafarm.

        Parameters
        ==========
        time_series : str or list of str
            The time series to get data from.
        range_start : str
            The start of the range to get data from.
        range_end : str
            The end of the range to get data from.
        iso8601_timestamp : bool, optional
            Whether to use ISO8601 timestamps.
            Defaults to True.
        fields : list of str, optional
            TODO: add description
        qualities : list of str, optional
            TODO: add description
        limit_row_count : int, optional
            The maximum number of rows to return.
            Defaults to 0, which means no limit.
        ascending : bool, optional
            Whether to sort the data in ascending order.
            Defaults to True.
        """
        qualities = qualities or []
        fields = fields or []
        sort_order = "soAscending" if ascending else "soDescending"
        if isinstance(time_series, str):
            time_series = [time_series]

        url = self.API_URL + "/TimeSeries/ExtractData"
        body = {
            "TimeSeries": time_series,
            "ISO8601_TimeStamp": iso8601_timestamp,
            "LimitRowCount": limit_row_count,
            "Qualities": qualities,
            "RangeEnd": range_end,
            "RangeStart": range_start,
            "SortOrder": sort_order,
            # "Fields": fields,   TODO: add fields
        }
        response = self.session.post(url, json=body, headers=self.headers)
        response.raise_for_status()

        data = response.json()[0]
        assert data is not None

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
        self.access_token = response.headers["Access-Token"]
        self.headers = {"Access-Token": self.access_token}
        return self.access_token

    def close(self):
        """Close the connection to the Datafarm API."""
        url = self.API_URL + "/Login/Logoff"
        response = self.session.post(url, headers=self.headers)
        response.raise_for_status()

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
            time_series=[
                # "Bor1_currents_RVO-FUGRO_derived_CS",
                "TNWB_wind_RVO-FUGRO_unfiltered_WS-130",
            ],
            iso8601_timestamp=False,
            range_start="2015-03-24T10:16:45.034Z",
            range_end="2023-03-24T10:16:45.034Z",
            limit_row_count=10,
        )
        print(data)
