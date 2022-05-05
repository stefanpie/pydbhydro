from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import datetime
import os
import shutil
from io import StringIO

from joblib import Parallel, delayed
from tqdm import tqdm


def ddmmss_to_dec(x):
    d = x//10000
    m = x//100 - d*100
    s = x - (x//100)*100
    dd = d + m/60 + s/3600
    return dd


class DBHYDRO:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.get("https://my.sfwmd.gov/dbhydroplsql/show_dbkey_info.main_menu")

    def get_all_stations(self, staion_name=None) -> pd.DataFrame:

        name_search_param = "%"
        if staion_name:
            name_search_param = staion_name

        url = "https://my.sfwmd.gov/dbhydroplsql/show_dbkey_info.show_station_info"
        params = {"v_station": name_search_param}
        r = self.session.get(url, params=params)

        table_df = pd.read_html(r.text)[2]

        remove_cols = ["GetData", "ShowMap", "NearbyStations",
                       "Attachments",
                       'X Coord(ft)', 'Y Coord(ft)',
                       "Sec", "Twp", "Rng"]
        table_df.drop(columns=remove_cols, inplace=True)

        new_column_names = ["station", "site", "type",
                            "lat", "lon", "county", "basin",
                            "description"]
        table_df.columns = new_column_names

        table_df = table_df.replace(r'^\s*$', np.nan, regex=True)
        table_df = table_df.replace(r'&nbsp', np.nan)

        table_df["lat"] = ddmmss_to_dec(table_df["lat"])
        table_df["lon"] = ddmmss_to_dec(table_df["lon"])*-1

        string_columns = ["station", "site", "type",
                          "county", "basin", "description"]
        table_df[string_columns] = table_df[string_columns].astype('string')

        return table_df

    def get_wx_data(self, start_date=None, end_date=None, station_name=None) -> pd.DataFrame:
        
        # name search param
        # defualt value is wildcad
        name_search_param = "%"
        if station_name:
            name_search_param = station_name

        # setup request for inital search
        url = "https://my.sfwmd.gov/dbhydroplsql/show_dbkey_info.show_dbkeys_matched"
        params = {"display_quantity": "999999",
                  "v_category": "WEATHER",
                  "v_frequency": "BK",
                  "v_station": name_search_param}

        # make request and extract table
        r = self.session.get(url, params=params)
        html_soup = BeautifulSoup(r.text, 'lxml')
        result = html_soup.select("form>table")
        table_df = pd.read_html(str(result))[0]

        # clean up table
        remove_cols = ["GetData",
                       'X Coord', 'Y Coord',
                       "Sec", "Twp", "Rng",
                       "Strata", "OpNum", "Struct"]
        table_df.drop(columns=remove_cols, inplace=True)

        new_column_names = ["dbkey", "station", "group", "site", "data_type",
                            "freq", "stat", "recorder", "agency",
                            "start_date", "end_date", "county",
                            "lat", "lon", "basin"]
        table_df.columns = new_column_names

        table_df = table_df.replace(r'^\s*$', np.nan, regex=True)
        table_df = table_df.replace(r'&nbsp', np.nan)

        table_df["lat"] = ddmmss_to_dec(table_df["lat"])
        table_df["lon"] = ddmmss_to_dec(table_df["lon"])*-1

        string_columns = ["dbkey", "station", "group", "site", "data_type",
                          "freq", "stat", "recorder", "agency",
                          "county", "basin"]
        table_df[string_columns] = table_df[string_columns].astype('string')

        date_columns = ["start_date", "end_date"]
        for c in date_columns:
            table_df[c] = pd.to_datetime(table_df[c], format="%d-%b-%Y")

        table_df = table_df[table_df['start_date'].notna()]
        table_df = table_df[table_df['end_date'].notna()]

        if start_date:
            table_df = table_df[table_df["start_date"].dt.date <= start_date]

        if end_date:
            table_df = table_df[table_df["end_date"].dt.date >= end_date]

        # get all db keys
        db_keys = table_df["dbkey"].tolist()[:]

        # download function for one dbkey entry
        def download_data(db_key):
            # print(db_key)
            start_date_param = start_date.strftime("%Y%m%d")
            end_date_param = end_date.strftime("%Y%m%d")
            dbkey_param = db_key

            url = "https://my.sfwmd.gov/dbhydroplsql/web_io.report_process"
            params = {"v_start_date": start_date_param,
                      "v_end_date": end_date_param,
                      "v_target_code": "file_csv",
                      "v_run_mode": "onLine",
                      "v_report_type": "format6",
                      "v_dbkey": dbkey_param,
                      "v_js_flag": "Y",
                      "v_os_code": "Win",
                      "v_interval_count": "5"}
            r = self.session.get(url, params=params)
            return r.content


        # download data for all entries
        # downloaded_data = list(map(download_data, db_keys))
        downloaded_data = Parallel(n_jobs=-1, verbose=11)(delayed(download_data)(db_key) for db_key in db_keys)


        #process function for one entry
        def process_downloaded_data(content):
            csv = content.decode("utf-8")

            # remove first 3 lines from file which are not part of the main table
            csv = os.linesep.join(csv.split(os.linesep)[3:])

            # somtimes table is empty, just return nothing
            if csv.isspace():
                return None

            # remove random footer that shows up somtimes
            random_footer_index = [idx for idx, s in enumerate(
                csv.split(os.linesep)) if 'MEASURING POINT REFERENCE ELEVATION' in s]
            if random_footer_index:
                csv = os.linesep.join(csv.split(os.linesep)[:random_footer_index[0]])

            # clean up table
            entry_df = pd.read_csv(StringIO(csv), header=None)
            entry_df.columns = ["sample_dt", "dcvp_station_id", "dbkey", "data_value", "tmp", "quality_flag"]
            entry_df.drop(columns=["dcvp_station_id", "tmp"], inplace=True)
            entry_df[["dbkey", "quality_flag"]] = entry_df[["dbkey", "quality_flag"]].astype('string')
            entry_df["sample_dt"] = pd.to_datetime(entry_df["sample_dt"], format="%d-%b-%Y %H:%M:%S")
            return entry_df

        # process all downloaded data
        # wx_dfs = list(map(process_downloaded_data, downloaded_data))
        wx_dfs = Parallel(n_jobs=-1, verbose=11)(delayed(process_downloaded_data)(data) for data in downloaded_data)
        
        # remove entries that retunred non because they were empty
        wx_dfs = [x for x in wx_dfs if x is not None]

        # stack all tables from all entries into one big table
        wx_df = pd.concat(wx_dfs)

        wx_df_complete = pd.merge(wx_df, table_df, how="left", on="dbkey")

        return wx_df_complete
