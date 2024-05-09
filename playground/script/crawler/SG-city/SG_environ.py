import sys
import os
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz


def get_items_df(data_json, sublist_name):
    item_list = list()

    for i in data_json["items"]:
        for sub_dict in i[sublist_name]:
            sub_dict.update({k: v for k, v in i.items() if k != sublist_name})
            item_list.append(sub_dict)

    item_df = pd.json_normalize(item_list)
    return item_df


def get_items_df_2(data_json, sublist_name):
    item_df = pd.DataFrame()

    for i in data_json["items"]:
        item_df_i = pd.DataFrame.from_dict(i[sublist_name])
        item_df_i["timestamp"] = i["timestamp"]
        item_df_i["update_timestamp"] = i["update_timestamp"]
        item_df = pd.concat([item_df, item_df_i])

    return item_df


def get_SG_environ_df(config):
    response = requests.get(config["url"])

    if response.status_code == 200:
        data_json = response.json()
        df = pd.DataFrame()

        if config["name"] == "2-hour-weather-forecast":
            meta_df = pd.json_normalize(data_json["area_metadata"])
            meta_df.rename(columns={"name": "area"}, inplace=True)
            item_df = get_items_df(data_json, "forecasts")
            df = pd.merge(meta_df, item_df, how="right", on="area")

        elif config["name"] == "24-hour-weather-forecast":
            df = get_items_df(data_json, "periods")

        elif config["name"] == "4-day-weather-forecast":
