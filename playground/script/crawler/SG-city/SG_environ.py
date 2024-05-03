import sys
import os
import requests
import json
import pandas as pd
from datetime import datetime
import pytz


def DATA_GOV_SG_api(config):

    response = requests.get(config["url"])

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        print(config["name"])
        data_json = response.json()
        df = pd.DataFrame()
        if config["nested_data"] != "":
            df = pd.json_normalize(eval(f"data_json{config['nested_data']['path_1']}"))
            df = df[config["nested_data"]["fields_1"]]
            df2 = pd.json_normalize(eval(f"data_json{config['nested_data']['path_2']}"))
            df2 = df2[config["nested_data"]["fields_2"]]
            df = pd.merge(
                df,
                df2,
                how="inner",
                left_on=config["nested_data"]["merge_key_1"],
                right_on=config["nested_data"]["merge_key_2"],
            )
            df.drop(columns=[config["nested_data"]["merge_key_2"]], inplace=True)
        for col in config["data_path"]:
            df[col.split("'")[-2]] = eval(f"data_json{col}")
        print(df)
        csv_path = os.path.join(
            config["csv_path"],
            os.path.basename(config["csv_path"])
            + "_"
            + datetime.now(pytz.timezone('Asia/Taipei')).strftime("%y%m%d%H%M%S")
            + ".csv",
        )
        df.to_csv(csv_path, index=False)

    else:
        # Handle the error, e.g., print an rror message
        print(f"Failed to retrieve data. Sttus code: {response.status_code}")


if __name__ == "__main__":
    config_file_path = sys.argv[1]
    with open(config_file_path, 'r') as file:
        config = json.load(file)
    DATA_GOV_SG_api(config)
