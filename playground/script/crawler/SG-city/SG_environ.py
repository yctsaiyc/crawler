import sys
import os
import requests
import json
import pandas as pd
from datetime import datetime
import pytz


def SG_environ(config):

    response = requests.get(config["url"])

    if response.status_code == 200:
        data_json = response.json()

        df = pd.json_normalize(eval(f"data_json{config['individual_data']['path_1']}"))
        df = df.replace("\n", "", regex=True)
        df = df[config["individual_data"]["fields_1"]]

        if config["individual_data"]["merge_key_1"] != "":

            if config["name"] in ["PM25", "Pollutant-Standards-Index"]:
                df2 = pd.DataFrame.from_dict(
                    eval(f"data_json{config['individual_data']['path_2']}")
                )
                df2[config["individual_data"]["merge_key_2"]] = df2.index

            else:
                df2 = pd.json_normalize(
                    eval(f"data_json{config['individual_data']['path_2']}")
                )
                df2 = df2[config["individual_data"]["fields_2"]]

            df = pd.merge(
                df,
                df2,
                how="inner",
                left_on=config["individual_data"]["merge_key_1"],
                right_on=config["individual_data"]["merge_key_2"],
            )
            df.drop(columns=[config["individual_data"]["merge_key_2"]], inplace=True)

        for col in config["common_data_path"]:
            col_name = col.split("'")[-2]

            if config["name"] == "24-hour-weather-forecast" and col_name in [
                "low",
                "high",
            ]:
                col_name = col.split("'")[-4] + "." + col_name

            df[col_name] = eval(f"data_json{col}")

        csv_path = os.path.join(
            config["csv_path"],
            os.path.basename(config["csv_path"])
            + "_"
            + datetime.now(pytz.timezone("Asia/Taipei")).strftime("%y%m%d%H%M%S")
            + ".csv",
        )

        df.to_csv(csv_path, index=False)

    else:
        print(f"Failed to retrieve data. Sttus code: {response.status_code}")


if __name__ == "__main__":
    config_file_path = sys.argv[1]
    with open(config_file_path, "r") as file:
        config = json.load(file)
    SG_environ(config)
