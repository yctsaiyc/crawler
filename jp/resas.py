import json
import requests
import pandas as pd
import os

### from airflow.exceptions import AirflowFailException


config = {}


def load_config(config_path):
    global config
    with open(config_path) as file:
        config = json.load(file)


def json_to_csv(json_data):
    # 1. data 轉 df, pd.Int64Dtype允許NaN和整數共存(避免欄位變成浮點數)
    # year,month,value0,value1,value2,value3,value4,value5,value6
    df = pd.DataFrame(json_data["result"]["data"], dtype=pd.Int64Dtype())

    # 2. 合併日期欄位
    df["year-month"] = (
        df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)
    )

    # 3. 加入都道府県名欄位
    prefCode = json_data["result"]["prefCode"]
    df["prefName"] = json_data["result"]["prefName"]

    # 4. 加入國籍欄位
    matter = json_data["result"]["matter"]

    if matter == "1":
        df["matter"] = "総数"

    elif matter == "2":
        df["matter"] = "日本"

    elif matter == "3":
        df["matter"] = "外国"

    # 5. 欄位排序 重新命名
    df = df[config["column_names"].keys()]
    df.columns = config["column_names"].values()

    # 6. 存檔
    csv_path = os.path.join(config["data_dir"], f"resas_{prefCode}_{matter}_2011to2021.csv")
    df.to_csv(csv_path, index=False)
    print(f"Saved to {csv_path}")

    return


def main(config_path):
    try:
        load_config(config_path)

        headers = {
            "X-API-KEY": config["X-API-KEY"],
        }

        print("headers:", headers)

        for prefCode in config["prefCode"]:
            for matter in [2, 3]:
                url = f"{config['base_url']}?display=2&unit=1&prefCode={int(prefCode)}&matter={matter}"
                print("Requesting URL:", url)

                response = requests.get(url, headers=headers)

                if response.status_code == 200:
                    # 1. 存原始資料(json)
                    json_data = response.json()

                    json_path = os.path.join(
                        config["data_dir"], "json", f"resas_{prefCode}_{matter}_2011to2021.json"
                    )

                    with open(json_path, "w", encoding="utf-8") as f:
                        json.dump(json_data, f, ensure_ascii=False, indent=4)

                    print(f"Saved to {json_path}")

                    # 2. json轉csv
                    json_to_csv(json_data)

                else:
                    raise Exception(f"Error: {response.status_code}")

        return

    except Exception as e:
        raise  ### AirflowFailException(e)


main("resas.json")
