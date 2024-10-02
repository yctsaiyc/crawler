import json
import requests
from io import StringIO
import pandas as pd
import os
import glob
from datetime import datetime, timedelta
## from airflow.exceptions import AirflowFailException


config = {}


def load_config(config_path):
    global config
    with open(config_path) as file:
        config = json.load(file)


def process_df(df):
    # 1. 刪除欄位值為"総数"者
    df = df[(df["港"] != "総数") & (df["国籍・地域"] != "総数")]

    # 2. 日期格式轉換
    def format_date(row):
        year = row.split("年")[0]
        month = row.split("年")[1].replace("月", "")
        return f"{year}-{int(month):02d}"

    df.rename(columns={'時間軸(月次)': '時間軸（月次）'}, inplace=True)
    df["時間軸（月次）"] = df["時間軸（月次）"].apply(format_date)

    # 3. 欄位排序
    df = df[["港", "国籍・地域", "時間軸（月次）", "value", "annotation"]]

    return df


# 下載csv格式資料
def save_csv_data(year, month):
    # 1. 組成url
    cdTime = f"{year}00{month:02}{month:02}"
    # url = f"{config['csv_url']}&appId={config['appId']}&statsDataId={config['statsDataId_201301-201801']}&cdTime={cdTime}"
    url = f"{config['csv_url']}&appId={config['appId']}&statsDataId={config['statsDataId']}&cdTime={cdTime}"
    print("url:", url)

    # 2. 取得csv原始資料(字串格式)
    response = requests.get(url)

    # 3. 原始資料轉df
    # "tab_code","表章項目","cat01_code","港","cat02_code","国籍・地域","time_code","時間軸（月次）","unit","value","annotation"
    # "100","入国外国人","100000000","総数","100000","総数","2024000707","2024年7月","人","3225712",""
    df = pd.read_csv(StringIO(response.text))

    # 4. 刪除不必要欄位
    # print("unit:", df["unit"].unique())
    for cols in [
        "tab_code",
        "表章項目",
        "cat01_code",
        "cat02_code",
        "area_code",
        "time_code",
        "unit",
    ]:
        if cols in df.columns:
            df.drop(columns=cols, inplace=True)

    # 5. 刪除欄位值為"総数"者/ 日期格式轉換
    df = process_df(df)

    # 6. 存檔
    csv_path = os.path.join(config["data_dir"], f"visitors_{year}{month:02}.csv")
    df.to_csv(csv_path, index=False)
    print(f"csv saved: {csv_path}")

    return


def save_json_data(year, month):
    # 1. 組成url
    cdTime = f"{year}00{month:02}{month:02}"
    url = f"{config['json_url']}&appId={config['appId']}&cdTime={cdTime}"
    print("url:", url)

    # 2. 取得json原始資料
    response = requests.get(url)

    # 3. 存json檔
    if response.status_code == 200:
        json_data = response.json()
        json_path = os.path.join(
            config["data_dir"], "json", f"visitors_{year}{month:02}.json"
        )

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(json_data, f, ensure_ascii=False, indent=4)

        print(f"json saved: {json_path}")

    else:
        print(f"error: {response.status_code}")
        return

    # 4. 存csv檔
    # 4-1. 編碼轉換表
    list_class_obj = json_data["GET_STATS_DATA"]["STATISTICAL_DATA"]["CLASS_INF"][
        "CLASS_OBJ"
    ]
    df_cat01 = pd.DataFrame(list_class_obj[1]["CLASS"])
    df_cat02 = pd.DataFrame(list_class_obj[2]["CLASS"])
    df_time = pd.DataFrame([list_class_obj[3]["CLASS"]])

    # 4-2. 資料轉df
    json_data = json_data["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"]
    df = pd.DataFrame(json_data)

    # 4-3. 合併代碼表
    df = df.merge(df_cat01, left_on="@cat01", right_on="@code", how="left")
    df = df.merge(df_cat02, left_on="@cat02", right_on="@code", how="left")
    df = df.merge(df_time, left_on="@time", right_on="@code", how="left")

    # 4-4. 刪除不必要欄位
    df = df[["@name_x", "@name_y", "@name", "$"]]
    df.columns = ["港", "国籍・地域", "時間軸（月次）", "value"]
    df["annotation"] = ""
    print(df.head(3))

    # 4-5. 資料處理
    df = process_df(df)

    # 4-6. 存檔
    csv_path = os.path.join(config["data_dir"], f"visitors_{year}{month:02}.csv")
    df.to_csv(csv_path, index=False)
    print(f"csv saved: {csv_path}")

    return


# 201802-202010
def process_excel():
    xls_paths = glob.glob(os.path.join(config["data_dir"], "excel", "*-01-2.xlsx"))

    for xls_path in xls_paths:
        print("Processing:", xls_path)  # 18-02-01-2.xlsx
        df = pd.read_excel(xls_path, skiprows=2)

        # 1. 刪除空欄
        df.drop(columns=[col for col in df.columns if "Unnamed:" in col], inplace=True)

        # 2. 刪除欄位名稱中的非必要符號
        df.columns = [col.replace("\n", "").replace(" ", "") for col in df.columns]

        # 3. Unpivot
        df = df.melt(id_vars=["国籍・地域"], var_name="港", value_name="value")

        # 4. 補缺少的欄位
        file_name = os.path.basename(xls_path)
        year = f"20{file_name.split('-')[0]}"
        month = file_name.split("-")[1]
        df["時間軸（月次）"] = f"{year}年{month}月"
        df["annotation"] = ""

        # 5. 刪除欄位值為"総数"者/ 日期格式轉換
        df = process_df(df)

        # 6. 存檔
        csv_path = os.path.join(config["data_dir"], f"visitors_{year}{month:02}.csv")
        df.to_csv(csv_path, index=False)
        print(f"csv saved: {csv_path}")

    return


def main(config_path):
    try:
        load_config(config_path)

        # 每月25日更新兩個月前資料
        date = datetime.today() - timedelta(days=90)
        save_csv_data(date.year, date.month)

    except Exception as e:
        raise  ## AirflowFailException(e)
