from ETL_tool_live import ETLprocessorLive
import requests
from datetime import datetime, date, timedelta
import json
import pandas as pd
import time
import os


class ExchangeFetcher(ETLprocessorLive):
    def __init__(self, config_file) -> None:
        super().__init__(config_file)

    def process_api(self, api_name):
        return super().process_api(api_name)

    def fetch_json_to_df(self, api_name, url):
        print("  url:", url)
        r = requests.get(url)
        content = json.loads(r.text)
        try:
            content = content["Data"]  # When using api method
        except TypeError:
            pass
        return pd.DataFrame(content)

    def roc_str_to_ad_str(self, roc_str):
        year = int(roc_str[:-4]) + 1911
        year_str = str(year)
        month_str = roc_str[-4:-2]
        day_str = roc_str[-2:]
        return f"{year_str}/{month_str}/{day_str}"

    def date_to_roc_year_str(self, date_obj):
        year_str = str(date_obj.year - 1911)
        month_and_day_str = date_obj.strftime("%m%d")
        return year_str + month_and_day_str

    def append_date_str_to_url(self, url, start_date_str, end_date_str=None):
        url = url + start_date_str
        if end_date_str is not None:
            url += "&End_time=" + end_date_str
        return url

    def update_config_latest_data_date(self, api_name, last_row):
        for date_col in ["date", "transDate", "TransDate", "日期", "交易日期"]:
            try:
                latest_data_date_str = last_row[date_col]
                self.config[api_name]["latest_data_date"] = latest_data_date_str
                with open("api_config_meat.json", "w", encoding="utf-8") as file:
                    json.dump(self.config, file, ensure_ascii=False, indent=4)
                print(f"Update {api_name} latest_data_date: {last_row[date_col]}\n\n")
                break
            except KeyError as e:
                pass

    def process_data_api(self, api_name, map_columns, dir_path):
        today = datetime.today().date()
        os.makedirs(dir_path, exist_ok=True)
        print("Get api info...")
        latest_data_date_str = self.config[api_name]["latest_data_date"]
        print("  latest data date:", latest_data_date_str)
        csv_file_path = (
            dir_path + dir_path.split("/")[-2] + "_" + today.strftime("%y%m%d") + ".csv"
        )
        print("  csv file path:", csv_file_path)

        if api_name == "API2" and latest_data_date_str != "":
            print("  The data stopped updating after 2014/03/31\n\n")
            return

        print("Download data...")
        if latest_data_date_str == "":
            url = self.config[api_name]["url_opendata"]
            df = self.fetch_json_to_df(api_name, url)

            if api_name == "API4":
                date_col = df.pop("日期")
                df.insert(0, "日期", date_col)

        else:
            today_str = today.strftime("%Y/%m/%d")

            latest_data_date = datetime.strptime(
                latest_data_date_str, "%Y/%m/%d"
            ).date()

            if api_name in ["API3", "API4", "API5", "API6", "API7", "API9"]:
                url = self.config[api_name]["url_api"]
                url = self.append_date_str_to_url(
                    url,
                    (latest_data_date + timedelta(days=1)).strftime("%Y/%m/%d"),
                    today_str,
                )
                df = self.fetch_json_to_df(api_name, url)

            else:  # ["API1", "API8"]
                if api_name == "API1":
                    url = self.config[api_name]["url_api"]
                else:  # "API8"
                    url = self.config[api_name]["url_opendata"]

                df = None

                while latest_data_date < today:
                    latest_data_date += timedelta(days=1)
                    latest_data_date_str = latest_data_date.strftime("%Y/%m/%d")

                    if api_name == "API1":
                        latest_data_date_str = self.date_to_roc_year_str(
                            latest_data_date
                        )

                    url = self.append_date_str_to_url(
                        url, latest_data_date_str, today_str
                    )

                    if df is None or len(df) == 0:
                        df = self.fetch_json_to_df(api_name, url)
                        # print(df)

                    else:
                        df2 = self.fetch_json_to_df(api_name, url)
                        # print(df2)
                        if len(df2) != 0:
                            df = pd.concat([df2, df], ignore_index=True)

        if df is None or len(df) == 0:
            return

        if api_name == "API1":
            for date_col in ["TransDate", "交易日期"]:
                try:
                    df[date_col] = df[date_col].apply(self.roc_str_to_ad_str)
                except KeyError:
                    pass

        # print("  Reverse the order of data with old data at the top...")
        df = df[::-1].reset_index(drop=True)
        # print(df.tail(10))

        print("Write to csv...")
        with open(csv_file_path, "w") as f:
            df.to_csv(f, index=False, header=False)

        self.update_config_latest_data_date(api_name, df.iloc[-1])


if __name__ == "__main__":
    etl_processor = ExchangeFetcher("api_config_meat.json")
    api_live_list_day = [f"API{i}" for i in range(1, 10)]
    for api in api_live_list_day:
        etl_processor.process_api(api)
