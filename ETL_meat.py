from ETL_tool_live import ETLprocessorLive
import requests
from datetime import datetime, date, timedelta
import json
import pandas as pd
import time
import os
import schedule


class ExchangeFetcher(ETLprocessorLive):
    def __init__(self, config_file) -> None:
        super().__init__(config_file)

    def process_api(self, api_name):
        return super().process_api(api_name)

    def fetch_json_to_df(self, api_name, url):
        r = requests.get(url)
        content = json.loads(r.text)
        try:
            content = content["Data"]  # When using api method
        except TypeError:
            pass
        df = pd.DataFrame(content)

        if api_name == "API1":
            for date_col in ["TransDate", "交易日期"]:
                try:
                    df[date_col] = df[date_col].apply(self.roc_str_to_ad_str)
                except KeyError:
                    pass

        return df

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

    def append_date_to_url(self, api_name, start_date):
        if api_name == "API1":
            start_date_str = self.date_to_roc_year_str(start_date)
            print("  Current date:", start_date_str)
            url = self.config[api_name]["url_api"] + start_date_str
        elif api_name == "API8":
            start_date_str = start_date.strftime("%Y/%m/%d")
            print("  Current date:", start_date_str)
            url = self.config[api_name]["url_opendata"] + start_date_str
        else:
            start_date_str = start_date.strftime("%Y/%m/%d")
            url = self.config[api_name]["url_api"] + start_date_str
            url += "&End_time=" + datetime.today().date().strftime("%Y/%m/%d")

        print("  url:", url)
        return url

    def update_config_latest_data_date(self, api_name, last_row):
        for date_col in ["date", "TransDate", "日期", "交易日期"]:
            try:
                latest_data_date_str = last_row[date_col]
                self.config[api_name]["latest_data_date"] = latest_data_date_str
                with open("api_config_meat.json", "w", encoding="utf-8") as file:
                    json.dump(self.config, file, ensure_ascii=False, indent=4)
                print(f"  Update {api_name} latest_data_date: {last_row[date_col]}\n\n")
                break
            except KeyError as e:
                pass

    def process_data_api(self, api_name, map_columns, dir_path):
        today = datetime.today().date()
        os.makedirs(dir_path, exist_ok=True)
        print("Get api info...")
        latest_data_date_str = self.config[api_name]["latest_data_date"]
        print("  latest data date:", latest_data_date_str)
        csv_file_path = dir_path + dir_path.split("/")[-2] + ".csv"
        print("  csv file path:", csv_file_path)

        if not os.path.exists(csv_file_path):
            print("First-time data download...")
            url = self.config[api_name]["url_opendata"]
            print("  url:", url)
            print("  Get json data and convert to dataframe...")
            df = self.fetch_json_to_df(api_name, url)
            print("  Reverse the order of data with old data at the top...")
            df = df[::-1].reset_index(drop=True)
            if api_name == "API4":
                date_col = df.pop("日期")
                df.insert(0, "日期", date_col)

            print(df.tail(10))
            print("  Write to csv...")
            with open(csv_file_path, "w") as f:
                df.to_csv(f, index=False, header=False)
            self.update_config_latest_data_date(api_name, df.iloc[-1])

        else:
            print("Update csv file...")

            if api_name == "API2":
                print("  The data stopped updating after 2014/4/1")
                return

            latest_data_date = datetime.strptime(
                latest_data_date_str, "%Y/%m/%d"
            ).date()
            loop_apis = ["API1", "API8"]

            if api_name not in loop_apis:
                url = self.append_date_to_url(
                    api_name, latest_data_date + timedelta(days=1)
                )
                df = self.fetch_json_to_df(api_name, url)

            else:
                df = None

                while latest_data_date < today:
                    latest_data_date += timedelta(days=1)
                    url = self.append_date_to_url(api_name, latest_data_date)
                    if df is None or len(df) == 0:
                        df = self.fetch_json_to_df(api_name, url)
                        print(df)
                    else:
                        df2 = self.fetch_json_to_df(api_name, url)
                        print(df2)
                        if len(df2) != 0:
                            df = pd.concat([df, df2], ignore_index=True)

            if df is None or len(df) == 0:
                return

            print("  Reverse the order of data with old data at the top...")
            df = df[::-1].reset_index(drop=True)
            print("  Write to csv")
            with open(csv_file_path, "a") as f:
                df.to_csv(f, index=False, header=False)
            print(df.tail(10))
            self.update_config_latest_data_date(api_name, df.iloc[-1])


if __name__ == "__main__":
    etl_processor = ExchangeFetcher("api_config_meat.json")
    for i in range(1):
        api_live_list_day = [f"API{i}" for i in [1]]
        for api in api_live_list_day:
            etl_processor.process_api(api)
