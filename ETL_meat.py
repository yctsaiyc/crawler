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
        self.dic_url_opendata = {}
        self.dic_url_api = {}
        self.dic_latest_data_date = {}
        for api_name in self.config:
            self.dic_latest_data_date[api_name] = self.config[api_name][
                "latest_data_date"
            ]
            self.dic_url_opendata[api_name] = self.config[api_name]["url_opendata"]
            self.dic_url_api[api_name] = self.config[api_name]["url_api"]
        print(self.dic_url_opendata)
        print(self.dic_url_api)
        print(self.dic_latest_data_date)

    def process_api(self, api_name):
        return super().process_api(api_name)

    def fetch_json_to_df(self, url):
        try:
            r = requests.get(url)
            content = json.loads(r.text)
            df = pd.DataFrame(content)
            return df

        except Exception as e:
            print("ERROR:", e)

    #    def roc_year_str_to_date(self, roc_year_str):
    #        # Convert ROC year string to Gregorian year
    #        gregorian_year = int(roc_year_str[:-4]) + 1911
    #        # Extract month and day from the ROC year string
    #        month = int(roc_year_str[-4:-2])
    #        day = int(roc_year_str[-2:])
    #        # Create a datetime object for the Gregorian date
    #        gregorian_date = datetime(gregorian_year, month, day)
    #        # Extract date part from datetime object
    #        return gregorian_date.date()

    def date_to_roc_year_str(self, date_obj):
        year_str = str(date_obj.year - 1911)
        month_and_day_str = date_obj.strftime("%m%d")
        return year_str + month_and_day_str

    # To test update function
    def del_data_of_recent_dates(self, api_name, df, dates):
        unique_dates = []
        for column_name in ["date", "transDate"]:
            # Check if the column exists in the DataFrame
            if column_name in df.columns:
                # Get unique dates from the column
                unique_dates = df[column_name].unique()
                # Get the last two dates
                dates_to_delete = unique_dates[-dates:]
                # Filter the DataFrame to keep only rows with dates not in the 'dates_to_delete' list
                print(dates_to_delete)
                df = df[~df[column_name].isin(dates_to_delete)]
                print(df.tail(10))
                # Update the latest data date
                self.dic_latest_data_date[api_name] = datetime.strptime(
                    df[column_name].unique()[-1], "%Y/%m/%d"
                ).date()
                # Break the loop once the column is found
                break
        return df

    def process_data_api(self, api_name, api_url, map_columns, dir_path):
        today = datetime.today().date()
        os.makedirs(dir_path, exist_ok=True)
        print("Get csv file path...")
        csv_file_path = dir_path + dir_path.split("/")[-2] + ".csv"
        print("  csv file path:", csv_file_path)
        print("  latest data date:", self.dic_latest_data_date[api_name])
        # if self.dic_latest_data_date[api_name] is None:
        if not os.path.exists(csv_file_path):
            print("First-time data download...")
            print("  Get json data and convert to dataframe...")
            print("    api_url:", api_url)
            df = self.fetch_json_to_df(api_url)
            print("  Reverse the order of data with old data at the top...")
            df = df[::-1].reset_index(drop=True)
            print(df.tail(10))
            print(
                "  Delete the data of the two most recent dates (To test update function)"
            )
            df = self.del_data_of_recent_dates(api_name, df, 2)
            print("  Write to csv")
            with open(csv_file_path, "w") as f:
                df.to_csv(f, index=False, header=False)
            # Update latest_data_date
            # self.dic_latest_data_date[api_name] = today
        else:
            print("Update csv file...")
            df = None
            latest_data_date = self.dic_latest_data_date[api_name]
            while latest_data_date < today:
                latest_data_date += timedelta(days=1)
                # Append parameters to the original URL
                if api_name == "API1":
                    date_str = self.date_to_roc_year_str(latest_data_date)
                else:
                    date_str = latest_data_date.strftime("%Y/%m/%d")
                print("  Current date:", date_str)
                updated_api_url = api_url + date_str
                if df is None:
                    df = self.fetch_json_to_df(updated_api_url)
                    for date in ["日期", "date", "transDate"]:
                        if df is not None and date in df.columns:
                            df[date] = df[date].apply(self.roc_to_ad)
                            break
                    print(updated_api_url, df)
                else:
                    df2 = self.fetch_json_to_df(updated_api_url)
                    print(updated_api_url, df2) if len(df2) != 0 else None
                    df = pd.concat([df, df2], ignore_index=True)
            print("  Write to csv")
            if df is not None:
                with open(csv_file_path, "a") as f:
                    df.to_csv(f, index=False, header=False)
                print(df.tail(10))
            # Update latest_data_date
            self.dic_latest_data_date[api_name] = latest_data_date
        print("\n", self.dic_latest_data_date, "\n")


if __name__ == "__main__":
    etl_processor = ExchangeFetcher("api_config_meat.json")
    # for i in range(2):
    #    api_live_list_day = [f"API{i}" for i in range(8, 10)]
    #    for api in api_live_list_day:
    #        etl_processor.process_api(api)
