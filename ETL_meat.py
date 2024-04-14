from ETL_tool_live import ETLprocessorLive
import requests
from datetime import datetime, timedelta
import json
import pandas as pd
import time
import os
import schedule


class ExchangeFetcher(ETLprocessorLive):
    def __init__(self, config_file) -> None:
        super().__init__(config_file)
        self.latest_data_date = None

    def process_api(self, api_name):
        return super().process_api(api_name)

    def fetch_json_to_df(self, api_url):
        try:
            r = requests.get(api_url)
            content = json.loads(r.text)
            df = pd.DataFrame(content)
            df["交易日期"] = df["交易日期"].apply(self.roc_to_ad)
            ## current_date = datetime.now().date()
            # date_list = ['日期' , 'date' , 'transDate']
            # if '交易日期' in df.columns:
            #    df['交易日期'] = df['交易日期'].apply(self.roc_to_ad)
            # for date in date_list:
            #    if date in df.columns:
            #        df[date] = df[date].apply(self.format_ad)
            return df

        except Exception as e:
            print("ERROR:", e)

    #    def roc_year_str_to_date(roc_year_str):
    #        # Convert ROC year string to Gregorian year
    #        gregorian_year = int(roc_year_str[:-4]) + 1911
    #        # Extract month and day from the ROC year string
    #        month = int(roc_year_str[-4:-2])
    #        day = int(roc_year_str[-2:])
    #        # Create a datetime object for the Gregorian date
    #        gregorian_date = datetime(gregorian_year, month, day)
    #        # Extract date part from datetime object
    #        return gregorian_date.date()

    def date_to_roc_year_str(date_obj):
        year_str = str(date_obj.year - 1911)
        month_and_day_str = date_obj.strftime("%m%d")
        return year_str + month_and_day_str

    def process_data_api(self, api_url, map_columns, dir_path):
        today = datetime.today().date()
        os.makedirs(dir_path, exist_ok=True)
        print("Get csv file path...")
        csv_file_path = dir_path + dir_path.split("/")[-2] + ".csv"
        print("  csv file path:", csv_file_path)
        if self.latest_data_date is None:
            print("First-time data download...")
            print("  Get json data and convert to dataframe...")
            df = self.fetch_json_to_df(api_url)
            print("  Reverse the order of data with old data at the top...")
            df = df[::-1]
            print(df.head())
            print("  Write to csv")
            with open(csv_file_path, "w") as f:
                df.to_csv(f, index=False, header=False)
            # Update latest_data_date
            self.latest_data_date = today
        else:
            print("Update csv file...")
            df = None
            while self.latest_data_date < today:
                self.latest_data_date += timedelta(days=1)
                date_str = date_to_roc_year_str(latest_data_date)
                updated_api_ur = api_url + date_str
                if df is None:
                    df = fetch_json_to_df(api_url)
                else:
                    df.append(fetch_json_to_df(api_url), ignore_index=True)
            print("  Write to csv")
            with open(csv_file_path, "a") as f:
                df.to_csv(f, index=False, header=False)


if __name__ == "__main__":
    etl_processor = ExchangeFetcher("api_config_meat.json")
    # api_live_list_day = [f"API{i}" for i in range(1, 9)]
    api_live_list_day = [f"API{i}" for i in range(1, 2)]
    for api in api_live_list_day:
        etl_processor.process_api(api)
#    schedule.every().day.at("00:00").do(day_job)
#    while True:
#        schedule.run_pending()
#        time.sleep(1)

