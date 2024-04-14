from ETL_tool_live import ETLprocessorLive
import requests
from datetime import datetime
import json
import pandas as pd
import time
import os
import schedule


class ExchangeFetcher(ETLprocessorLive):
    def __init__(self, config_file) -> None:
        with open(config_file, "r") as file:
            self.config = json.load(file)

    def process_api(self, api_name):
        return super().process_api(api_name)

    def process_data_api(self, api_url, map_columns, path):
        try:
            r = requests.get(api_url)
            content = json.loads(r.text)
            df = pd.DataFrame(content)
            # current_date = datetime.now().date()
            date_list = ["日期", "date", "transDate"]
            if "交易日期" in df.columns:
                df["交易日期"] = df["交易日期"].apply(self.roc_to_ad)
            for date in date_list:
                if date in df.columns:
                    df[date] = df[date].apply(self.format_ad)
            os.makedirs(path, exist_ok=True)
            # filename = int(time.mktime(current_date.timetuple()))
            prefix = path.split("/")[-1]
            csv_file_path = os.path.join(path, f"{prefix}.csv")
            print(csv_file_path)
            df.to_csv(csv_file_path, index=False)
        except Exception as e:
            print("ERROR:", e)


if __name__ == "__main__":
    etl_processor = ExchangeFetcher("api_config_meat.json")
    # print(etl_processor.process_api("API1"))
    #    def day_job():
    api_live_list_day = [f"API{i}" for i in range(1, 9)]
    for api in api_live_list_day:
        etl_processor.process_api(api)
#    schedule.every().day.at("00:00").do(day_job)
#    while True:
#        schedule.run_pending()
#        time.sleep(1)
