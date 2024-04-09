from ETL_tool_live import ETLprocessorLive
import requests
from datetime import datetime
import json
import pandas as pd
import time
import os
import schedule

# Define a class named ExchangeFetcher that inherits from ETLprocessorLive.
class ExchangeFetcher(ETLprocessorLive):
    # Constructor method to initialize the class instance.
    def __init__(self, config_file) -> None:
        # Open the specified config file and load its content as JSON.
        with open(config_file,'r') as file:
            self.config = json.load(file)
    
    # Method to process API data.
    def process_api(self, api_name):
        # Call the superclass method to process the API.
        return super().process_api(api_name)
    
    # Method to process data from an API URL.
    def process_data_api(self, api_url, map_columns, path):
        try:
            # Send a GET request to the API URL.
            r = requests.get(api_url)
            # Load the JSON response into a DataFrame using Pandas.
            content = json.loads(r.text)
            df = pd.DataFrame(content)
            
            # Process date columns if present.
            date_list = ['日期' , 'date' , 'transDate']
            if '交易日期' in df.columns:
                df['交易日期'] = df['交易日期'].apply(self.roc_to_ad)
            for date in date_list:
                if date in df.columns:
                    df[date] = df[date].apply(self.format_ad)
            
            # Create the directory if it doesn't exist.
            os.makedirs(path, exist_ok=True)
            # Generate the CSV file path.
            prefix = path.split("/")[-1]
            csv_file_path = os.path.join(path, f"{prefix}.csv")
            print(csv_file_path)
            # Save the DataFrame to a CSV file.
            df.to_csv(csv_file_path, index=False)
        except Exception as e:
            # Print error message if an exception occurs.
            print("ERROR:" ,e)

# Entry point of the script.
if __name__ == "__main__":
    # Create an instance of ExchangeFetcher with the specified config file.
    etl_processor = ExchangeFetcher("api_config_meat.json")
    
    # Define a function to process API data.
    def day_job():
        api_live_list_day=[f'API{i}' for i in range(1,9)]
        for api in api_live_list_day:
            etl_processor.process_api(api)
    
    # Schedule the day_job function to run every day at midnight.
    schedule.every().day.at("00:00").do(day_job)
    
    # Continuously check for scheduled tasks and run them.
    while True:
        schedule.run_pending()
        time.sleep(1)

