import json


class ETLprocessor:
    def __init__(self, config_file) -> None:
        with open(config_file, "r") as file:
            self.config = json.load(file)

    def process_api(self, api_name):
        if api_name in self.config:
            api_info = self.config[api_name]
            api_url = api_info.get("url")
            store_path = api_info.get("path")
            data_processor_name = api_info.get("data_processor")
            map_columns = api_info.get("columns")
            if api_url and data_processor_name:
                print(f"Processing API: {api_name}")
                print(f"API URL: {api_url}")
                print(f"Data Processor: {data_processor_name}")
                # 動態調用對應的資料處理方法
                data_processor = getattr(self, data_processor_name, None)
                if callable(data_processor):
                    data_processor(api_name, api_url, map_columns, store_path)
                else:
                    print(f"Invalid data processor for API: {api_name}")
            else:
                print(f"Invalid configuration for API: {api_name}")
        else:
            print(f"API not found: {api_name}")
