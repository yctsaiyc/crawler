import os
import json


def DATA_GOV_SG_api(param_dict):
    
    register = os.path.join(self.param_dict['destination'], 'register')
    # Make the GET request
    response = requests.get(self.param_dict['url'])

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data_json = response.json()

        for doc_needed, doc_json_path in zip(self.param_dict['docs_needed'], self.param_dict['docs_json_path']):

            json_paths = doc_json_path.split(',')
            
            # DataFrame is a two-dimensional, size-mutable, and tabular data structure.
            dataframe = pd.DataFrame()
            for json_path in json_paths:

                data_subset = eval(f"data_json{json_path}")

                # dict
                if isinstance(data_subset, dict):
                    new_dataframe = pd.json_normalize(data_subset)
                    dataframe = pd.concat([dataframe, new_dataframe], ignore_index=True)
                
                # list
                elif isinstance(data_subset, list):

                    list_has_dict = False
                    for item in data_subset:
                        if isinstance(item, dict):
                            list_has_dict = True
                            break
                    
                    # dict
                    if list_has_dict == True:
                        new_dataframe = pd.json_normalize(data_subset)
                        dataframe = pd.concat([dataframe, new_dataframe], ignore_index=True)
                    
                    # list
                    else:
                        matches = re.findall(r'\[(.*?)\]', json_path)
                        column_name = eval(matches[-1])
                        for item in data_subset:
                            new_dataframe = pd.DataFrame({column_name: [item]})
                            dataframe = pd.concat([dataframe, new_dataframe], ignore_index=True)
                
                # value
                else:
                    matches = re.findall(r'\[(.*?)\]', json_path)
                    column_name = eval(matches[-1])
                    dataframe[column_name] = data_subset
                    # new_dataframe = pd.DataFrame({column_name: [data_subset]})
                    # dataframe = pd.concat([dataframe, new_dataframe], ignore_index=True)



            csv = os.path.join(register, doc_needed)

            tz_taipei = pytz.timezone('Asia/Taipei')
            datetime_taipei = datetime.now(tz_taipei)
            dataframe['data_time'] = datetime_taipei.strftime("%Y-%m-%d %H:%M:%S")

            dataframe.to_csv(csv, index=False)

    else:
        # Handle the error, e.g., print an error message
        print(f"Failed to retrieve data. Status code: {response.status_code}")


if __name__ == "__main__":
    for d in os.listdir():
        if os.path.isdir(d) and d.startswith("Environ_"):
            with open(f"{d}/{d}.json", 'r') as file:
                param_dict = json.load(file)
                DATA_GOV_SG_api(param_dict)
