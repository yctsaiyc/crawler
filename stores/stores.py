from tgos import get_addr_info

import os
import requests
import pandas as pd
import time


store_list = ["cosmed"]

def get_cosmed_df():
    # Send a GET request to the API
    url = "https://www.cosmed.com.tw/api/getStore.aspx?t=store&c=&d=&s="
    response = requests.get(url)
    # time.sleep(5)
    
    # Parse the JSON response
    data = response.json()
    
    # Initialize lists to store data
    store_names = []
    zip_code_names_1 = []
    zip_code_names_2 = []
    addresses = []
    
    # Extract data from each store entry
    for store in data["data"]:
        store_names.append(store["StoreNM"])
        zip_code_names_1.append(store["ZipCodeName1"])
        zip_code_names_2.append(store["ZipCodeName2"])
        addresses.append(store["Address"])
    
    # Concatenate zip code names and address for column 2
    addresses = [f"{z1}{z2}{addr.split('(')[0]}" for z1, z2, addr in zip(zip_code_names_1, zip_code_names_2, addresses)]
    
    df = pd.DataFrame({
        "Store Name": store_names,
        "City": zip_code_names_1,
        "District": zip_code_names_2,
        "Address": addresses
    })

    return df


if __name__ == "__main__":
    df = pd.DataFrame()
    for store in store_list:
        if not os.path.exists(f"{store}.csv"):
            df = get_cosmed_df()
            df.to_csv(f"{store}.csv", index=False)
        else:
            df = pd.read_csv(f"{store}.csv")

        for idx, row in df.iterrows():
            addr = row["Address"].split("號")[0].split("、")[0] + "號"
            df.at[idx, "latitude"], df.at[idx, "longitude"], df.at[idx, "code_base"], df.at[idx, "code1"], df.at[idx, "code2"] = get_addr_info(addr)

    df.to_csv("cosmed_tgos.csv", index=False)
