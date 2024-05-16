from tgos import get_addr_info

import os
import requests
import pandas as pd
from tqdm import tqdm


STORE_LIST = ["cosmed"]

def get_cosmed_df():
    # Send a GET request to the API
    url = "https://www.cosmed.com.tw/api/getStore.aspx?t=store&c=&d=&s="
    response = requests.get(url)
    
    # Parse the JSON response
    data = response.json()
    
    # Initialize lists to store data
    store_names = []
    addrs = []
    
    # Extract data from each store entry
    for store in data["data"]:
        store_names.append(store["StoreNM"])
        addrs.append(store["Address"])
    
    df = pd.DataFrame({
        "Store Name": store_names,
        "Raw Address": addrs
    })

    return df

def clean_addr(addr):
    addr = addr.split("號")[0]
    addr = addr.split(" ")[0]
    addr = addr.split("、")[0]
    addr = addr.split(".")[0] 
    addr += "號"
    return addr

if __name__ == "__main__":
    df = pd.DataFrame()

    for store in STORE_LIST:
        if not os.path.exists(f"{store}.csv"):
            df = get_cosmed_df()
            df.to_csv(f"{store}.csv", index=False)
        else:
            df = pd.read_csv(f"{store}.csv")

        total_iterations = len(df)
        progress_bar = tqdm(total=total_iterations, desc="Processing")

        for idx, row in df.iterrows():
            addr = clean_addr(row["Raw Address"])
            tgos_dict = get_addr_info(addr)

            df.at[idx, "Address"] = tgos_dict["address"]
            df.at[idx, "County"] = tgos_dict["county"]
            df.at[idx, "Town"] = tgos_dict["town"]
            df.at[idx, "Longitude"] = tgos_dict["longitude"]
            df.at[idx, "Latitude"] = tgos_dict["latitude"]
            df.at[idx, "Code Base"] = tgos_dict["code_base"]
            df.at[idx, "Code1"] = tgos_dict["code1"]
            df.at[idx, "Code2"] = tgos_dict["code2"]

            progress_bar.update(1)

        progress_bar.close()

    df.to_csv("cosmed_tgos.csv", index=False)
