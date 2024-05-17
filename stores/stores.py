from tgos import get_addr_info

import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm


STORE_LIST = [
    # "cosmed",
    "watsons",
]


def get_store_df(store_name):
    if store_name == "cosmed":
        url = "https://www.cosmed.com.tw/api/getStore.aspx?t=store&c=&d=&s="
        response = requests.get(url)
        data = response.json()

        store_names = []
        cities = []
        districts = []
        addrs = []

        for store in data["data"]:
            store_names.append(store["StoreNM"])
            cities.append(store["ZipCodeName1"])
            districts.append(store["ZipCodeName2"])
            addrs.append(store["Address"])

        df = pd.DataFrame(
            {
                "Store Name": store_names,
                "Raw Address": addrs,
                "City": cities,
                "District": districts,
            }
        )

    elif store_name == "watsons":
        url = "https://api.watsons.com.tw/api/v2/wtctw/stores/watStores?pageSize=1000&isCceOrCc=false&fields=FULL&lang=zh_TW&curr=TWD"

        # Headers to mimic a browser request
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-TW",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "DNT": "1",  # Do Not Track Request Header
        }

        response = requests.get(url)
        print("got response")
        soup = BeautifulSoup(response.content, 'xml')
        print("got soup")
    
        store_names = []
        cities = []
        districts = []
        addrs = []

        stores = soup.find_all('stores')
        for store in stores:
            store_names.append(store.find('displayName').text)
            cities.append(store.find('town').text)
            districts.append(store.find('province').text.split("縣")[-1])
            print(district)
            addrs.append(store.find('streetName').text)
    
    return #df


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
            df = get_store_df(store)
            df.to_csv(f"{store}.csv", index=False)
        else:
            df = pd.read_csv(f"{store}.csv")

        total_iterations = len(df)
        progress_bar = tqdm(total=total_iterations, desc="Processing")

        for idx, row in df.iterrows():
            addr = row["City"] + row["District"] + clean_addr(row["Raw Address"])
            print(addr)
            tgos_dict = get_addr_info(addr)
            print(tgos_dict)

            df.at[idx, "Address"] = tgos_dict["address"]
            df.at[idx, "Longitude"] = tgos_dict["longitude"]
            df.at[idx, "Latitude"] = tgos_dict["latitude"]
            df.at[idx, "Code Base"] = tgos_dict["code_base"]
            df.at[idx, "Code1"] = tgos_dict["code1"]
            df.at[idx, "Code2"] = tgos_dict["code2"]

            progress_bar.update(1)

        progress_bar.close()

    df.to_csv(f"{store}_tgos.csv", index=False)
