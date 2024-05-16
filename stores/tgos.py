import requests
import json


KEY = input("Enter TGOS keystr: ")


def get_addr_info(address):
    url = f"https://gis.tgos.tw/TGLocator/TGLocator.ashx?format=json&input={address}&keystr={KEY}"
    response = requests.get(url)
    data = json.loads(response.text)

    data_dict = {
        "address": "",
        "county": "",
        "town": "",
        "longitude": "",
        "latitude": "",
        "code_base": "",
        "code1": "",
        "code2": "",
    }

    try:
        data = data["results"][0]

        data_dict["address"] = data["FULL_ADDR"]
        data_dict["county"] = data["COUNTY"]
        data_dict["town"] = data["TOWN"]
        data_dict["longitude"] = data["geometry"]["x"]
        data_dict["latitude"] = data["geometry"]["y"]
        data_dict["code_base"] = data["CODEBASE"]
        data_dict["code1"] = data["CODE1"]
        data_dict["code2"] = data["CODE2"]

    except IndexError:
        pass

    except KeyError:
        print(data["error_message"])

    return data_dict
