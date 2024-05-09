import pandas as pd


data_2 = {
    "area_metadata": [
        {
            "name": "a1.n",
            "label_location": {"latitude": "a1.l.la", "longitude": "a1.l.lo"},
        },
        {
            "name": "a2.n",
            "label_location": {"latitude": "a2.l.la", "longitude": "a2.l.lo"},
        },
    ],
    "items": [
        {
            "update_timestamp": "i1.ut",
            "timestamp": "i1.t",
            "valid_period": {"start": "i1.v.s", "end": "i1.v.e"},
            "forecasts": [
                {"area": "a1.n", "forecast": "i1.f1.f"},
                {"area": "a2.n", "forecast": "i1.f2.f"},
            ],
        },
        {
            "update_timestamp": "i2.ut",
            "timestamp": "i2.t",
            "valid_period": {"start": "i2.v.s", "end": "i2.v.e"},
            "forecasts": [
                {"area": "a1.n", "forecast": "i2.f1.f"},
                {"area": "a2.n", "forecast": "i2.f2.f"},
            ],
        },
    ],
}

data_24 = {
    "items": [
        {
            "update_timestamp": "i1.ut",
            "timestamp": "i1.t",
            "valid_period": {"start": "i1.v.s", "end": "i1.v.e"},
            "general": {
                "forecast": "i1.g.f",
                "relative_humidity": {"low": "i1.g.rh.l", "high": "i1.g.rh.h"},
                "temperature": {"low": "i1.g.t.l", "high": "i1.g.t.h"},
                "wind": {
                    "speed": {"low": "i1.g.w.s.l", "high": "i1.g.w.s.h"},
                    "direction": "i1.g.w.d",
                },
            },
            "periods": [
                {
                    "time": {"start": "i1.p1.t.s", "end": "i1.p1.t.e"},
                    "regions": {
                        "west": "i1.p1.r.w",
                        "east": "i1.p1.r.e",
                        "central": "i1.p1.r.c",
                        "south": "i1.p1.r.s",
                        "north": "i1.p1.r.n",
                    },
                },
                {
                    "time": {"start": "i1.p2.t.s", "end": "i1.p2.t.e"},
                    "regions": {
                        "west": "i1.p2.r.w",
                        "east": "i1.p2.r.e",
                        "central": "i1.p2.r.c",
                        "south": "i1.p2.r.s",
                        "north": "i1.p2.r.n",
                    },
                },
            ],
        },
        {
            "update_timestamp": "i1.ut",
            "timestamp": "i1.t",
            "valid_period": {"start": "i1.v.s", "end": "i1.v.e"},
            "general": {
                "forecast": "i1.g.f",
                "relative_humidity": {"low": "i1.g.rh.l", "high": "i1.g.rh.h"},
                "temperature": {"low": "i1.g.t.l", "high": "i1.g.t.h"},
                "wind": {
                    "speed": {"low": "i1.g.w.s.l", "high": "i1.g.w.s.h"},
                    "direction": "i1.g.w.d",
                },
            },
            "periods": [
                {
                    "time": {"start": "i2.p1.t.s", "end": "i2.p1.t.e"},
                    "regions": {
                        "west": "i2.p1.r.w",
                        "east": "i2.p1.r.e",
                        "central": "i2.p1.r.c",
                        "south": "i2.p1.r.s",
                        "north": "i2.p1.r.n",
                    },
                },
                {
                    "time": {"start": "i2.p2.t.s", "end": "i2.p2.t.e"},
                    "regions": {
                        "west": "i2.p2.r.w",
                        "east": "i2.p2.r.e",
                        "central": "i2.p2.r.c",
                        "south": "i2.p2.r.s",
                        "north": "i2.p2.r.n",
                    },
                },
            ],
        },
    ],
    "api_info": {"status": "healthy"},
}

data_4 = {
    "items": [
        {
            "update_timestamp": "i1.ut",
            "timestamp": "i1.t",
            "forecasts": [
                {
                    "temperature": {"low": "i1.f1.t.l", "high": "i1.f1.t.h"},
                    "date": "i1.f1.d",
                    "forecast": "i1.f1.f",
                    "relative_humidity": {"low": "i1.f1.rh.l", "high": "i1.f1.rh.h"},
                    "wind": {
                        "speed": {"low": "i1.f1.w.s.l", "high": "i1.f1.w.s.h"},
                        "direction": "i1.f1.w.d",
                    },
                    "timestamp": "i1.f1.t",
                },
                {
                    "temperature": {"low": "i1.f2.t.l", "high": "i1.f2.t.h"},
                    "date": "i1.f2.d",
                    "forecast": "i1.f2.f",
                    "relative_humidity": {"low": "i1.f2.rh.l", "high": "i1.f2.rh.h"},
                    "wind": {
                        "speed": {"low": "i1.f2.w.s.l", "high": "i1.f2.w.s.h"},
                        "direction": "i1.f2.w.d",
                    },
                    "timestamp": "i1.f2.t",
                },
            ],
        },
        {
            "update_timestamp": "i2.ut",
            "timestamp": "i2.t",
            "forecasts": [
                {
                    "temperature": {"low": "i2.f1.t.l", "high": "i2.f1.t.h"},
                    "date": "i2.f1.d",
                    "forecast": "i2.f1.f",
                    "relative_humidity": {"low": "i2.f1.rh.l", "high": "i2.f1.rh.h"},
                    "wind": {
                        "speed": {"low": "i2.f1.w.s.l", "high": "i2.f1.w.s.h"},
                        "direction": "i2.f1.w.d",
                    },
                    "timestamp": "i2.f1.t",
                }
            ],
        },
    ],
    "api_info": {"status": "healthy"},
}

data_A_Ra_Re_WD_WS = {
    "metadata": {
        "stations": [
            {
                "id": "m.s1.i",
                "device_id": "m.s1.di",
                "name": "m.s1.n",
                "location": {"latitude": "m.s1.l.la", "longitude": "m.s1.l.lo"},
            },
            {
                "id": "m.s2.i",
                "device_id": "m.s2.di",
                "name": "m.s2.n",
                "location": {"latitude": "m.s2.l.la", "longitude": "m.s2.l.lo"},
            },
        ],
        "reading_type": "m.rt",
        "reading_unit": "m.ru",
    },
    "items": [
        {
            "timestamp": "i1.t",
            "readings": [
                {"station_id": "m.s1.i", "value": "i1.r1.v"},
                {"station_id": "m.s2.i", "value": "i1.r2.v"},
            ],
        },
        {
            "timestamp": "i2.t",
            "readings": [
                {"station_id": "m.s1.i", "value": "i2.r1.v"},
                {"station_id": "m.s2.i", "value": "i2.r2.v"},
            ],
        },
    ],
    "api_info": {"status": "healthy"},
}

data_PM = {
    "region_metadata": [
        {
            "name": "west",
            "label_location": {"latitude": "rm_w.l.la", "longitude": "rm_w.l.lo"},
        },
        {
            "name": "east",
            "label_location": {"latitude": "rm_e.l.la", "longitude": "rm_e.l.lo"},
        },
        {
            "name": "central",
            "label_location": {"latitude": "rm_c.l.la", "longitude": "rm_c.l.lo"},
        },
        {
            "name": "south",
            "label_location": {"latitude": "rm_s.l.la", "longitude": "rm_s.l.lo"},
        },
        {
            "name": "north",
            "label_location": {"latitude": "rm_n.l.la", "longitude": "rm_n.l.lo"},
        },
    ],
    "items": [
        {
            "timestamp": "i1.t",
            "update_timestamp": "i1.ut",
            "readings": {
                "pm25_one_hourly": {
                    "west": "i1.r.p.w",
                    "east": "i1.r.p.e",
                    "central": "i1.r.p.c",
                    "south": "i1.r.p.s",
                    "north": "i1.r.p.n",
                }
            },
        },
        {
            "timestamp": "i2.t",
            "update_timestamp": "i2.ut",
            "readings": {
                "pm25_one_hourly": {
                    "west": "i2.r.p.w",
                    "east": "i2.r.p.e",
                    "central": "i2.r.p.c",
                    "south": "i2.r.p.s",
                    "north": "i2.r.p.n",
                }
            },
        },
    ],
    "api_info": {"status": "healthy"},
}

data_Po = {
    "region_metadata":[
        {
            "name":"west",
            "label_location":{
                "latitude":1.35735,
                "longitude":103.7
            }
        },
        {
            "name":"east",
            "label_location":{
                "latitude":1.35735,
                "longitude":103.94
            }
        },
        {
            "name":"central",
            "label_location":{
                "latitude":1.35735,
                "longitude":103.82
            }
        },
        {
            "name":"south",
            "label_location":{
                "latitude":1.29587,
                "longitude":103.82
            }
        },
        {
            "name":"north",
            "label_location":{
                "latitude":1.41803,
                "longitude":103.82
            }
        }
    ],
    "items":[
        {
            "timestamp":"2024-05-08T01:00:00+08:00",
            "update_timestamp":"2024-05-08T01:00:48+08:00",
            "readings":{
                "o3_sub_index":{
                    "west":3,
                    "east":4,
                    "central":6,
                    "south":2,
                    "north":4
                },
                "pm10_twenty_four_hourly":{
                    "west":30,
                    "east":22,
                    "central":28,
                    "south":17,
                    "north":20
                },
                "pm10_sub_index":{
                    "west":30,
                    "east":22,
                    "central":28,
                    "south":17,
                    "north":20
                },
                "co_sub_index":{
                    "west":7,
                    "east":11,
                    "central":9,
                    "south":5,
                    "north":8
                },
                "pm25_twenty_four_hourly":{
                    "west":14,
                    "east":10,
                    "central":13,
                    "south":9,
                    "north":11
                },
                "so2_sub_index":{
                    "west":4,
                    "east":1,
                    "central":3,
                    "south":2,
                    "north":2
                },
                "co_eight_hour_max":{
                    "west":1,
                    "east":1,
                    "central":1,
                    "south":1,
                    "north":1
                },
                "no2_one_hour_max":{
                    "west":60,
                    "east":65,
                    "central":51,
                    "south":31,
                    "north":83
                },
                "so2_twenty_four_hourly":{
                    "west":6,
                    "east":2,
                    "central":5,
                    "south":4,
                    "north":3
                },
                "pm25_sub_index":{
                    "west":54,
                    "east":41,
                    "central":52,
                    "south":37,
                    "north":45
                },
                "psi_twenty_four_hourly":{
                    "west":54,
                    "east":41,
                    "central":52,
                    "south":37,
                    "north":45
                },
                "o3_eight_hour_max":{
                    "west":6,
                    "east":10,
                    "central":14,
                    "south":4,
                    "north":10
                }
            }
        },
        {
            "timestamp":"2024-05-08T23:00:00+08:00",
            "update_timestamp":"2024-05-08T23:00:56+08:00",
            "readings":{
                "o3_sub_index":{
                    "west":10,
                    "east":30,
                    "central":21,
                    "south":12,
                    "north":22
                },
                "pm10_twenty_four_hourly":{
                    "west":37,
                    "east":30,
                    "central":40,
                    "south":23,
                    "north":24
                },
                "pm10_sub_index":{
                    "west":37,
                    "east":30,
                    "central":40,
                    "south":23,
                    "north":24
                },
                "co_sub_index":{
                    "west":5,
                    "east":7,
                    "central":3,
                    "south":4,
                    "north":4
                },
                "pm25_twenty_four_hourly":{
                    "west":15,
                    "east":15,
                    "central":20,
                    "south":13,
                    "north":13
                },
                "so2_sub_index":{
                    "west":4,
                    "east":2,
                    "central":3,
                    "south":5,
                    "north":2
                },
                "co_eight_hour_max":{
                    "west":0,
                    "east":1,
                    "central":0,
                    "south":0,
                    "north":0
                },
                "no2_one_hour_max":{
                    "west":29,
                    "east":23,
                    "central":52,
                    "south":17,
                    "north":66
                },
                "so2_twenty_four_hourly":{
                    "west":6,
                    "east":3,
                    "central":6,
                    "south":8,
                    "north":3
                },
                "pm25_sub_index":{
                    "west":55,
                    "east":54,
                    "central":60,
                    "south":53,
                    "north":52
                },
                "psi_twenty_four_hourly":{
                    "west":55,
                    "east":54,
                    "central":60,
                    "south":53,
                    "north":52
                },
                "o3_eight_hour_max":{
                    "west":24,
                    "east":72,
                    "central":50,
                    "south":28,
                    "north":52
                }
            }
        }
    ],
    "api_info":{
        "status":"healthy"
    }
}

data_U = {
    "items":[
        {
            "timestamp":"2024-05-08T07:00:00+08:00",
            "update_timestamp":"2024-05-08T07:11:05+08:00",
            "index":[
                {
                    "value":0,
                    "timestamp":"2024-05-08T07:00:00+08:00"
                }
            ]
        },
        {
            "timestamp":"2024-05-08T19:00:00+08:00",
            "update_timestamp":"2024-05-08T19:11:17+08:00",
            "index":[
                {
                    "value":0,
                    "timestamp":"2024-05-08T19:00:00+08:00"
                },
                {
                    "value":1,
                    "timestamp":"2024-05-08T18:00:00+08:00"
                },
                {
                    "value":0,
                    "timestamp":"2024-05-08T07:00:00+08:00"
                }
            ]
        }
    ],
    "api_info":{
        "status":"healthy"
    }
}


def get_items_df(data_json, sublist_name):
    item_list = list()
    for i in data_json["items"]:
        for sub_dict in i[sublist_name]:
            sub_dict.update({k: v for k, v in i.items() if k != sublist_name})
            item_list.append(sub_dict)
    item_df = pd.json_normalize(item_list)
    return item_df

def get_items_df_2(data_json, sublist_name):
    item_df = pd.DataFrame()
    for i in data_json["items"]:
        item_df_i = pd.DataFrame.from_dict(i[sublist_name])
        item_df_i["timestamp"] = i["timestamp"]
        item_df_i["update_timestamp"] = i["update_timestamp"]
        item_df = pd.concat([item_df, item_df_i])
    return item_df

print("\n2:")
meta_df = pd.json_normalize(data_2["area_metadata"])
meta_df.rename(columns={"name": "area"}, inplace=True)
item_df = get_items_df(data_2, "forecasts")
df = pd.merge(meta_df, item_df, how="right", on="area")
print(df)


print("\n24:")
items_df = get_items_df(data_24, "periods")
print(items_df)


print("\n4:")
item_df = get_items_df(data_4, "forecasts")
print(items_df)


print("\nA, Ra, Re, WD, WS:")
meta_df = pd.json_normalize(data_A_Ra_Re_WD_WS["metadata"]["stations"])
meta_df.rename(columns={"id": "station_id"}, inplace=True)
item_df = get_items_df(data_A_Ra_Re_WD_WS, "readings")
df = pd.merge(meta_df, item_df, how="right", on="station_id")
# df["reading_type"] = data_A_Ra_Re_WD_WS["metadata"]["reading_type"]
df["reading_unit"] = data_A_Ra_Re_WD_WS["metadata"]["reading_unit"]
print(df)


print("\nPM:")
meta_df = pd.json_normalize(data_PM["region_metadata"])
item_df = get_items_df_2(data_PM, "readings")
df = pd.merge(meta_df, item_df, how="right", left_on="name", right_on=item_df.index)
print(df)


print("\nPo:")
meta_df = pd.json_normalize(data_Po["region_metadata"])
items_df = get_items_df_2(data_Po, "readings")
df = pd.merge(meta_df, items_df, how="right", left_on="name", right_on=item_df.index)
print(df)


print("\nU:")
item_list = list()
for i in data_U["items"]:
    for sub_dict in i["index"]:
        keys = list(sub_dict.keys())
        for key in keys:
            new_key = f"index.{key}"
            sub_dict[new_key] = sub_dict.pop(key)
        sub_dict.update({k: v for k, v in i.items() if k != "index"})
        item_list.append(sub_dict)
item_df = pd.json_normalize(item_list)
pd.set_option('display.max_columns', None)
print(item_df)
