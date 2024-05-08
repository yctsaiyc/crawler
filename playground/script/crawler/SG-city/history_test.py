import pandas as pd


data_2 = {
    "area_metadata":[
        {
            "name":"a1.n",
            "label_location":{
                "latitude":"a1.l.la",
                "longitude":"a1.l.lo"
            }
        },
        {
            "name":"a2.n",
            "label_location":{
                "latitude":"a2.l.la",
                "longitude":"a2.l.lo"
            }
        }
    ],
    "items":[
        {
            "update_timestamp":"i1.ut",
            "timestamp":"i1.t",
            "valid_period":{
                "start":"i1.v.s",
                "end":"i1.v.e"
            },
            "forecasts":[
                {
                    "area":"a1.n",
                    "forecast":"i1.f1.f"
                },
                {
                    "area":"a2.n",
                    "forecast":"i1.f2.f"
                }
            ]
        },
        {
            "update_timestamp":"i2.ut",
            "timestamp":"i2.t",
            "valid_period":{
                "start":"i2.v.s",
                "end":"i2.v.e"
            },
            "forecasts":[
                {
                    "area":"a1.n",
                    "forecast":"i2.f1.f"
                },
                {
                    "area":"a2.n",
                    "forecast":"i2.f2.f"
                }
            ]
        }
    ]
}

data_24 = {
    "items":[
        {
            "update_timestamp":"i1.ut",
            "timestamp":"i1.t",
            "valid_period":{
                "start":"i1.v.s",
                "end":"i1.v.e"
            },
            "general":{
                "forecast":"i1.g.f",
                "relative_humidity":{
                    "low":"i1.g.rh.l",
                    "high":"i1.g.rh.h"
                },
                "temperature":{
                    "low":"i1.g.t.l",
                    "high":"i1.g.t.h"
                },
                "wind":{
                    "speed":{
                        "low":"i1.g.w.s.l",
                        "high":"i1.g.w.s.h"
                    },
                    "direction":"i1.g.w.d"
                }
            },
            "periods":[
                {
                    "time":{
                        "start":"i1.p1.t.s",
                        "end":"i1.p1.t.e"
                    },
                    "regions":{
                        "west":"i1.p1.r.w",
                        "east":"i1.p1.r.e",
                        "central":"i1.p1.r.c",
                        "south":"i1.p1.r.s",
                        "north":"i1.p1.r.n"
                    }
                },
                {
                    "time":{
                        "start":"i1.p2.t.s",
                        "end":"i1.p2.t.e"
                    },
                    "regions":{
                        "west":"i1.p2.r.w",
                        "east":"i1.p2.r.e",
                        "central":"i1.p2.r.c",
                        "south":"i1.p2.r.s",
                        "north":"i1.p2.r.n"
                    }
                }
            ]
        },
        {
            "update_timestamp":"i1.ut",
            "timestamp":"i1.t",
            "valid_period":{
                "start":"i1.v.s",
                "end":"i1.v.e"
            },
            "general":{
                "forecast":"i1.g.f",
                "relative_humidity":{
                    "low":"i1.g.rh.l",
                    "high":"i1.g.rh.h"
                },
                "temperature":{
                    "low":"i1.g.t.l",
                    "high":"i1.g.t.h"
                },
                "wind":{
                    "speed":{
                        "low":"i1.g.w.s.l",
                        "high":"i1.g.w.s.h"
                    },
                    "direction":"i1.g.w.d"
                }
            },
            "periods":[
                {
                    "time":{
                        "start":"i2.p1.t.s",
                        "end":"i2.p1.t.e"
                    },
                    "regions":{
                        "west":"i2.p1.r.w",
                        "east":"i2.p1.r.e",
                        "central":"i2.p1.r.c",
                        "south":"i2.p1.r.s",
                        "north":"i2.p1.r.n"
                    }
                },
                {
                    "time":{
                        "start":"i2.p2.t.s",
                        "end":"i2.p2.t.e"
                    },
                    "regions":{
                        "west":"i2.p2.r.w",
                        "east":"i2.p2.r.e",
                        "central":"i2.p2.r.c",
                        "south":"i2.p2.r.s",
                        "north":"i2.p2.r.n"
                    }
                }
            ]
        }
    ],
    "api_info":{
        "status":"healthy"
    }
}

data_4 = {
    "items":[
        {
            "update_timestamp":"i1.ut",
            "timestamp":"i1.t",
            "forecasts":[
                {
                    "temperature":{
                        "low":"i1.f1.t.l",
                        "high":"i1.f1.t.h"
                    },
                    "date":"i1.f1.d",
                    "forecast":"i1.f1.f",
                    "relative_humidity":{
                        "low":"i1.f1.rh.l",
                        "high":"i1.f1.rh.h"
                    },
                    "wind":{
                        "speed":{
                            "low":"i1.f1.w.s.l",
                            "high":"i1.f1.w.s.h"
                        },
                        "direction":"i1.f1.w.d"
                    },
                    "timestamp":"i1.f1.t"
                },
                {
                    "temperature":{
                        "low":"i1.f2.t.l",
                        "high":"i1.f2.t.h"
                    },
                    "date":"i1.f2.d",
                    "forecast":"i1.f2.f",
                    "relative_humidity":{
                        "low":"i1.f2.rh.l",
                        "high":"i1.f2.rh.h"
                    },
                    "wind":{
                        "speed":{
                            "low":"i1.f2.w.s.l",
                            "high":"i1.f2.w.s.h"
                        },
                        "direction":"i1.f2.w.d"
                    },
                    "timestamp":"i1.f2.t"
                }
            ]
        },
        {
            "update_timestamp":"i2.ut",
            "timestamp":"i2.t",
            "forecasts":[
                {
                    "temperature":{
                        "low":"i2.f1.t.l",
                        "high":"i2.f1.t.h"
                    },
                    "date":"i2.f1.d",
                    "forecast":"i2.f1.f",
                    "relative_humidity":{
                        "low":"i2.f1.rh.l",
                        "high":"i2.f1.rh.h"
                    },
                    "wind":{
                        "speed":{
                            "low":"i2.f1.w.s.l",
                            "high":"i2.f1.w.s.h"
                        },
                        "direction":"i2.f1.w.d"
                    },
                    "timestamp":"i2.f1.t"
                }
            ]
        }
    ],
    "api_info":{
        "status":"healthy"
    }
}

data_A_Ra_Re_WD_WS = {
    "metadata":{
        "stations":[
            {
                "id":"m.s1.i",
                "device_id":"m.s1.di",
                "name":"m.s1.n",
                "location":{
                    "latitude":"m.s1.l.la",
                    "longitude":"m.s1.l.lo"
                }
            },
            {
                "id":"m.s2.i",
                "device_id":"m.s2.di",
                "name":"m.s2.n",
                "location":{
                    "latitude":"m.s2.l.la",
                    "longitude":"m.s2.l.lo"
                }
            }
        ],
        "reading_type":"m.rt",
        "reading_unit":"m.ru"
    },
    "items":[
        {
            "timestamp":"i1.t",
            "readings":[
                {
                    "station_id":"m.s1.i",
                    "value":"i1.r1.v"
                },
                {
                    "station_id":"m.s2.i",
                    "value":"i1.r2.v"
                }
            ]
        },
        {
            "timestamp":"i2.t",
            "readings":[
                {
                    "station_id":"m.s1.i",
                    "value":"i2.r1.v"
                },
                {
                    "station_id":"m.s2.i",
                    "value":"i2.r2.v"
                }
            ]
        }
    ],
    "api_info":{
        "status":"healthy"
    }
}

data_PM = {
    "region_metadata":[
        {
            "name":"west",
            "label_location":{
                "latitude":"rm_w.l.la",
                "longitude":"rm_w.l.lo"
            }
        },
        {
            "name":"east",
            "label_location":{
                "latitude":"rm_e.l.la",
                "longitude":"rm_e.l.lo"
            }
        },
        {
            "name":"central",
            "label_location":{
                "latitude":"rm_c.l.la",
                "longitude":"rm_c.l.lo"
            }
        },
        {
            "name":"south",
            "label_location":{
                "latitude":"rm_s.l.la",
                "longitude":"rm_s.l.lo"
            }
        },
        {
            "name":"north",
            "label_location":{
                "latitude":"rm_n.l.la",
                "longitude":"rm_n.l.lo"
            }
        }
    ],
    "items":[
        {
            "timestamp":"i1.t",
            "update_timestamp":"i1.ut",
            "readings":{
                "pm25_one_hourly":{
                    "west":"i1.r.p.w",
                    "east":"i1.r.p.e",
                    "central":"i1.r.p.c",
                    "south":"i1.r.p.s",
                    "north":"i1.r.p.n"
                }
            }
        },
        {
            "timestamp":"i2.t",
            "update_timestamp":"i2.ut",
            "readings":{
                "pm25_one_hourly":{
                    "west":"i2.r.p.w",
                    "east":"i2.r.p.e",
                    "central":"i2.r.p.c",
                    "south":"i2.r.p.s",
                    "north":"i2.r.p.n"
                }
            }
        }
    ],
    "api_info":{
        "status":"healthy"
    }
}

data_Po = None

data_U = None


print("\n2:")
item_list = list()
for i in data_2["items"]:
    for f in i["forecasts"]:
        f["timestamp"] = i["timestamp"]
        f["update_timestamp"] = i["update_timestamp"]
        f["start"] = i["valid_period"]["start"]
        f["end"] = i["valid_period"]["end"]
        item_list.append(f)
item_df = pd.json_normalize(item_list)
meta_df = pd.json_normalize(data_2["area_metadata"])
meta_df.rename(columns={"name": "area"}, inplace=True)
df = pd.merge(meta_df, item_df, how="right", on="area")
print(df)

print("\nA, Ra, Re, WD, WS:")
item_list = list()
for i in data_A_Ra_Re_WD_WS["items"]:
    for r in i["readings"]:
        r["timestamp"] = i["timestamp"]
        item_list.append(r)
item_df = pd.json_normalize(item_list)
meta_df = pd.json_normalize(data_A_Ra_Re_WD_WS["metadata"]["stations"])
meta_df.rename(columns={"id": "station_id"}, inplace=True)
df = pd.merge(meta_df, item_df, how="right", on="station_id")
# df["reading_type"] = data_A_Ra_Re_WD_WS["metadata"]["reading_type"]
df["reading_unit"] = data_A_Ra_Re_WD_WS["metadata"]["reading_unit"]
print(df)
