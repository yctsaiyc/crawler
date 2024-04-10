from ETL_tool import ETLprocessor
import json
from datetime import datetime,timedelta
import requests
import pandas as pd
import os
import time
import schedule



class ETLprocessorLive(ETLprocessor):
    def __init__(self,config_file) -> None:
         with open(config_file,'r') as file:
            self.config =json.load(file)
    def process_api(self, api_name):
        return super().process_api(api_name)
    def process_live_api1(self, api_url,map_columns,path):
        try:
            r = requests.get(api_url)
            content = json.loads(r.text)
            df = pd.DataFrame(content)
            df.columns = map_columns
            current_date = datetime.now().date()
            df['date'] = current_date.strftime('%Y-%m-%d %H:%M:%S')
            os.makedirs(path, exist_ok=True)
            filename = int(time.mktime(current_date.timetuple()))
            csv_file_path = os.path.join(path, f"stock_day_all_{filename}.csv")
            print(csv_file_path)
            df.to_csv(csv_file_path,index=False)
        except Exception as e:
            print(e)
    def process_live_api2(self, api_url,map_columns,path):
        try:
            r = requests.get(api_url)
            content = json.loads(r.text)
            df = pd.DataFrame(content)
            df = df.rename(columns=map_columns)
            current_date = datetime.now().date()
            df['date'] = current_date.strftime('%Y-%m-%d %H:%M:%S')
            os.makedirs(path, exist_ok=True)
            filename = int(time.mktime(current_date.timetuple()))
            csv_file_path = os.path.join(path, f"listed_mi_margn_{filename}.csv")
            df.to_csv(csv_file_path,index=False)
        except Exception as e:
            print(e)
    def process_live_api3(self,api_url,map_columns,path):
        try:
            r=requests.get(api_url)
            content = json.loads(r.text)
            df = pd.DataFrame(content)
            df = df.rename(columns=map_columns)
            df['date'] = df['date'].apply(self.roc_to_ad)
            df['data_year_month']=df['data_year_month'].apply(self.roc_to_ad)
            current_date = datetime.now().date()
            os.makedirs(path, exist_ok=True)
            filename = int(time.mktime(current_date.timetuple()))
            csv_file_path = os.path.join(path, f"listed_revenue_{filename}.csv")
            df.to_csv(csv_file_path,index=False)
        except Exception as e:
            print(e)
    def process_live_api_listed_EPS_AL(self,api_url,map_columns,path):
        try:
            r=requests.get(api_url)
            content = json.loads(r.text)
            df = pd.DataFrame(content)
            df = df.rename(columns=map_columns)
            df['date'] = df['date'].apply(self.roc_to_ad)
            if 'year' in df.columns:
                df['year'] = df['year'].apply(self.roc_to_ad)
            if 'year_month' in df.columns:
                df['year_month'] = df['year_month'].apply(self.roc_to_ad)
            if 'delisting_date' in df.columns:
                df['delisting_date'] = df['delisting_date'].apply(self.roc_to_ad)
            if 'transaction_date' in df.columns:
                df['transaction_date'] = df['transaction_date'].apply(self.roc_to_ad)
            if 'applying_date' in df.columns:
                df['applying_date'] = df['applying_date'].apply(self.format_ad)
            if 'establishment_date' in df.columns:
                df['establishment_date'] = df['establishment_date'].apply(self.format_ad)
            if 'listing_date' in df.columns:
                df['listing_date'] = df['listing_date'].apply(self.format_ad)
            current_date = datetime.now().date()
            os.makedirs(path, exist_ok=True)
            filename = int(time.mktime(current_date.timetuple()))
            category = path.split("/")[-1]
            csv_file_path = os.path.join(path, f"{category}_{filename}.csv")
            print(csv_file_path)
            df.to_csv(csv_file_path,index=False)
        except Exception as e:
            print("ERROR:" ,e)
    def process_api_listed_new_listing(self,api_url,map_columns,path):
        try:
            r=requests.get(api_url)
            content = json.loads(r.text)
            df = pd.DataFrame(content)
            df = df.rename(columns=map_columns)
            date_lists = ['application_date','committee_date','approved_date','agreement_date','listing_date','approved_listing_date']
            for date_col in date_lists:
                df[date_col] = df[date_col].apply(self.roc_to_ad)
            current_date = datetime.now().date()
            os.makedirs(path, exist_ok=True)
            filename = int(time.mktime(current_date.timetuple()))
            category = path.split("/")[-1]
            csv_file_path = os.path.join(path, f"{category}_{filename}.csv")
            print(csv_file_path)
            df.to_csv(csv_file_path,index=False)
        except Exception as e:
            print("ERROR:" ,e)
    def process_api_tpex_esb_ac(self,api_url,map_columns,path):
        try:
            r=requests.get(api_url)
            content = json.loads(r.text)
            df = pd.DataFrame(content)
            df = df.rename(columns=map_columns)
            date_lists = ['date',
                          'tpex_listing_screening_committee_date',
                          'tpex_sanctioned_date',
                          'tpex_approved_trading_date',
                          'listing_date']
            for date_col in date_lists:
                df[date_col] = df[date_col].apply(self.format_ad)
            current_date = datetime.now().date()
            os.makedirs(path, exist_ok=True)
            filename = int(time.mktime(current_date.timetuple()))
            category = path.split("/")[-1]
            csv_file_path = os.path.join(path, f"{category}_{filename}.csv")
            print(csv_file_path)
            df.to_csv(csv_file_path,index=False)
        except Exception as e:
            print("ERROR:" ,e)
    def process_api_tpex_esb_ac1(self,api_url,map_columns,path):
        try :
            r=requests.get(api_url)
            content = json.loads(r.text)
            df = pd.DataFrame(content)
            df = df.rename(columns=map_columns)
            df['date'] = df['date'].apply(self.roc_to_ad)
            date_lists = ['date_of_incorporation',
                          'date_of_listing'
                          ]
            for date_col in date_lists:
                df[date_col] = df[date_col].apply(self.format_ad)
            current_date = datetime.now().date()
            os.makedirs(path, exist_ok=True)
            filename = int(time.mktime(current_date.timetuple()))
            category = path.split("/")[-1]
            csv_file_path = os.path.join(path, f"{category}_{filename}.csv")
            print(csv_file_path)
            df.to_csv(csv_file_path,index=False)
        except Exception as e:
            print("ERROR:" ,e)
    def process_live_cashflow(slef,api_url,map_columns,path,category):
        season = datetime.now().month//3 +1
        print("Fetch data time: ",datetime.now(),"at season:",season)
        # start_year = datetime(2020,1,1).year
        current_year = datetime.now().year
        if not os.path.exists(path):
            os.mkdir(path)
        # while start_year<=current_year:
        try:
            r = requests.post(api_url, {
            'encodeURIComponent':1,
            'step':1,
            'firstin':1,
            'off':1,
            'TYPEK':f'{category}',
            'year':str(current_year-1911),
            'season':str(season),
            })
            r.encoding = 'utf8'
            dfs = pd.read_html(r.text, header=None)
            _dfs = dfs[1:]
            tables = [table for table in map_columns.keys()]
            for index in range(len(tables)):#[0,1,2,3,4,5]
                if not os.path.exists(path+"/"+tables[index]):
                    os.mkdir(path+"/"+tables[index])
                try:
                    for i in range(len(_dfs)):
                        if set(map_columns[tables[index]].keys())==set(_dfs[i].columns):
                            _df = _dfs[i]
                            filename = str(tables[index])+"_"+str(current_year)+"_"+str(season)
                            csv_file_path = os.path.join(path,f"{tables[index]}/{filename}.csv")
                            _df.to_csv(csv_file_path,index=False)
                            print(csv_file_path)
                        else:
                            continue                         
                except IndexError as e:
                    print(e)
                time.sleep(2)
            print("year: ",current_year,"season: ",season)
        except Exception as e:
            print(f"Error: {e}")
        # start_year = start_year + 1
        print(f"Processing data for Cash flow API from {api_url}")
    def process_live_listed_dividend(self,api_url,map_columns,path):
        current_date = datetime.now()
        one_month_ago = current_date - timedelta(days=7)
        start_date_str = one_month_ago.strftime("%Y%m%d")
        end_date_str = current_date.strftime("%Y%m%d")
        # start_date_str = "20200101"#到時候可以直接註解
        api_url = api_url.format(start_date_str,end_date_str)
        try :
            r = requests.get(api_url)
            content = json.loads(r.text)
            data = content['data']
            orig_col= content['fields']
            df = pd.DataFrame(data,columns=orig_col)
            df = df.rename(columns=map_columns)
            df['date'] = df['date'].str.replace('年', '')
            df['date'] = df['date'].str.replace('月', '')
            df['date'] = df['date'].str.replace('日', '')
            df['date'] = pd.to_datetime(df['date'].astype(str).apply(lambda x: str(int(x) + 19110000)), format='%Y%m%d')
            df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            os.makedirs(path, exist_ok=True)
            filename = int(time.mktime(current_date.timetuple()))
            category = path.split("/")[-1]
            csv_file_path = os.path.join(path, f"{category}_{filename}.csv")
            print(csv_file_path)
            df.to_csv(csv_file_path,index=False)
            time.sleep(10)
        except Exception as e:
            print(e)
    def process_live_otc_3itrade(self,api_url,map_columns,path):
        current_date = datetime.now().date() - timedelta(days=1)        
        # end_date = datetime.now().date()
        # date_range = [current_date + timedelta(days=x) for x in range((end_date - current_date).days + 1)]
        # for current_date in date_range:
        roc_year = current_date.year - 1911
        formatted_date = current_date.strftime(f"{roc_year:02d}/%m/%d")
        print(f"Fetching data for {formatted_date}...")
        url = api_url.format(formatted_date)
        print(url)
        try:
            r = requests.get(url)
            content = json.loads(r.text)
            data = content['aaData']
            df = pd.DataFrame(data)
            df.columns= map_columns.values()
            df.drop(df.columns[-1], axis=1, inplace=True)
            for column in df.columns:
                df[column] = pd.to_numeric(df[column].str.replace(',', ''), errors='coerce').combine_first(df[column])
            df['date'] = current_date.strftime('%Y-%m-%d %H:%M:%S')
            os.makedirs(path, exist_ok=True)
            filename = int(time.mktime(current_date.timetuple()))
            category = path.split("/")[-1]
            csv_file_path = os.path.join(path, f"{category}_{filename}.csv")
            print(csv_file_path)
            df.to_csv(csv_file_path,index=False)
        except Exception as e:
            print(e)
        time.sleep(5)
    def process_live_otc_dividend(self,api_url,map_columns,path):
        current_date = datetime.now().date()
        one_week_ago = current_date - timedelta(days=7)
        start_date_str = one_week_ago.strftime(f"{one_week_ago.year-1911:02d}/%m/%d")
        end_date_str = current_date.strftime(f"{current_date.year-1911:02d}/%m/%d")
        # start_date_str = "109/01/01"#到時候直接註解
        api_url = api_url.format(start_date_str,end_date_str)
        print(api_url)
        try:
            r = requests.get(api_url)
            content = json.loads(r.text)
            data = content['aaData']
            df = pd.DataFrame(data)
            df.columns= map_columns.values()
            df['date'] = df['date'].str.replace('/', '')
            df['date'] = pd.to_datetime(df['date'].astype(str).apply(lambda x: str(int(x) + 19110000)), format='%Y/%m/%d')
            df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            os.makedirs(path, exist_ok=True)
            filename = int(time.mktime(current_date.timetuple()))
            category = path.split("/")[-1]
            csv_file_path = os.path.join(path, f"{category}_{filename}.csv")
            print(csv_file_path)
            df.to_csv(csv_file_path,index=False)
        except Exception as e:
            print(e)
        time.sleep(10)
    def process_live_rotc_dividend(self,api_url,map_columns,path):
        current_date = datetime.now().date()
        one_week_ago = current_date - timedelta(days=7)
        start_date_str = one_week_ago.strftime(f"{one_week_ago.year-1911:02d}/%m/%d")
        end_date_str = current_date.strftime(f"{current_date.year-1911:02d}%m%d")
        # start_date_str = "1090101"#可以直接註解
        api_url = api_url.format(start_date_str,end_date_str)
        print(api_url)
        try:
            r = requests.get(api_url)
            content = json.loads(r.text)
            data = content['aaData']
            df = pd.DataFrame(data)
            df.columns= map_columns
            df['date'] = df['date'].str.replace('/', '')
            df['date'] = pd.to_datetime(df['date'].astype(str).apply(lambda x: str(int(x) + 19110000)), format='%Y/%m/%d')
            df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            os.makedirs(path, exist_ok=True)
            filename = int(time.mktime(current_date.timetuple()))
            category = path.split("/")[-1]
            csv_file_path = os.path.join(path, f"{category}_{filename}.csv")
            print(csv_file_path)
            df.to_csv(csv_file_path,index=False)
        except Exception as e:
            print(e)
        time.sleep(10)
    def process_live_listed_3itrade_hedge_result(self,api_url,map_columns,path):
        current_date = datetime.now().date() - timedelta(days=1)        
        formatted_date = current_date.strftime(f"%Y%m%d")
        print(f"Fetching data for {formatted_date}...")
        url = api_url.format(formatted_date)
        print(url)
        try:
            r = requests.get(url)
            content = json.loads(r.text)
            data = content['data']
            df = pd.DataFrame(data)
            df.columns = map_columns.values()          
            df['date'] = current_date.strftime('%Y-%m-%d %H:%M:%S')
            os.makedirs(path, exist_ok=True)
            filename = int(time.mktime(current_date.timetuple()))
            category = path.split("/")[-1]
            csv_file_path = os.path.join(path, f"{category}_{filename}.csv")
            print(csv_file_path)
            df.to_csv(csv_file_path,index=False)
        except Exception as e:
            print(e)
        time.sleep(10)
    #DATA TRANSFORM FUNCTIONS
    def roc_to_ad(self,date_str):
        if not date_str:
            return None
        roc_year = int(date_str[:3]) + 1911
        ad_date_str = f"{roc_year}{date_str[3:]}"
        if len(date_str)==7:
            ad_date = pd.to_datetime(ad_date_str, format='%Y%m%d', errors='coerce')
        elif len(date_str)==3:
            ad_date = pd.to_datetime(ad_date_str, format='%Y', errors='coerce')
            return ad_date.strftime('%Y') if not pd.isnull(ad_date) else None
        else:
            ad_date = pd.to_datetime(ad_date_str, format='%Y%m', errors='coerce')
        return ad_date.strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(ad_date) else None
    def format_ad(self,date_str):
        if len(date_str) == 8:
            ad_date = pd.to_datetime(date_str,format='%Y%m%d',errors='coerce')
            return ad_date.strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(ad_date) else None
        elif len(date_str) == 10:
            ad_date = pd.to_datetime(date_str,format='%Y/%m/%d',errors='coerce')
            return ad_date.strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(ad_date) else None
if __name__ == "__main__":
    etl_processor =ETLprocessorLive("api_config_live.json")
    # print(etl_processor.process_api("API49"))
    # print(etl_processor.process_api("API50"))
    # print(etl_processor.process_api("API51"))

   #想辦法把排程搞出來
    def day_job():
        api_live_list_day=["API1","API2","API17","API18","API33","API34","API47","API48","API53","API56"]
        for api in api_live_list_day:
            etl_processor.process_api(api)
    #每月工作
    def monthly_job():
        api_live_list_month=["API3","API19"]
        today = datetime.now().day
        print(today)
        if today == 1:
            for api in api_live_list_month:
                etl_processor.process_api(api)
    def weely_job():
        api_live_list_week=["API9","API14","API52","API54","API55"]
        for api in api_live_list_week:
            etl_processor.process_api(api)
    def quarterly_job():
        api_live_list_season = ["API4","API5","API6",
                        "API7","API8","API9",
                        "API10","API11","API12",
                        "API13","API14","API15","API16"
                        "API20","API21","API22","API23",
                        "API24","API25","API26","API27",
                        "API28","API29","API30","API31",
                        "API32","API35","API36","API37",
                        "API38","API39","API40","API41",
                        "API42","API43","API44","API45",
                        "API46","API49","API50","API51"
                        ]        
        today = datetime.now().day
        month = datetime.now().month
        if today == 1 and month % 3 == 1:
            for api in api_live_list_season:
                etl_processor.process_api(api)
    schedule.every().day.at("00:00").do(day_job)
    schedule.every().day.at("00:00").do(monthly_job)
    schedule.every().day.at("00:00").do(quarterly_job)
    while True:
        schedule.run_pending()
        time.sleep(1)