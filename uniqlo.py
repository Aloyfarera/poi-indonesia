import requests
import pendulum
import pandas as pd
from requests_html import HTMLSession,AsyncHTMLSession
import datetime, time
from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv
load_dotenv()

con_string = os.getenv('con_string')    
class uniqlo():
    def __init__(self, from_main=False):
      self.file_name = 'uniqlo'.replace('/', '_').replace('.py', '')
      self.from_main = from_main
      self.session = HTMLSession()
      self.content = list()
      self.get_data()
      x = pd.DataFrame(self.content)
      x = x.drop_duplicates('store_name')
      x = x.reindex(columns=['brand_code',
                            'store_name',
                            'address',
                            'tel_no',
                            'url_store',
                            'lat',
                            'lon',
                            'open_hours',
                            'services',
                            'scrape_date'])
      if from_main:
          x.to_csv(f"csv/{self.file_name}.csv",index=False)
      else:
        # upload to azure data lake storage
        csv_data = x.to_csv(index=False).encode('utf-8')
        blob_service_client = BlobServiceClient.from_connection_string(con_string)
        blob_obj = blob_service_client.get_blob_client(container="poi-indonesia", blob=f'csv/{self.file_name}.csv')
        blob_obj.upload_blob(csv_data,overwrite=True)
        print(f"Total data {self.file_name}: {x.shape}")
        print(f'File {self.file_name} uploaded to azure data lake storage poi-indonesia bucket')

    def get_data(self):
        headers = {
            'sec-ch-ua': '"Brave";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            'Referer': 'https://map.uniqlo.com/id/id/',
            'sec-ch-ua-mobile': '?0',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'sec-ch-ua-platform': '"Windows"',
        }
        
        params = {
            'limit': '100',
            'RESET': 'true',
            'lang': 'local',
            'offset': '0',
            'r': 'storelocator',
        }
        response = requests.get('https://map.uniqlo.com/id/api/storelocator/v1/id/stores', params=params, headers=headers).json()
        list_data = response['result']['stores']
        for data in list_data:
            store_name =data['name']
            address = data['address']
            lat = data['latitude']
            lon = data['longitude']
            tel_no = data['phone']
            url_store = 'https://map.uniqlo.com/id/id/'
            wd_openh = data['wdOpenAt'] + '-' + data['wdCloseAt']
            we_openh = data['weHolOpenAt'] + '-' + data['weHolCloseAt']
            open_hours = '[weekdays] ' + wd_openh +  ' [weekends] ' + we_openh
            print(open_hours)
            
            _data = dict()
            _data['brand_code'] = self.file_name.replace(".py",'')
            _data['store_name'] = store_name
            _data['address'] = address
            _data['tel_no'] = tel_no
            _data['url_store'] = url_store
            _data['lat'] = lat
            _data['lon'] = lon
            _data['open_hours'] = open_hours
            utc_time = pendulum.now()
            indonesia = utc_time.in_timezone('Asia/Bangkok')
            _data["scrape_date"] = indonesia.strftime("%Y/%m/%d")
            print(_data)
            self.content.append(_data)
            
if __name__ == '__main__':
    uniqlo(False)