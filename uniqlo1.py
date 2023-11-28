import requests
import pendulum
import pandas as pd
from requests_html import HTMLSession,AsyncHTMLSession
import datetime, time
import oss2
import os
from dotenv import load_dotenv
load_dotenv()

access_key_id = os.getenv('access_key_id')
access_key_secret = os.getenv('access_key_secret')
endpoint = 'oss-ap-southeast-5.aliyuncs.com'
bucket_name = 'poi-indonesia'


          
class uniqlo():
    def __init__(self, from_main=False):
      self.file_name = 'uniqlo'.replace('/', '_').replace('.py', '')
      self.from_main = from_main
      self.session = HTMLSession()
      self.content = list()
      start = time.time()
      self.get_data()
      x = pd.DataFrame(self.content)
      x = x.drop_duplicates('store_name')
      if from_main:
          x.to_csv(f"csv/{self.file_name}.csv",index=False)
      else:
          csv_data = x.to_csv(index=False).encode('utf-8')
          oss_object_key = f'{self.file_name}.csv'
    
          # Create an OSS auth instance
          auth = oss2.Auth(access_key_id, access_key_secret)
    
          # Create an OSS bucket instance
          bucket = oss2.Bucket(auth, endpoint, bucket_name)
    
          bucket.put_object(oss_object_key, csv_data)
    
          print(f'File {oss_object_key} uploaded to OSS bucket {bucket_name}')

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
            _data['store_name'] = store_name
            _data['address'] = address
            _data['tel_no'] = tel_no
            _data['url_store'] = 'https://map.uniqlo.com/id'
            _data['lat'] = lat
            _data['lon'] = lon
            _data['open_hours'] = open_hours
            utc_time = pendulum.now()
            indonesia = utc_time.in_timezone('Asia/Bangkok')
            _data["scrape_date"] = indonesia.strftime("%m/%d/%y")
            print(_data)
            self.content.append(_data)
            
if __name__ == '__main__':
    uniqlo(False)