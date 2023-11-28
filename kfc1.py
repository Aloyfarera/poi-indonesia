import requests
import pendulum
import datetime, time
import pandas as pd
import http.client
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from requests_html import HTMLSession,AsyncHTMLSession
import oss2
import os
from dotenv import load_dotenv
load_dotenv()

access_key_id = os.getenv('access_key_id')
access_key_secret = os.getenv('access_key_secret')
endpoint = 'oss-ap-southeast-5.aliyuncs.com'
bucket_name = 'poi-indonesia'

class kfc():
    def __init__(self, from_main=False):
      self.file_name = 'kfc'.replace('/', '_').replace('.py', '')
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
      
    def update_cookies(self):
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        browser.get('https://kfcku.com/store')
        time.sleep(2)
        cookie_data = browser.get_cookies()
        cbr_ions_kfcku_cookie = next((cookie['value'] for cookie in cookie_data if cookie['name'] == 'cbr_ions_kfcku'), None)
        xsrf_token_cookie = next((cookie['value'] for cookie in cookie_data if cookie['name'] == 'XSRF-TOKEN'), None)
        cookies = dict()
        cookies['cbr_ions_kfcku'] = cbr_ions_kfcku_cookie 
        cookies['XSRF-TOKEN'] = xsrf_token_cookie
        browser.close()
        return cookies
    
    def get_data(self):
        cookies = {
            'XSRF-TOKEN': 'eyJpdiI6ImNzK0xXRUk4UjJXYXg2Z2lLWUd2Nmc9PSIsInZhbHVlIjoiQllMdlwvRU9nK3NZSE5hbUtyUzZFdWU1TG0xbXNoRDJZUkFqRnpuWTEyaVJLYWlCSXJyeU5wNnNrNEhQOW56eTBDYmVrXC9haWlaRCtrRHhcL3pcL1V5dTd3PT0iLCJtYWMiOiI3MDA4YmQwYzY5ZmRjMmMzMDUwMzYxNGU4ZWIyZmI0MjkzMTRkYjA1YzBmMTc1NTZlYmQ5ZWExNzgzNDhmNTcyIn0%3D',
            'cbr_ions_kfcku': 'eyJpdiI6IklmWTFvVWgwYys3bElNeWlMVml3Q1E9PSIsInZhbHVlIjoidkdjb0lFVFpCQ28zUlIyczlvYlR6RXJPOCsrZlBEK1Fwb28rdUs5VFBLQUVDelJaK3ZHVmxmekVqS1BZbElmcTBDUWhuYTVYRUJqOU42OFB1YXA4a3c9PSIsIm1hYyI6ImVjYzE5ZDZiZWFhZDYwYmI1ZTY3MzAzOTVlMTFjZmE5MjY0YTk2MDhlNTJkZTE4ZDcyODg3NGI3MmM2YjA1ZjAifQ%3D%3D',
        }
        
        headers = {
            'authority': 'kfcku.com',
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-language': 'en-US,en;q=0.9',
            # 'cookie': 'XSRF-TOKEN=eyJpdiI6IkRvVW1aWGkxbjRnNVpsaG5PWmpBMmc9PSIsInZhbHVlIjoidnFmQUJXMG9IdjRERnVhbTNcLzdoRmNZTW5uOVIrY0FaWVVRbzBVWDQ3Q0tiWGdTdENDclR4VlpcLzBUMElKRmtjOTVsSmJscExRQkpObjVJTEF1Z3hoQT09IiwibWFjIjoiODQ4N2NiOGY1NGQwM2QxZDk0YmZjYTEwN2E5ZTA1YjcwMjU1OWExNDc2NWIyYWNkMWJiNTc4YzM2NDE3YzVhNyJ9; cbr_ions_kfcku=eyJpdiI6Ilgzczc2cUFBYlNxQWtWV0hncm1rMEE9PSIsInZhbHVlIjoiNG1OTDFKV2pQUWZVckhROGFwXC9kQXpuaHdBS2lJeHJcL1R6eFZlb3dlYmlqSEFXTnJzQzZldkRCSmVMU09CMG1rendiUDBCd3hiTUcrUkR0MmhpXC9oZGc9PSIsIm1hYyI6ImMyNzBiZjdiYzZiY2ZmZjg1NGIzNDBiNDc1OTdkMzdlYTBhNDFkOWJhYmQwMGNjMjY5NTQ5ZjA0YjkzZjc3OTgifQ%3D%3D',
            'referer': 'https://kfcku.com/store',
            'sec-ch-ua': '"Brave";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sec-gpc': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
        }
        page = 0
        while True:
            params = {
                'page': f'{page}',
            }
            response = requests.get('https://kfcku.com/api/stores', params=params, cookies=cookies, headers=headers).json()
            last_page = response['last_page']
            if page % 25 == 0:
                 cookies = self.update_cookies()
            if page % 50 == 0:
                time.sleep(15)
            list_data = response['data']
            for store in list_data:
                store_name = store['name']
                district = store['district']['name']
                address = store['address'] +' '+ district
                lat = store['lat'] 
                lon = store['long']
                offices  = store['offices']
                try:
                    tel_no = offices['phone']
                except (KeyError,TypeError):
                    tel_no = ''
                try:
                    open_hours = " ".join(offices['working'])
                except (KeyError,TypeError):
                    open_hours= ''
                try:
                    services = ",".join([x['name'] for x in store['store_services']])
                except (KeyError,TypeError):
                    services = ''
                
                _data = dict()
                _data['store_name'] = store_name
                _data['address'] = address
                _data['tel_no'] = tel_no
                _data['url_store'] = 'https://kfcku.com/store'
                _data['lat'] = lat
                _data['lon'] = lon
                _data['open_hours'] = open_hours
                _data['services'] = services
                utc_time = pendulum.now()
                indonesia = utc_time.in_timezone('Asia/Bangkok')
                _data["scrape_date"] = indonesia.strftime("%m/%d/%y")
                print(_data)
                self.content.append(_data)
            page += 1
            print(page)
            if page == last_page:break


if __name__ == '__main__':
    kfc(False)



