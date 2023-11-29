from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
from requests_html import HTMLSession,AsyncHTMLSession
import pandas as pd
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
#from gmaps_geocode import get_latlon
import pendulum
import oss2
import os
from dotenv import load_dotenv
load_dotenv()

access_key_id = os.getenv('access_key_id')
access_key_secret = os.getenv('access_key_secret')
endpoint = 'oss-ap-southeast-5.aliyuncs.com'
bucket_name = 'poi-indonesia'


class mcd():
    def __init__(self, from_main=False):
      self.file_name = 'mcd'.replace('/', '_').replace('.py', '')
      self.from_main = from_main
      self.session = HTMLSession()
      self.content = list()
      options = Options()
      options.add_argument('--headless')
      options.add_argument('--no-sandbox')
      options.add_argument('--disable-dev-shm-usage')
      self.browser = webdriver.Chrome(options=options)
      self.get_data()
      x = pd.DataFrame(self.content)
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
          csv_data = x.to_csv(index=False).encode('utf-8')
          oss_object_key = f'{self.file_name}.csv'
    
          # Create an OSS auth instance
          auth = oss2.Auth(access_key_id, access_key_secret)
    
          # Create an OSS bucket instance
          bucket = oss2.Bucket(auth, endpoint, bucket_name)
    
          bucket.put_object(oss_object_key, csv_data)
    
          print(f'File {oss_object_key} uploaded to OSS bucket {bucket_name}')
      
      
    def get_data(self):
        url = "https://www.mcdonalds.co.id/location"
        self.browser.get(url)
        time.sleep(3)
        soup = BeautifulSoup(self.browser.page_source,'lxml-html')
        self.browser.close()
        cards = soup.select("div.location-list")
        for card in cards:
            store_name = " ".join(card.select_one('.title h5').text.split())
            address = " ".join(card.select_one("p.quiet").text.split())
            tel_no = " ".join(card.select_one("a.btn-telephone")['href'].replace('tel:','').split())
            url_gmaps = card.select_one("a.btn-directions")['href']
            lat_lon = url_gmaps.split("Location/")[-1]   
            lat = lat_lon.split(",")[0]
            lon = lat_lon.split(",")[1]
            
            _data = dict()
            _data['store_name'] = store_name
            _data['address'] = address
            _data['tel_no'] = tel_no
            _data['url_store'] =  url
            _data['lat'] = lat
            _data['lon'] = lon    
            utc_time = pendulum.now()
            indonesia = utc_time.in_timezone('Asia/Bangkok')
            _data["scrape_date"] = indonesia.strftime("%Y/%m/%d")
            print(_data)
            print("jumlah data--",len(self.content))
            self.content.append(_data)
            
if __name__ == '__main__':
    mcd(False)      

