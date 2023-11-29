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
from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv
load_dotenv()

con_string = os.getenv('con_string') 
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
        #upload to azure data lake storage
        csv_data = x.to_csv(index=False).encode('utf-8')
        blob_service_client = BlobServiceClient.from_connection_string(con_string)
        blob_obj = blob_service_client.get_blob_client(container="poi-indonesia", blob=f'csv/{self.file_name}.csv')
        blob_obj.upload_blob(csv_data,overwrite=True)
        print(f"Total data {self.file_name}: {x.shape}")
        print(f'File {self.file_name} uploaded to azure data lake storage poi-indonesia bucket')
      
      
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
            _data["scrape_date"] = indonesia.strftime("%m/%d/%Y")
            print(_data)
            print("jumlah data--",len(self.content))
            self.content.append(_data)
            
if __name__ == '__main__':
    mcd(True)      

