from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from requests_html import HTMLSession
import pandas as pd

url = 'https://p2k.stekom.ac.id/ensiklopedia/Daftar_kabupaten_dan_kota_di_Indonesia'
req = HTMLSession().get(url,headers={'User-Agent': UserAgent().random})
soup =  BeautifulSoup(req.content, "html.parser")
cards = soup.select("table:nth-of-type(n+2) tr:nth-of-type(n+2)")
all_info = []
for card in cards:
    info = {}
    info['kab_kota'] = " ".join(card.select_one("table:nth-of-type(n+2) td:nth-of-type(2) a").text.split())
    try:
        info['ibu_kota'] = " ".join(card.select_one("table:nth-of-type(n+2) td:nth-of-type(3) a:nth-of-type(1)").text.split())
    except:
        info['ibu_kota']  = ''
    info['Luas wilayah (km2)'] = " ".join(card.select_one("table:nth-of-type(n+2) td:nth-of-type(5)").text.split())
    info['Jumlah penduduk (2022)']= " ".join(card.select_one("table:nth-of-type(n+2) td:nth-of-type(6)").text.split())
    info['IPM']= " ".join(card.select_one("table:nth-of-type(n+2) td:nth-of-type(7)").text.split())
    print(info)
    all_info.append(info)
df = pd.DataFrame(all_info)
df.to_csv("city list indonesia.csv",index=False)