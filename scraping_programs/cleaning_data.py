import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from oauth2client.service_account import ServiceAccountCredentials
import logging
import pandas as pd
import time

def update_google_sheet(brand_code,x):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    SPREADSHEET_ID = '19ar_D7OC-Gycg27XFivsXDhZftJ5giEau1npbE7JbBw'
    SERVICE_ACCOUNT_FILE = '/root/airflow/poi-indonesia/poi-indonesia/gsheet/keys.json'
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    worksheet = spreadsheet.sheet1

    brand_code_values = worksheet.col_values(1)[1:]  # Skip header
    current_total_rows = len(brand_code_values)

    # Find the index of the brand_code in the list of brand_code_values
    try:
        row_index = brand_code_values.index(brand_code) + 2  # Account for header and 0-based index
        worksheet.update(f'B{row_index}', 'success')
        print(f"Updated 'success' for brand_code {brand_code} in row {row_index}.")

        # Update the 'total_rows' column with the total number of rows in x
        total_rows = x.shape[0]
        worksheet.update(f'C{row_index}', total_rows)
        print(f"Updated 'total_rows' for brand_code {brand_code} in row {row_index} with {total_rows}.")

    except ValueError:
        # If brand_code not found, append it to the end of the sheet and update the columns
        new_row = [brand_code] + [''] * (current_total_rows - 1)  
        worksheet.append_row(new_row)
        print(f"Brand_code {brand_code} added to the end of the sheet.")

        # Find the index of the new brand_code
        new_row_index = current_total_rows + 2
        for idx, value in enumerate(brand_code_values):
            if not value:
                new_row_index = idx + 2 
                break

        # Update the 'success' and 'total_rows' columns
        worksheet.update(f'B{new_row_index}', 'success')
        print(f"Updated 'success' for brand_code {brand_code} in row {new_row_index}.")

        total_rows = x.shape[0]
        worksheet.update(f'C{new_row_index}', total_rows)
        print(f"Updated 'total_rows' for brand_code {brand_code} in row {new_row_index} with {total_rows}.")
    

        
# Replace 'your_shapefile.shp' with the actual path to your shapefile
def get_city(df):
    print('classify indonesian city from coordinates..')
    gdf_cities = gpd.read_file('/root/airflow/poi-indonesia/poi-indonesia/assets/BATAS KABUPATEN KOTA DESEMBER 2019 DUKCAPIL.shp')
    geometry = [Point(xy) for xy in zip(df['lon'], df['lat'])]
    geo_df_points = gpd.GeoDataFrame(df, geometry=geometry, crs=gdf_cities.crs)
    result = gpd.sjoin(geo_df_points, gdf_cities, how="left", op="within")
    df['CITY'] = result['KAB_KOTA'].reset_index(drop=True)
    return df

def clean_dataframe(df):
    # Reindexing the DataFrame
    df = df.reset_index(drop=True)

    # Dropping unnecessary columns
    #columns_to_drop = ['url_store']
    #df = df.drop(columns=columns_to_drop, errors='ignore')

    # Renaming columns
    column_mapping = {
        'store_name': 'Store Name',
        'url_store' : "Url Store",
        'address': 'Address',
        'tel_no': 'Telephone Number',
        'lat': 'Latitude',
        'lon': 'Longitude',
        'open_hours': 'Opening Hours',
        'services': 'Services',
        'scrape_date': 'Scrape Date'
    }
    df = df.rename(columns=column_mapping)
