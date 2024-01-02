import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# Replace 'your_shapefile.shp' with the actual path to your shapefile
def get_city(df):
    gdf_cities = gpd.read_file('assets/BATAS KABUPATEN KOTA DESEMBER 2019 DUKCAPIL.shp')
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