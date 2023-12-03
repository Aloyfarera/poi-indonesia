import streamlit as st
from snowflake.snowpark import Session
import pandas as pd
from streamlit_folium import st_folium
import folium
import plotly.express as px
import plotly.graph_objects as go


st.set_page_config(page_icon='chart_with_upwards_trend',
                   layout="wide")

st.title('❄️ Iop-indonesia')
tab_titles = ['Dashboard', 'Map']
tab_dash, tab_map = st.tabs(tab_titles)

# Establish Snowflake session
@st.cache_resource
def create_session():
    return Session.builder.configs(st.secrets.snowflake).create()

session = create_session()

# Load data table
@st.cache_data
def load_data(table_name):
    ## Read in data table
    #st.write(f"Try load table `{table_name}`:")
    table = session.table(table_name)
    
    ## Do some computation on it
    #table = table.limit(100)
    
    ## Collect the results. This will run the query and download the data
    table = table.collect()
    return table

# Select and display data table
table_name = "POI_INDONESIA.PUBLIC.POI_INDONESIA_MASTER"
df_all = load_data(table_name)
def load_data2(df):
    df = pd.DataFrame(df)
    df['MONTH']  =pd.to_datetime(df['SCRAPE_DATE']).dt.strftime('%b')
    #df['CITY'] = df.apply(lambda row: get_city_from_coordinates(row['LAT'], row['LON']), axis=1)
    return df

with tab_dash:
    
    df = load_data2(df_all)
    ## Display data table
    with st.expander("See Table"):  
        choice_dash = st.selectbox("choose brand",df['BRAND_CODE'].unique()) 
        df = df[df['BRAND_CODE'] == choice_dash]
        df['SCRAPE_DATE'] = pd.to_datetime(df['SCRAPE_DATE'])
        df['YEAR'] = pd.to_datetime(df['SCRAPE_DATE']).dt.strftime('%Y')
        start = df.SCRAPE_DATE.min()
        end = df.SCRAPE_DATE.max()
        
        placeholder = st.empty()
        
        with placeholder.container():
                kpi1, kpi2 = st.columns(2)
                start_date = st.date_input('Start date', start)
                end_date = st.date_input('End date', end)        
                df1 = df.loc[(df['SCRAPE_DATE'].dt.date >= start_date) & (df['SCRAPE_DATE'].dt.date <= end_date) ]
                st.dataframe(df1)




# with tab_map:
#     def draw_folium_map(df,brand_choice):
#         # Creating the Folium Map
        
#         CircuitsMap = folium.Map(location=['110.51870',
#                                             '-7.43252']
#                                             , zoom_start=5
#                                             , control_scale=True
#                                             , tiles='openstreetmap')

#         # Adding Tile Layers
#         folium.TileLayer('openstreetmap').add_to(CircuitsMap)
#         folium.TileLayer('cartodb positron').add_to(CircuitsMap)
#         folium.TileLayer('stamenterrain').add_to(CircuitsMap)
#         folium.TileLayer('stamentoner').add_to(CircuitsMap)
#         folium.TileLayer('stamenwatercolor').add_to(CircuitsMap)
#         folium.TileLayer('cartodbdark_matter').add_to(CircuitsMap)

#         # Other mapping code (e.g. lines, markers etc.)
#         folium.LayerControl().add_to(CircuitsMap)


#         for index, location_info in df.iterrows():
#             folium.Marker([location_info["LAT"], location_info["LON"]], popup='<a href=' + location_info["BRAND_CODE"] + ' target="_blank">' + location_info["BRAND_CODE"] + '</a>', icon=folium.Icon(icon_color='white', icon="car", prefix='fa', color='darkgreen')).add_to(CircuitsMap)   

#         # Zoom to LAT, LONG bounds
#         lft_dwn = df[['LAT', 'LON']].min().values.tolist() # Left Down
#         top_rgt = df[['LAT', 'LON']].max().values.tolist() # Top Right
        
#         CircuitsMap.fit_bounds([lft_dwn, top_rgt]) 
#         return CircuitsMap
#     df = load_data2(df_all)
#     choice_map = st.selectbox("choose brand to show in map",df['BRAND_CODE'].unique())
#     df = df[df['BRAND_CODE'] == choice_map]
#     st_folium(draw_folium_map(df,choice_map) , width = 500)

def plot_bottom_left(df):
    month_counts = df.groupby('MONTH').size().reset_index(name='COUNT')
    fig = px.line(
        month_counts,
        x="MONTH",
        y='COUNT',
        markers=True,
        title="Number of Stores per Month",
    )
    
    fig.update_traces(textposition="top center")
    st.plotly_chart(fig, use_container_width=True)

bottom_left_column, bottom_right_column = st.columns(2)
with bottom_left_column:
    df  = load_data2(df)
    plot_bottom_left(df)