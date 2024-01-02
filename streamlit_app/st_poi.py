import streamlit as st
from snowflake.snowpark import Session
import pandas as pd
from streamlit_folium import st_folium
import folium
import plotly.express as px
import plotly.graph_objects as go


st.set_page_config(page_icon='chart_with_upwards_trend',
                   layout="wide")

st.title('❄️ POI INDONESIA DASHBOARD')
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
    table = table.collect()
    return table

def load_data2(df):
    df = pd.DataFrame(df)
    df['MONTH']  =pd.to_datetime(df['SCRAPE_DATE']).dt.strftime('%b')
    #df['CITY'] = df.apply(lambda row: get_city_from_coordinates(row['LAT'], row['LON']), axis=1)
    return df

@st.cache_data
def convert_df_to_csv(df):
  # IMPORTANT: Cache the conversion to prevent computation on every rerun
  return df.to_csv(index=False).encode('utf-8')


def plot_bottom_left(df):
    month_counts = df.groupby('MONTH').size().reset_index(name='COUNT')
    fig = px.line(
        month_counts,
        x="MONTH",
        y='COUNT',
        markers=True,
        title=f"{choice_dash} Number of Stores per Month",
    )
    
    fig.update_traces(textposition="top center")
    st.plotly_chart(fig, use_container_width=True)

def plot_bottom_right(df):
    last_scraping_df = df.drop_duplicates("STORE_NAME").reset_index(drop=True)
    city_counts = last_scraping_df.groupby('CITY').size().reset_index(name='COUNT')
    city_counts = df.groupby('CITY').size().reset_index(name='COUNT')
    city_counts = city_counts.sort_values(by='COUNT', ascending=False)
    fig = px.bar(
        city_counts,
        x='CITY',
        y='COUNT',
        title=f"{choice_dash} Store Distribution by City",
        labels={'matched_city': 'City', 'COUNT': 'Number of Stores'},
    )

    fig.update_layout(
        xaxis=dict(title='City'),
        yaxis=dict(title='Number of Stores'),
    )

    st.plotly_chart(fig, use_container_width=True)
@st.cache_data
def draw_folium_map(df):
        df['LAT'] = pd.to_numeric(df['LAT'])
        df['LON'] = pd.to_numeric(df['LON'])
        map_center = [df['LAT'].mean(), df['LON'].mean()]
        folium_map = folium.Map(location=map_center, zoom_start=5)
        for index, row in df.iterrows():
            folium.Marker([row['LAT'], row['LON']], popup=row['BRAND_CODE']).add_to(folium_map)
        return folium_map

table_name = "POI_INDONESIA.PUBLIC.POI_INDONESIA_MASTER"
df_all = load_data(table_name)
with st.sidebar:
    st.header("Configuration")
    df = load_data2(df_all)
    choice_dash = st.selectbox("CHOOSE BRAND", ['All'] + df['BRAND_CODE'].unique().tolist())

    if choice_dash == 'All':
        df_filtered = df
    else:
        df_filtered = df[df['BRAND_CODE'] == choice_dash].reset_index(drop=True)  
    df_filtered['SCRAPE_DATE'] = pd.to_datetime(df_filtered['SCRAPE_DATE'])
    df_filtered['YEAR'] = pd.to_datetime(df_filtered['SCRAPE_DATE']).dt.strftime('%Y')
    start = df_filtered.SCRAPE_DATE.min()
    end = df_filtered.SCRAPE_DATE.max()
    start_date = st.date_input('Start date', start)
    end_date = st.date_input('End date', end) 

          
with tab_dash:
    bottom_left_column, bottom_right_column = st.columns(2)
    with bottom_left_column:
        #df  = load_data2(df)
        plot_bottom_left(df_filtered)
    with bottom_right_column:
        #df  = load_data2(df)
        plot_bottom_right(df_filtered)
    with st.expander("See Table"):  
        df1 = df_filtered.loc[(df_filtered['SCRAPE_DATE'].dt.date >= start_date) & (df_filtered['SCRAPE_DATE'].dt.date <= end_date) ]
        st.dataframe(df1)
        st.download_button(
        label="Download data as CSV",
        data=convert_df_to_csv(df1),
        file_name=f'{choice_dash}.csv',
        mime='text/csv',
        )

with tab_map:
    st_folium(draw_folium_map(df_filtered) , width=2000, height=800)