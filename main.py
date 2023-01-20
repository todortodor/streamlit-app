#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 10 23:53:47 2023

@author: slepot
"""

#%% import librairies

# import pandas as pd
import polars as pl
import streamlit as st
from millify import millify
import plotly.express as px
import plotly.graph_objects as go
import time
import functools
import operator
from helper_0 import style_benchmark_table
import geopandas as gpd

#%% defines functions

#track start of script time for performance
start = time.perf_counter()

# default the app to wide mode
st.set_page_config(layout="wide")

# if we want to cache a function :
# @st.cache(persist=True, show_spinner=False, allow_output_mutation=True)

# load the data from a dictionary of the syntax {'year':y,:'path':path}
def load_data_set(yearly_data):
    return pl.concat([pl.scan_parquet(year_d['path']
                                      # ).with_column(pl.lit(name = "year_data",values = year_d['year'])
                                      ).with_column(pl.lit(year_d['year']).alias("year_data")
                                                    ) 
                      for year_d in yearly_data],
                     rechunk = True,
                     parallel = True).collect()

# get a list of cantons for the initial selector button                              
def get_list_of_options(data, column_name):
    return sorted(data.get_column(column_name).unique().to_list())

# filters the data for a selected list of cantons
def filter_for_selections(data, selection_dictionary):
    conditions = [
                  pl.col(column_name).is_in(selected_values) for column_name,selected_values in selection_dictionary.items()
                ]
    return data.filter(
        functools.reduce(operator.and_,conditions)
        )

# takes the data and outputs an aggregated version according to two columns. The percentage is for
# the percentage of "column_to_display" for a given value of "column_to_categorize"
def filter_compute_count_and_percent(data, 
                                     column_to_display, 
                                     column_to_categorize,
                                     new_percentage_column_name = 'percentage'):
    if 'count' not in data.columns:
        data_filtered = data.lazy().groupby([column_to_display,column_to_categorize]).agg(
                [
                pl.count(column_to_display).alias("count"),
                ]
            ).collect().sort([column_to_display,column_to_categorize])
    else:
        data_filtered = data#.sort([column_to_display,column_to_categorize])
            
    total_count = data_filtered.groupby(pl.col(column_to_categorize)).agg(
        [pl.sum('count').alias('total_count')]
        )
    data_filtered = data_filtered.join(total_count, on=column_to_categorize)
    data_filtered = data_filtered.with_column(((pl.col('count')/pl.col('total_count'))*100).alias(new_percentage_column_name))
    return data_filtered

@st.experimental_memo
def convert_df(df):
   return df.to_csv(index=False).encode('utf-8')

#%% load data, make initial filters

year_list = [2020,2021,2022,2023,2024]
yearly_data_paths = [{'year':y,'path':f'pet_data_{y}.parquet'} for y in year_list]

data = load_data_set(yearly_data_paths)

year_selected = st.sidebar.slider(
    'Choose a year',
    data['year_data'].min(), 
                data['year_data'].max(),
    )


if 'count_r_percentage' not in st.session_state:
    st.session_state['count_r_percentage'] = 'count'
    
st.session_state['count_r_percentage'] = st.sidebar.radio(
    'Metric',
    options = ['count','percentage'],
    )


list_of_cantons = get_list_of_options(data, 'kanton')

container_canton_selection = st.sidebar.container()

all_cantons_checkbox = st.sidebar.checkbox("Select all",value=True)
if all_cantons_checkbox:
    cantons_selected = container_canton_selection.multiselect("Cantons:",
         options = list_of_cantons,
         default = list_of_cantons)
else:
    cantons_selected = container_canton_selection.multiselect("Cantons:",
        options = list_of_cantons,
        default = [])
    
selection_dictionary = {'kanton':cantons_selected,
                        'year_data':[year_selected]}    

data = filter_for_selections(data,selection_dictionary)

# Metric columns
col = st.columns(6)
    
nbr_of_obs = data.shape[0]

col[0].metric(label = "Observations", value = millify(nbr_of_obs, precision=1))

average_value = float(data.select(pl.col("value")).mean()[0,0] or 0)

col[1].metric(label = "Value", value = millify(average_value, precision=1))

median_rating_CO2  = data["ratings_CO2_cls"].sort(reverse=True)[round(data.shape[0]/2)]

col[2].metric(label = "CO2 median rating", value = median_rating_CO2)

data_filtered = filter_compute_count_and_percent(data, 
                                                 'ratings_CO2_cls', 
                                                 'year_data')

wch_colour_box = (0,204,102)
wch_colour_font = (0,0,0)
fontsize = 18
valign = "left"
iconname = "fas fa-asterisk"
sline = "Observations"
lnk = '<link rel="stylesheet" href="https://use.fontawesome.com/releases/v5.12.1/css/all.css" crossorigin="anonymous">'
i = 123

htmlstr = f"""<p style='background-color: rgb({wch_colour_box[0]}, 
                                              {wch_colour_box[1]}, 
                                              {wch_colour_box[2]}, 0.75); 
                        color: rgb({wch_colour_font[0]}, 
                                   {wch_colour_font[1]}, 
                                   {wch_colour_font[2]}, 0.75); 
                        font-size: {fontsize}px; 
                        border-radius: 7px; 
                        padding-left: 12px; 
                        padding-top: 18px; 
                        padding-bottom: 18px; 
                        line-height:25px;'>
                        <i class='{iconname} fa-xs'></i> {i}
                        </style><BR><span style='font-size: 14px; 
                        margin-top: 0;'>{sline}</style></span></p>"""

col[3].markdown(lnk + htmlstr, unsafe_allow_html=True)

dictionary_colors = {
    'A':'#00A652',
    'B':'#51B747',
    'C':'#BDD630',
    'D':'#FEF200',
    'E':'#FDB813',
    'F':'#F37020',
    'G':'#EE1B24',
    'home':'#FFFFFF',
    'hotel':'#DAE6DF',
    'uni':'#DAE6DF',
    'factory':'#DAE6DF',
    'office':'#DAE6DF',
    'EG':'#DAE6DF',
    'EF':'#DAE6DF',
    'STE':'#DAE6DF',
    'EH':'#DAE6DF',
    }


    
#bar plot with categories and animation
fig_ratings_bar = px.bar(data_frame=data_filtered.to_pandas(), x='ratings_CO2_cls', y=st.session_state['count_r_percentage'], 
             color='ratings_CO2_cls', pattern_shape=None, 
              facet_row=None, facet_col=None, facet_col_wrap=0, facet_row_spacing=None, 
              facet_col_spacing=None, hover_name=None, hover_data=None, custom_data=None, 
              text=None, base=None, error_x=None, error_x_minus=None, error_y=None, 
              error_y_minus=None, animation_frame=None, animation_group=None, 
              category_orders=None, labels=dict(ratings_CO2_cls='CO2 rating', 
                                                count="Count",percentage = "Percentage")
              , color_discrete_sequence=None, 
              color_discrete_map=dictionary_colors, color_continuous_scale=None, 
              pattern_shape_sequence=None, pattern_shape_map=None, range_color=None, 
              color_continuous_midpoint=None, opacity=None, orientation=None, 
              barmode='relative', log_x=False, log_y=False, range_x=None, range_y=None, 
              text_auto=False, title=None, template=None, width=1200, height=None)
    
#update layout, here the margins
fig_ratings_bar.update_layout(
    margin=dict(l=0, r=0, t=30, b=0)
)
    
#display the fig
st.write(fig_ratings_bar)

with st.expander("Find the data here", expanded = False):
    download_data_bar = data_filtered.to_pandas()[
        ['ratings_CO2_cls','count','percentage']
        ].rename(columns = {'ratings_CO2_cls':'Rating CO2',
                                'count':'Number of buildings',
                                'percentage':'Percentage of buildings',
                                })
    st.table(style_benchmark_table(download_data_bar),
             # data_filtered.to_pandas()[['ratings_CO2_cls','count','percentage']]
             # .style.set_table_styles(styles, overwrite=False).apply(style_diag, axis=None)
             )
    
    data_download = convert_df(download_data_bar)
    st.download_button('Download data here',
                           data_download,
                           f'bar_data_{year_selected}.csv',
                           "text/csv",
                           # key='download-csv'
                        )


#Sunburst graph
def make_sunburst(yearly_data,col_list,dictionary_colors):
    data_filtered_sunburst = pl.concat(
        [yearly_data.lazy().groupby(col_list[:i+1]).agg(
            [pl.count('property_id').alias("count")]
            ).with_column(pl.col(col).alias("labels"))
            .with_column(pl.fold('', lambda acc, s: acc + s, pl.all().exclude([col,'parents','labels','count'])).alias('parents'))
            .with_column(pl.fold('', lambda acc, s: acc + s, pl.all().exclude(['parents','labels','count'])).alias('id'))
        for i, col in enumerate(col_list)
        ],
        how = "diagonal",
        rechunk = True,
        parallel = True).with_column(
            pl.col("labels").apply(lambda x: dictionary_colors[x]).alias('colors')
            ).sort(col_list).collect().to_pandas()
    
    fig = go.Sunburst(
        labels=data_filtered_sunburst['labels'].tolist(),
        parents=data_filtered_sunburst['parents'].tolist(),
        values=data_filtered_sunburst['count'].tolist(),
        ids=data_filtered_sunburst['id'].tolist(),
        branchvalues='total',
        # colors=data_filtered_sunburst['labels'].tolist()
        marker=dict(colors=data_filtered_sunburst['colors'].tolist())
    )
    return fig, data_filtered_sunburst

col_list = ['segment','ratings_CO2_cls','type_of_building']
sunburst_trace, sunburst_data = make_sunburst(data,col_list, dictionary_colors)

fig_sb = go.Figure(sunburst_trace)

fig_sb.update_layout(width=1200,height = 800) 

if st.session_state['count_r_percentage'] == 'count':
    fig_sb.update_traces(textinfo="label+value")
if st.session_state['count_r_percentage'] == 'percentage':
    fig_sb.update_traces(textinfo="label+percent parent")

st.write(fig_sb)

with st.expander("Find the data here", expanded = False):
    download_data_sb = sunburst_data.dropna()[
        ['ratings_CO2_cls','segment','type_of_building', 'count']
        ].rename(columns = {'ratings_CO2_cls':'Rating CO2',
                                'count':'Number of buildings',
                                'segment':'Segment',
                                'type_of_building':'Type of building',
                                })
    st.dataframe(style_benchmark_table(download_data_sb),
             # data_filtered.to_pandas()[['ratings_CO2_cls','count','percentage']]
             # .style.set_table_styles(styles, overwrite=False).apply(style_diag, axis=None)
             )
    data_download = convert_df(download_data_sb)
    st.download_button('Download data here',
                           data_download,
                           "sunburst_data.csv",
                           "text/csv",
                           # key='download-csv'
                        )

#Waterfall graph
col_list = ['ratings_CO2_cls','segment']
data_filtered_waterfall = pl.concat(
    [
     data.lazy().groupby(col_list).agg(
    [pl.count('property_id').alias("count")]
    ).sort(['segment','ratings_CO2_cls'],reverse=[False,True]),
     data.lazy().groupby(['ratings_CO2_cls']).agg(
    [pl.count('property_id').alias("count")]
    ).with_column(pl.lit('Total').alias('segment')).sort('ratings_CO2_cls',reverse=True)
    ],how='diagonal'
    ).with_column((pl.col('count')*200/pl.col('count').sum()).alias('percentage')).collect()

base_waterfall = [0]+pl.cumsum(data_filtered_waterfall.filter(
                pl.col('segment') != 'Total'
                )[st.session_state['count_r_percentage']]).to_list()[:-1]+[0]+pl.cumsum(data_filtered_waterfall.filter(
                pl.col('segment') == 'Total'
                )[st.session_state['count_r_percentage']]).to_list()[:-1]

data_filtered_waterfall = filter_compute_count_and_percent(data_filtered_waterfall, 
                                          'ratings_CO2_cls',
                                          'segment', 
                                          new_percentage_column_name = 'categorical_percentage')

#create properly formatted columns to display
data_filtered_waterfall = data_filtered_waterfall.with_column(
    (pl.col('categorical_percentage').round(2)+'%').alias('display_categorical_percentage'))
data_filtered_waterfall = data_filtered_waterfall.with_column(
    pl.col('count').apply(millify).alias('display_count'))

if st.session_state['count_r_percentage'] == 'percentage': 
    text_to_display_on_bars = 'display_categorical_percentage'
    text_to_display_on_hovers = 'categorical_percentage'
    custom_data = ['ratings_CO2_cls','segment', 'categorical_percentage','percentage']
    hovertemplate = "<br>".join([
        "Ratings CO2: %{customdata[0]}",
        "Segment: %{customdata[1]}",
        "Percentage: %{customdata[2]:.3f}%",
        "Percentage of total: %{customdata[3]:.3f}%",
        ])
    
if st.session_state['count_r_percentage'] == 'count': 
    text_to_display_on_bars = 'display_count'
    text_to_display_on_hovers = 'count'
    custom_data = ['ratings_CO2_cls','segment', 'count']
    hovertemplate = "<br>".join([
        "Ratings CO2: %{customdata[0]}",
        "Segment: %{customdata[1]}",
        "Count: %{customdata[2]}"])
fig_wf = px.bar(data_filtered_waterfall.to_pandas(), x='segment', y=st.session_state['count_r_percentage'],
                # hover_data=['ratings_CO2_cls', 'segment', text_to_display_on_hovers],
                color='ratings_CO2_cls',
                height=800,
                width=1200,
                color_discrete_map=dictionary_colors,
                text=text_to_display_on_bars,
                base=base_waterfall,
                custom_data=custom_data
                )
fig_wf.update_traces(
        hovertemplate=hovertemplate,
        textangle=0)

st.write(fig_wf)

with st.expander("Find the data here", expanded = False):
    download_data_wf = data_filtered_waterfall.to_pandas()[
        ['ratings_CO2_cls','segment','percentage','categorical_percentage']
        ].rename(columns = {'ratings_CO2_cls':'Rating CO2',
                                'segment':'Segment',
                                'percentage':'Percentage of total',
                                'categorical_percentage':'Percentage of category',
                                })
    st.table(style_benchmark_table(download_data_wf),
             # data_filtered.to_pandas()[['ratings_CO2_cls','count','percentage']]
             # .style.set_table_styles(styles, overwrite=False).apply(style_diag, axis=None)
             )
    
    data_download = convert_df(download_data_wf)
    st.download_button('Download data here',
                           data_download,
                           "waterfall_data.csv",
                           "text/csv",
                           # key='download-csv'
                        )

#stop performance clock and display time taken to run the script
stop = time.perf_counter()
st.write(stop-start)
