#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 10:06:56 2023

@author: slepot
"""

import geopandas as gpd
f=r"maps/swissBOUNDARIES3D_1_3_TLM_KANTONSGEBIET.shp"
shapes = gpd.read_file(f)
shapes.geometry=shapes.geometry.simplify(0.5)
import plotly.io as pio
pio.renderers.default = "browser"
import pyproj
from shapely.ops import transform

lv03 = pyproj.CRS('EPSG:21781')
wgs84 = pyproj.CRS('EPSG:4326')
project = pyproj.Transformer.from_crs(lv03, wgs84, 
                                          always_xy=True).transform

for i, shape in shapes.iterrows():
    if shape.geometry is not None:
        shape.geometry = transform(project, shape.geometry)
        shapes.iloc[i] = shape
        
import plotly.express as px

d = {'ids': ['1','2'], 
     'unemployment': [0.0, 12.0]
    }

fig = px.choropleth_mapbox(d, geojson=shapes, 
                           locations='ids', 
                           color='unemployment',
                           mapbox_style="carto-positron",
                           zoom=6.3, 
                           center = {"lat": 46.8, "lon": 8.5},
                           opacity=0.5,
                          )
fig.show()

gdf=gpd.GeoDataFrame(columns=shapes.columns)

for i, shape in shapes.iterrows():
  if shape.geometry is not None:
    if shape.NAME in gdf.NAME.values:
      available_geometry = gdf.loc[gdf.NAME == shape.NAME,   
                                        'geometry'].geometry.values
      gdf.loc[gdf.NAME == shape.NAME, 'geometry']=available_geometry.union(shape.geometry)
    else:
      gdf = gdf.append(shape)
gdf=gdf.drop(columns=['KT_TEIL']) # For the sake of clean data ;-)

gdf=gdf.rename(columns={'EINWOHNERZ':'Inhabitants'})

gdf = gdf[['geometry','NAME','Inhabitants','KANTONSNUM']]
fig = px.choropleth_mapbox(gdf, geojson=gdf.geometry,
                           locations=gdf.index,
                           color='Inhabitants', 
                           hover_name = 'NAME',
                           color_continuous_scale='algae',
                           mapbox_style="carto-positron",
                           zoom=7.3, 
                           center = {"lat": 46.8, "lon": 8.5},
                           opacity=0.5,
                           )
fig.show()
