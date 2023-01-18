# -*- coding: utf-8 -*-
"""
Created on Fri Jan  6 12:35:21 2023

@author: A278853
"""

#%% Librairies 
import pandas as pd
import numpy as np

#%% CS colors 
core_blue = "#003868"

# Bold blue
blue_bold_dark = "#002746"
blue_bold_mid = "#003868"
blue_bold_bright = "#265682"
blue_bold_light = "#507CAB"

# Dimmed blue
blue_dimmed_dark = "#4E6177"
blue_dimmed_mid = "#72869D"
blue_dimmed_bright = "#A7BBD4"
blue_dimmed_light = "#D7E4F6"

# Bold green
green_bold_dark = "#1C5038"
green_bold_mid = "#35684F"
green_bold_bright = "#4F8367"
green_bold_light = "#689C80"

# Dimmed green
green_dimmed_dark = "#687C72"
green_dimmed_mid = "#8EA298"
green_dimmed_bright = "#B7CCC0"
green_dimmed_light = "#DAE6DF"

# Bold gold
gold_bold_dark = "#B27500"
gold_bold_mid = "#C28C00"
gold_bold_bright = "#EBAF34"
gold_bold_light = "#F0C352"

# Dimmed gold
gold_dimmed_dark = "#B79961"
gold_dimmed_mid = "#D0B481"
gold_dimmed_bright = "#E5D2AF"
gold_dimmed_light = "#EDE1CF"

# Bold bronze
bronze_bold_dark = "#914927"
bronze_bold_mid = "#AE623F"
bronze_bold_bright = "#CB7C56"
bronze_bold_light = "#E9966F"

# Dimmed bronze
bronze_dimmed_dark = "#C08062"
bronze_dimmed_mid = "#D69E83"
bronze_dimmed_bright = "#E6BDAB"
bronze_dimmed_light = "#F3DFD4"

# Bold red
red_bold_dark = "#B6413F"
red_bold_mid = "#D45C56"
red_bold_bright = "#F37870"
red_bold_light = "#E9966F"

# Dimmed red
red_dimmed_dark = "#D7897F"
red_dimmed_mid = "#ECA69E"
red_dimmed_bright = "#E3BEB8"
red_dimmed_light = "#F7DDDA"

# Bold gray
gray_bold_dark = "#45464E"
gray_bold_mid = "#5D5E66"
gray_bold_bright = "#82838C"
gray_bold_light = "#9D9DA7"

# Dimmed gray
gray_dimmed_dark = "#82838C"
gray_dimmed_mid = "#9D9DA7"
gray_dimmed_bright = "#C5C6CF"
gray_dimmed_light = "#E1E2EC"

#%% Rating colors


#%% Style comparison table

#Highlight diagonal elements of a data frame 
def style_diag(data):
    diag_mask = pd.DataFrame("", index=data.index, columns=data.columns)
    min_axis = min(diag_mask.shape)
    diag_mask.iloc[range(min_axis), range(min_axis)] = 'background-color: #B7CCC0'
    return diag_mask

# Styler header and rows, highlight diagonal 
def style_benchmark_table(data, title="Table"):
    # df_styled = data.style.set_table_styles([
    #                         {
    #                             "selector":"thead",
    #                             "props": [("background-color", "dodgerblue"), ("color", "green"),
    #                                       ("border", "3px solid red"),
    #                                       ("font-size", "2rem"), ("font-style", "italic")]
    #                         },
    #                         {
    #                             "selector":"th.row_heading",
    #                             "props": [("background-color", "orange"), ("color", "green"),
    #                                       ("border", "3px solid black"),
    #                                       ("font-size", "2rem"), ("font-style", "italic")]
    #                         },
    #                     ])
    
    styles = [
        {"selector": "caption", 
         "props": [("text-align", "center"), ("font-size", "20pt"), ("color", "black")]
         },
        {"selector":"thead",
        "props": [("background-color", green_bold_dark), ("color", "white"),  
                  ("font-size", "1rem")]
        },
       {"selector":"th.col_heading",
       "props": [("background-color", green_bold_dark), ("color", "white"),  
                 ("font-size", "1rem")]
       },
       {"selector":"th.row_heading",
       "props": [("background-color", green_bold_dark), ("color", "white"), 
                 ("font-size", "1rem")]
       },
    ]

    df_styled = data.style.set_caption(title).set_table_styles(styles, overwrite=False).apply(style_diag, axis=None)
    
    
    return df_styled
