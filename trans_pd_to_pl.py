# -*- coding: utf-8 -*-
"""
Created on Thu Oct 23 14:03:02 2025

@author: miona
"""
#import pandas as pd
import matplotlib.pyplot as plt
import polars as pl


# =============================================================================
# #pandas style
# broken_df = pd.read_csv("./data/bikes.csv", encoding="ISO-8859-1") #read data
# 
# broken_df[:3] #pick up first 3 rows
# 
# fixed_df = pd.read_csv(
#     "./data/bikes.csv",
#     sep=";",
#     encoding="latin1",
#     parse_dates=["Date"],
#     dayfirst=True,
#     index_col="Date",
# )
# fixed_df[:3] #read collectly and pick up 3 rows
# 
# fixed_df["Berri 1"] #chose a specific column
# fixed_df["Berri 1"].plot() #plot a specific columns
# fixed_df.plot(figsize=(15, 10)) #plot all and resize figures
# =============================================================================

#polars style
broken_df = pl.read_csv("./data/bikes.csv", encoding="ISO-8859-1")
broken_df.head(3)

fixed_df = pl.read_csv(
    "./data/bikes.csv",
    separator=";",
    encoding="latin1",
    try_parse_dates=True
)
fixed_df.head(3)
fixed_df.select("Date","Berri 1") #indexを追加する

fixed_df_pd = fixed_df.to_pandas()
fixed_df_pd["Berri 1"].plot()
plt.show()

fixed_df_pd.plot(figsize=(15, 10))
plt.show()
