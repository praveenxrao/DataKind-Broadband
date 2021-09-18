#%%
import os
import glob
import pandas as pd
import pyarrow.parquet as pq
import geopandas as gp

from shapely.geometry import Point
from adjustText import adjust_text

import matplotlib
import matplotlib.pyplot as plt

#%%
os.getcwd()
#%%
#data_file = '../data/parquet/2019-01-01_performance_fixed_tiles.parquet'
#data_dir = '/Users/praveenrao/Google Drive/DataKind/Broadband/data/parquet'
#data_dir = '../Data/parquet'
data_dir = '../Data/shapefiles'

#%%

#df_fixed = pd.concat([pq.read_table(source=f).to_pandas() for f in glob.glob(data_dir + '/*_fixed_*.parquet')], ignore_index=True)

#%%

df_list = []
bb_type = 'fixed'
suffix = 'zip'
#suffix = 'parquet'
for year in [2019, 2020, 2021]:
    for file in glob.glob(data_dir + '/' + str(year) + '*_' + bb_type + '_*.' + suffix):
        print(file)
        #df = pq.read_table(source=file).to_pandas()
        df = gp.read_file(file)
        df['year'] = year
        df['bb_type'] = bb_type
        df_list.append(df)

#%%

df_fixed = pd.concat(df_list)

#%%



#%%

df_fixed.info()

#%%

df_fixed.head()

#%%

# Load raw data

#df = pd.read_csv(data_file)


#%%

#%%

df_fixed_yearly.info()

#%%

df_fixed_yearly.head()

#%%
#df_fixed_yearly['geometry'] = gp.GeoSeries.from_wkt(df['tile'])
#%%
# Florida State Code = 12

#%%
# zipfile of U.S. county boundaries
county_url = "https://www2.census.gov/geo/tiger/TIGER2019/COUNTY/tl_2019_us_county.zip"
counties = gp.read_file(county_url)
#%%
# filter out the FL fips code
fl_counties = counties.loc[counties['STATEFP'] == '12'].to_crs(4326)
#%%

fl_counties.head()

#%%
#df_fl = pd.DataFrame(fl_counties)
#%%

tiles_in_fl = gp.sjoin(df_fixed, fl_counties, how='inner', op='intersects')
#%%
df_fl = pd.DataFrame(tiles_in_fl)
#%%
tiles_in_fl.head()
#%%
tiles_in_fl.info()
#%%
df_fl.head()
#%%
df_fl.info()
#%%
df_fl_fixed_yearly = df_fl.groupby(['year','bb_type']).agg(
    median_d_kbps = pd.NamedAgg(column='avg_d_kbps', aggfunc='median'),
    median_u_kbps = pd.NamedAgg(column='avg_u_kbps', aggfunc='median'),
    median_lat_ms = pd.NamedAgg(column='avg_lat_ms', aggfunc='median'),
    median_tests = pd.NamedAgg(column='tests', aggfunc='median'),
    median_devices = pd.NamedAgg(column='devices', aggfunc='median')
).reset_index()
#%%
#df_fl['geometry'] = df_fl['geometry'].astype(str)
#%%

#df_fl_fixed_yearly = pd.merge(df_fixed_yearly, df_fl, left_on='geometry', right_on='geometry', how='inner')

#%%

df_fl_fixed_yearly.info()
#%%

df_fl_fixed_yearly.head()
#%%
