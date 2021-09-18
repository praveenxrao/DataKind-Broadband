#%%
import glob
import pandas as pd
import pyarrow.parquet as pq

#%%

#data_file = '../data/parquet/2019-01-01_performance_fixed_tiles.parquet'
data_dir = '/Users/praveenrao/Google Drive/DataKind/Broadband/data/parquet'
#%%

df_fixed = pd.concat([pq.read_table(source=f).to_pandas() for f in glob.glob(data_dir + '/*_fixed_*.parquet')], ignore_index=True)

#%%
df_list = []
bb_type = 'fixed'
for year in [2019, 2020, 2021]:
    for file in glob.glob(data_dir + '/' + str(year) + '*_' + bb_type + '_*.parquet'):
        print(file)
        df = pq.read_table(source=file).to_pandas()
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

df_fixed[df_fixed['quadkey'] == '0231113112003202'].head()

#%%

df_fixed.year.unique()

#%%
df_fixed_yearly = df_fixed.groupby(['year','bb_type','quadkey','tile']).agg(
    avg_d_kbps = pd.NamedAgg(column='avg_d_kbps', aggfunc='mean'),
    avg_u_kbps = pd.NamedAgg(column='avg_u_kbps', aggfunc='mean'),
    avg_lat_ms = pd.NamedAgg(column='avg_lat_ms', aggfunc='mean'),
    tests = pd.NamedAgg(column='tests', aggfunc='mean'),
    devices = pd.NamedAgg(column='devices', aggfunc='mean')
).reset_index()
#%%

df_fixed_yearly.info()
#%%
df_fixed_yearly.head()