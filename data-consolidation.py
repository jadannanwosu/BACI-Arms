
import numpy as np
import pandas as pd

'''
List of variables -
t: year
i: exporter
j: importer
k: product
v: value - thousand USD
q: quantity - metric ton
'''

# Merging CSVs
df_salw = pd.DataFrame(columns=['exporter', 'importer', 'year', 'product_code', 'value', 'weight'])

data_years = list(range(1995,2023))
prod_codes = [930100, 930120, 930190, 930200, 930320, 930330]


for year in data_years:
    fname = f'BACI_HS92_Y{year}_V202401b.csv'
    temp_df = pd.read_csv('/Users/jennenwosu/Documents/ROCCA_Arms/'+fname)
    temp_df = temp_df.rename(columns={'t':'year','i':'exporter','j':'importer','k':'product_code','v':'value','q':'weight'})
    temp_df = temp_df[temp_df['product_code'].isin(prod_codes)]
    df_salw = pd.concat([df_salw,temp_df])
    print(f'data for {year} added')
    print(20*'*')

df_salw.replace('NA', np.nan, inplace=True)
df_salw['weight'] = pd.to_numeric(df_salw['weight'], errors='coerce')
df_salw = df_salw.dropna()

aggregation_functions = {'value': 'sum', 'weight': 'sum'}
new_df_test = df_salw.groupby(['year','exporter','importer', 'product_code']).aggregate(aggregation_functions).reset_index()
df_salw = new_df_test

df_salw.to_csv('small_arms_95_22.csv')