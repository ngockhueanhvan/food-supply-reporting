import pandas as pd
import numpy as np
import os
import glob

base_dir = r'./data_in/'

# directory of csv files
data_in = glob.glob(os.path.join(base_dir,'*.csv'))

# selected columns from data input
columns = ['Area Code','Area','Element Code','Element','Item Code','Item','Year','Unit','Value']

# read and union all the data inputs
df = pd.DataFrame()
for file in data_in:
    data = pd.read_csv(file)
    data = data[columns]
    df = pd.concat([df,data],ignore_index=True)


# df = df.groupby([col for col in columns if col != 'Value']).agg({'Value':'sum'}).reset_index()

# trim element column
df['Element'].replace({' \(kcal/capita/day\)':'',' \(g/capita/day\)':''}, regex=True, inplace=True)

# calculate daily kcal supply of protein , which is equal daily protein quantity * 4
pro_kcal = df[df['Element']=='Protein supply quantity'].reset_index(drop = True)
pro_kcal['Element'] = 'Protein supply'
pro_kcal['Unit'] = 'kcal/capita/day'
pro_kcal['Value'] = pro_kcal['Value']*4

# calculate daily kcal supply of fat , which is equal daily fat quantity * 9
fat_kcal = df[df['Element']=='Fat supply quantity'].reset_index(drop = True)
fat_kcal['Element'] = 'Fat supply'
fat_kcal['Unit'] = 'kcal/capita/day'
fat_kcal['Value'] = fat_kcal['Value']*9

total_kcal = df[df['Element']=='Food supply'].reset_index(drop = True)

temp = pd.concat([total_kcal,pro_kcal,fat_kcal],ignore_index=True)
temp = pd.pivot_table(temp, index=['Area','Item Code','Item','Year','Unit'],columns='Element',values='Value',aggfunc=np.sum).reset_index()

# calculate carbs supply by sucstracting daily food supply by protein supply and fat supply
temp['Carbs supply'] = temp['Food supply']-temp['Protein supply']-temp['Fat supply']
temp['Carbs supply quantity'] = temp['Carbs supply']/4

carbs_kcal = temp[temp.columns.difference(['Fat supply', 'Food supply', 'Protein supply','Carbs supply quantity'])]
carbs_kcal['Element'] = 'Carbs supply'
carbs_kcal.rename(columns={'Carbs supply':'Value'},inplace=True)

carbs_quant  = temp[temp.columns.difference(['Fat supply', 'Food supply', 'Protein supply','Carbs supply'])]
carbs_quant['Element'] = 'Carbs supply quantity'
carbs_quant.rename(columns={'Carbs supply quantity':'Value'},inplace=True)
carbs_quant['Unit'] = 'g/capita/day'

df = pd.concat([df, pro_kcal, fat_kcal, carbs_kcal, carbs_quant], ignore_index=True)

df = df.rename(columns={'Item Code':'Item_Code'})

print(df.head())

# print(df[['Element','Unit']].drop_duplicates())



# # https://ourworldindata.org/diet-compositions

