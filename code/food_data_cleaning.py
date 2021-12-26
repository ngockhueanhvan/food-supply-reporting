import pandas as pd
import numpy as np
import os
import glob
pd.options.mode.chained_assignment = None  # default='warn'


absolutepath = os.path.abspath(__file__)
fileDirectory = os.path.dirname(absolutepath)
parentDirectory = os.path.dirname(fileDirectory)

base_dir = os.path.join(parentDirectory, 'data_in/food_daily_consumption') 

# directory of csv files
data_in = glob.glob(os.path.join(base_dir, 'raw_data/*.csv'))

# selected columns from data input
columns = ['Area Code (FAO)', 'Area', 'Element Code',
           'Element', 'Item Code', 'Item', 'Year', 'Unit', 'Value']
renamed_columns = ['Area_Code', 'Area', 'Element_Code',
                   'Element', 'Item_Code', 'Item', 'Year', 'Unit', 'Value']

# read and union all the data inputs
df = pd.DataFrame()
for file in data_in:
    data = pd.read_csv(file,encoding='UTF-8')
    data = data[columns]
    df = pd.concat([df, data], ignore_index=True)


# rename df
df.columns = renamed_columns

# add item_group column to df based on the lookup table
# read the lookup table from location
item_group_lookup = pd.read_csv('data_in/food_daily_consumption/Item_Group_Lookup.csv')
item_group_lookup = item_group_lookup[['Item_Code','Item_Group']]
# merge item_group
df = pd.merge(left=df, right=item_group_lookup, left_on='Item_Code', right_on='Item_Code', how='left').reset_index(drop=True)

# Dictionary of each element and related information
element_dict = {
    'food_energy': {'fao_ele_code': 664, 'element_code': 100,
                    'element_name': 'Food Supply Energy', 'unit': 'kcal/capita/day'},
    'food_quant': {'element_code': 110,
                   'element_name': 'Food Supply Quantity', 'unit': 'g/capita/day'},
    'protein_energy': {'element_code': 200,
                       'element_name': 'Protein Supply Energy', 'unit': 'kcal/capita/day'},
    'protein_quant': {'fao_ele_code': 674, 'element_code': 210,
                      'element_name': 'Protein Supply Quantity', 'unit': 'g/capita/day'},
    'fat_energy': {'element_code': 300,
                   'element_name': 'Fat Supply Energy', 'unit': 'kcal/capita/day'},
    'fat_quant': {'fao_ele_code': 684, 'element_code': 310,
                  'element_name': 'Fat Supply Quantity', 'unit': 'g/capita/day'},
    'carbs_energy': {'element_code': 400,
                     'element_name': 'Carbs Supply Energy', 'unit': 'kcal/capita/day'},
    'carbs_quant': {'element_code': 410,
                    'element_name': 'Carbs Supply Quantity', 'unit': 'g/capita/day'}
}

# Function that takes data input and returns data output which complies to the naming convention defined in element_dict
def Revised(input, element_name):
    selected_dict = element_dict[element_name]
    output = input
    output['Element'] = selected_dict['element_name']
    output['Element_Code'] = selected_dict['element_code']
    output['Unit'] = selected_dict['unit']
    return output

# Function that extracts the existing data provided by FAO Stats (food energy, protein quant and fat quant) and returns the clean data output complied to element_dict
# Posible element_name variables of this function include food_energy, pretoin_quant and fat_quant
def RevisedRaw(element_name):
    selected_dict = element_dict[element_name]
    fao_ele_code = selected_dict['fao_ele_code']
    input = df[df['Element_Code'] == fao_ele_code].reset_index(drop=True)
    output = Revised(input, element_name)
    return output

# Convert protein and fat energy into protein and fat quant based on defined formula
def EnergyFromQuant(element_name):
    selected_dict = element_dict[element_name]
    if element_name == 'protein_energy':
        input = RevisedRaw('protein_quant')
        input['Value'] = input['Value']*4
    elif element_name == 'fat_energy':
        input = RevisedRaw('fat_quant')
        input['Value'] = input['Value']*9
    output = input
    output['Element'] = selected_dict['element_name']
    output['Element_Code'] = selected_dict['element_code']
    output['Unit'] = selected_dict['unit']
    return output

# Return clean data frame for each element input
def ElementDataFrame(element_name):
    # convert the data from the raw fao data source
    if element_name in ('food_energy', 'protein_quant', 'fat_quant'):
        return RevisedRaw(element_name)
    elif element_name in ('protein_energy', 'fat_energy'):
        return EnergyFromQuant(element_name)

# From the raw data provided by FAO Stats and further calculations, we can create the following data frames
food_energy = ElementDataFrame('food_energy')
protein_energy = ElementDataFrame('protein_energy')
fat_energy = ElementDataFrame('fat_energy')
protein_quant = ElementDataFrame('protein_quant')
fat_quant = ElementDataFrame('fat_quant')

# Next is to compute the missing dataframes which include food_quant, carbs_energy and carbs_quant
energy = pd.concat([food_energy, protein_energy,
                   fat_energy], ignore_index=True)
energy_pivot = pd.pivot_table(energy, index=['Area_Code', 'Area', 'Item_Code', 'Item', 'Item_Group',
                              'Year', 'Unit'], columns='Element', values='Value', aggfunc=np.sum).reset_index()

# !!! Important! When substracting a number by a NULL value, the result will be NULL. Hence, we'd need to convert NULL to 0 first.
energy_pivot = energy_pivot.fillna(0)


# Calculate the missing fields based on defined formulas
energy_pivot['Carbs Supply Energy'] = energy_pivot['Food Supply Energy'] - (energy_pivot['Protein Supply Energy'] + energy_pivot['Fat Supply Energy'])
energy_pivot['Carbs Supply Quantity'] = energy_pivot['Carbs Supply Energy']/4
energy_pivot['Food Supply Quantity'] = energy_pivot['Protein Supply Energy']/4 + energy_pivot['Fat Supply Energy']/9 + energy_pivot['Carbs Supply Energy']/4

# Create carbs_energy df
carbs_energy = energy_pivot[energy_pivot.columns.difference(
    ['Fat Supply Energy', 'Food Supply Energy', 'Protein Supply Energy', 'Carbs Supply Quantity', 'Food Supply Quantity'])]
carbs_energy = Revised(carbs_energy, 'carbs_energy')
carbs_energy.rename(columns={'Carbs Supply Energy': 'Value'}, inplace=True)

# Create carbs_quant df
carbs_quant = energy_pivot[energy_pivot.columns.difference(
    ['Fat Supply Energy', 'Food Supply Energy', 'Protein Supply Energy', 'Carbs Supply Energy', 'Food Supply Quantity'])]
carbs_quant = Revised(carbs_quant, 'carbs_quant')
carbs_quant.rename(columns={'Carbs Supply Quantity': 'Value'}, inplace=True)

# Create food_quant df
food_quant = energy_pivot[energy_pivot.columns.difference(
    ['Fat Supply Energy', 'Food Supply Energy', 'Protein Supply Energy', 'Carbs Supply Energy', 'Carbs Supply Quantity'])]
food_quant = Revised(food_quant, 'food_quant')
food_quant.rename(columns={'Food Supply Quantity': 'Value'}, inplace=True)

energy = pd.concat([food_energy, protein_energy, fat_energy,
                   carbs_energy], ignore_index=True)
quantity = pd.concat(
    [food_quant, protein_quant, fat_quant, carbs_quant], ignore_index=True)

df = pd.concat([energy, quantity], ignore_index=True).reset_index(drop=True)