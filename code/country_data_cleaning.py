import pandas as pd
import os
from glob import glob
from food_data_cleaning import df as food

absolutepath = os.path.abspath(__file__)
fileDirectory = os.path.dirname(absolutepath)
parentDirectory = os.path.dirname(fileDirectory)

# read country mapping table from the directory
base_dir = os.path.join(parentDirectory, 'data_in/country_table/unstats_country_codes.csv')
country = pd.read_csv(base_dir,encoding='UTF-8')

# define the selected columns
selected_columns = ['Region Code','Region Name','Sub-region Code','Sub-region Name','M49 Code','ISO-alpha2 Code','ISO-alpha3 Code','Country or Area','Developed / Developing Countries']

# column names to be replaced
column_name = { 
    'Region Code':'Region_Code',
    'Region Name':'Region_Name',
    'Sub-region Code':'Sub_Region_Code',
    'Sub-region Name':'Sub_Region_Name',
    'M49 Code':'M49_Code',
    'ISO-alpha2 Code':'ISO_Alpha2_Code',
    'ISO-alpha3 Code':'ISO_Alpha3_Code',
    'Country or Area':'Area',
    'Developed / Developing Countries':'Developed_Developing'
}

# remove the unwanted columns and rename as defined
country = country[selected_columns]
country = country.rename(columns=column_name)

# below is the list of country names to be renamed so that they're consistent vs. unstats' naming converntion
country_unstats = {
    "C?te d'Ivoire":"Côte d’Ivoire",
    'China, Hong Kong SAR':'China Hong Kong Special Administrative Region',
    'China, Macao SAR':'China  Macao Special Administrative Region',
    'Netherlands Antilles (former)':'Netherlands',
    'Serbia and Montenegro':'Serbia',
    'Sudan (former)':'Sudan'

}

# rename the values based on above dict
food['Area'].replace(country_unstats, inplace=True)

df = pd.merge(left=food, right=country, left_on='Area', right_on='Area', how='left').reset_index()
