###############################
#            Paths            #
###############################

import os
import sys

PATH_SRC = os.path.dirname(os.path.abspath(__file__))
PATH_PROJECT = os.path.normpath(os.path.join(PATH_SRC, ".."))
PATH_DATA = os.path.join(PATH_PROJECT, "data")
PATH_SUPPORT = os.path.join(PATH_PROJECT, "support")

if PATH_PROJECT not in sys.path:
    sys.path.append(PATH_PROJECT)

###############################
#          Imports            #
###############################

import pandas as pd

###############################
#         Parameters          #
###############################

FILE_DATA = os.path.join(PATH_DATA, "data_EU_OFAC.parquet")
FILE_RESULTS = os.path.join(PATH_SRC, "stats.xlsx")

MISSING_COLUMNS_INFO = {
    "names_whole_name": {
        "dedup": ["id", "type", "names_whole_name"],
        "col": "names_whole_name" 
    },
    "names_whole_name_norm": {
        "dedup": ["id", "type", "names_whole_name_norm"],
        "col": "names_whole_name_norm"        
    },
    "addresses_country_ISO_code": {
        "dedup": ["id", "type", "addresses_country_ISO_code"],        
        "col": "addresses_country_ISO_code"        
    },
    "addresses_city_norm": {
        "dedup": ["id", "type", "addresses_city_norm"],        
        "col": "addresses_city_norm"        
    },
    "nationalities_country_ISO_code": {
        "dedup": ["id", "type", "nationalities_country_ISO_code"],        
        "col": "nationalities_country_ISO_code"
    },
    "dates_of_birth_year": {
        "dedup": ["id", "type", "dates_of_birth_year"],
        "col": "dates_of_birth_year"
    },
    "places_of_birth_country_ISO_code": {
        "dedup": ["id", "type", "places_of_birth_country_ISO_code"],        
        "col": "places_of_birth_country_ISO_code"        
    },
    "identifications_id": {
        "dedup": ["id", "type", "identifications_id"],        
        "col": "identifications_id"        
    }
}

###############################
#    Classes and functions    #
###############################

###############################
#          Process            #
###############################

# Data Retrieval

dfData = pd.read_parquet(FILE_DATA)

# Different ids

dfGeneralStatsDataSet = dfData.drop_duplicates(
    subset=["source", "id"],
    keep="first"
)

dfGeneralStats = dfGeneralStatsDataSet.groupby(
    ["source", "type"]  
).size().reset_index(name = "num_ids")

# Different names by id

dfDifferentNamesDataSet = dfData.sort_values(
    by=["source", "id", "names_whole_name"]    
)

dfDifferentNamesDataSet.drop_duplicates(
    subset=["source", "id", "names_whole_name"],
    keep="first",
    inplace = True
)

dfDifferentNames = dfDifferentNamesDataSet.groupby(
    ["source", "type", "names_strong"]    
).size().reset_index(name = "num_names_by_id")

# Different normalized names by id

dfDifferentNormNamesDataSet = dfData.sort_values(
    by=["source", "id", "names_whole_name_norm"]    
)

dfDifferentNormNamesDataSet.drop_duplicates(
    subset=["source", "id", "names_whole_name_norm"],
    keep="first",
    inplace=True
)

dfDifferentNormNames = dfDifferentNormNamesDataSet.groupby(
    ["source", "type", "names_strong"]    
).size().reset_index(name = "num_normalized_names_by_id")

# Different addresses country ISO code

dfDifferentCountryISODataSet = dfData.sort_values(
    by=["source", "id", "addresses_country_ISO_code"]    
)

dfDifferentCountryISODataSet.drop_duplicates(
    subset=["source", "id", "addresses_country_ISO_code"],
    keep="first",
    inplace = True
)

dfDifferentCountryISO = dfDifferentCountryISODataSet.groupby(
    ["source", "addresses_country_ISO_code"],  
    dropna=False
).size().reset_index(name = "num_country_ISO_code_by_id")

# Different addresses cities by address country ISO code

dfDifferentCitiesDataSet = dfData.sort_values(
    by=[
        "source", 
        "id", 
        "addresses_city", 
        "addresses_country_ISO_code"
    ]    
)

dfDifferentCitiesDataSet.drop_duplicates(
    subset=[
        "source", 
        "id", 
        "addresses_city", 
        "addresses_country_ISO_code"
    ],
    keep="first",
    inplace = True
)

dfDifferentCities = dfDifferentCitiesDataSet.groupby(
    by = [
       "source", 
       "addresses_city", 
       "addresses_country_ISO_code"
    ],    
    dropna=False
).size().reset_index(name = "num_cities_by")

# Missing columns analysis

lsMissingColumns = []

for missing_column, missing_column_info in MISSING_COLUMNS_INFO.items():

    dfMissingColumnDataSet = dfDifferentNamesDataSet.sort_values(
        by=missing_column_info["dedup"]
    )

    dfMissingColumnDataSet = dfMissingColumnDataSet.drop_duplicates(
        subset=missing_column_info["dedup"],
        keep="first"
    )
        
    dfMissingColumnDataSet["column_name"] = missing_column

    dfMissingColumn = dfMissingColumnDataSet.groupby(
        ["column_name", "source", "type"],
            as_index=False
    )[[missing_column_info["col"]]].agg(
        num_records=(missing_column_info["col"], "size"),
        num_missing=(missing_column_info["col"], lambda x: x.isna().sum()),
        pct_missing=(missing_column_info["col"], lambda x: x.isna().mean() * 100)
    )

    lsMissingColumns.append(dfMissingColumn)   

dfMissingColumns = pd.concat(lsMissingColumns, ignore_index=True)
dfMissingColumns.sort_values(["column_name", "type"], inplace=True)

# Export results

with pd.ExcelWriter(FILE_RESULTS) as writerExcel:
    dfGeneralStats.to_excel(writerExcel, index=False, sheet_name="General")
    dfDifferentNames.to_excel(writerExcel, index=False, sheet_name="Different Names")
    dfDifferentNormNames.to_excel(writerExcel, index=False, sheet_name="Different Normalized Names")
    dfDifferentCountryISO.to_excel(writerExcel, index=False, sheet_name="Different Address Countries")
    dfDifferentCities.to_excel(writerExcel, index=False, sheet_name="Different Addresses Cities")    
    dfMissingColumns.to_excel(writerExcel, index=False, sheet_name="Missing Columns")  