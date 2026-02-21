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

import inoutlists
import pandas as pd
import numpy as np
import csv
from support.name_normalization import normalize_name, remove_legal_forms, normalize_name_basic

###############################
#         Parameters          #
###############################

FILE_ADDRESSES_CITY_NORMALIZATION = os.path.join(PATH_SUPPORT, "addresses_city_normalization.csv")
FILE_RESULTS = os.path.join(PATH_DATA, "data_EU_OFAC.parquet")

DATA_SETS = {
    "EU": {
        "file": os.path.join(PATH_DATA, "20251219-FULL-1_1(xsd).xml"),
        "loader": inoutlists.LoaderEUXML,
        "description": "EU Sanctions list"
    },
    "OFAC": {
        "file": os.path.join(PATH_DATA, "sdn.xml"),
        "loader": inoutlists.LoaderOFACXML,
        "description": "OFAC SDN list"
    }
}

###############################
#    Classes and functions    #
###############################

###############################
#          Process            #
###############################

# Data Retrieval

dataRaw = {}
lsDataRaw = []
lsDataCleaned = []

for dataSetName, dataSetInfo in DATA_SETS.items():
    
    dataRaw[dataSetName] = inoutlists.load(
        dataSetInfo["file"],
        dataSetInfo["loader"],
        description=dataSetInfo["description"]
    )
    dfDataSet = inoutlists.dump(
        dataRaw[dataSetName],
        inoutlists.DumperPandas        
    )
    dfDataSet["source"] = dataSetName
    lsDataRaw.append(dfDataSet)

dfDataRaw = pd.concat(lsDataRaw, ignore_index=True)
dfDataRaw["IDX"] = dfDataRaw["id"] + "-" + dfDataRaw.index.astype(str)

dfAddressesCityNormalization = pd.read_csv(
    FILE_ADDRESSES_CITY_NORMALIZATION,
    dtype=str,
    sep=",",
    keep_default_na=False,
    quoting=csv.QUOTE_ALL,
    quotechar='"',
    encoding='utf-8'
)

# Data Cleaning and normalization

dfDataCleaned = dfDataRaw.copy()    

## Adresses country ISO code missing code normalization

dfDataCleaned['addresses_country_ISO_code'] = (
    dfDataCleaned['addresses_country_ISO_code']
    .replace("00", np.nan)
)

maskEmptyCountryISO = (
    (dfDataCleaned.addresses_country_ISO_code == "") |
    (dfDataCleaned.addresses_country_ISO_code.isna())
)

dfDataCleaned.loc[maskEmptyCountryISO, "addresses_country_ISO_code"] = np.nan

## Names normalization: transliteration to latin script + basic normalization

dfDataCleaned['names_whole_name_norm'] = dfDataCleaned['names_whole_name'].apply(
    lambda x: normalize_name(x)
)

## Remove organizational legal forms 

dfDataCleaned.loc[dfDataCleaned["type"] == "O", "names_whole_name_norm"] = (
    dfDataCleaned
    .loc[dfDataCleaned["type"] == "O", "names_whole_name_norm"]
    .apply(remove_legal_forms)
)

## Name normalization basic

dfDataCleaned['names_whole_name_norm_basic'] = dfDataCleaned['names_whole_name'].apply(
    lambda x: normalize_name_basic(x)
)

## Keep rows with non empty normalized names

maskEmptyNamesNorm = (
    (dfDataCleaned.names_whole_name_norm == "") |
    (dfDataCleaned.names_whole_name_norm.isna() |
    (dfDataCleaned.names_whole_name_norm_basic == "") |
    (dfDataCleaned.names_whole_name_norm_basic.isna())
    )
)

dfDataCleaned = dfDataCleaned[~maskEmptyNamesNorm]

## Addresses city normalization

dfDataCleaned = dfDataCleaned.merge(
    dfAddressesCityNormalization,
    on=[
        "addresses_city",
        "addresses_country_ISO_code"
    ],
    how="left"
)

dfDataCleaned["addresses_city_norm_ds"] = dfDataCleaned["addresses_city"].mask(
    dfDataCleaned["addresses_city_norm"].notna(),
    dfDataCleaned["addresses_city_norm"]
)

dfDataCleaned = dfDataCleaned.drop(columns=["addresses_city_norm"])
dfDataCleaned.rename(
    columns={
        "addresses_city_norm_ds": "addresses_city_norm"
    },
    inplace=True
)

maskEmptyCityNorm = (
    (dfDataCleaned.addresses_city_norm == "") |
    (dfDataCleaned.addresses_city_norm.isna())
)

dfDataCleaned.loc[maskEmptyCityNorm, "addresses_city_norm"] = np.nan

## Deduplication after normalization

dfDataCleaned.drop_duplicates(
    inplace = True
)

# Export results

dfDataCleaned.to_parquet(
    FILE_RESULTS, 
    index=False, 
    engine="fastparquet",
    compression="snappy"
)