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
import numpy as np
import recordlinkage

###############################
#         Parameters          #
###############################

FILE_OS = os.path.join(PATH_DATA, "open_sanctions.parquet")
FILE_DATA = os.path.join(PATH_DATA, "data_EU_OFAC.parquet")
FILE_RESULTS = os.path.join(PATH_SRC, "fuzzy_logic_distance_real_pairs_percentiles.xlsx")

###############################
#    Classes and functions    #
###############################

###############################
#          Process            #
###############################

# Data Retrieval

dfOS = pd.read_parquet(
    FILE_OS,
    engine = "fastparquet"
)

dfData = pd.read_parquet(
    FILE_DATA,
    engine= "fastparquet"
)

# Data Preparation

## Keep only different names

dfDataNames = dfData[
    [
        "source",
        "id",
        "type",
        "IDX",
        "names_whole_name",
        "names_whole_name_norm"
    ]
].copy()

dfDataNames.sort_values(
    by = [
        "source",
        "id",
        "names_whole_name_norm",
        "IDX"
    ],
    inplace = True
)

dfDataNames.drop_duplicates(
    subset=[
        "source",
        "id",
        "names_whole_name_norm"
    ],
    keep="first",
    inplace=True
)

## Keep only OS ids in data

dfOSInData = dfOS.copy()

maskOsInDataEU = (
    (~dfOSInData.id_ori_eu.isin(dfData[dfData.source == "EU"].id.unique())) &
    (dfOSInData.source.isin(["EU", "EU & OFAC"]))
)
dfOSInData = dfOSInData[~maskOsInDataEU].copy()

maskOsInDataOFAC = (
    (~dfOSInData.id_ori_ofac.isin(dfData[dfData.source == "OFAC"].id.unique())) &
    (dfOSInData.source.isin(["OFAC", "EU & OFAC"]))
)
dfOSInData = dfOSInData[~maskOsInDataOFAC].copy()

dfOSInData.rename(
    columns={
        "id_ori_eu": "id_EU",
        "id_ori_ofac": "id_OFAC"
    },
    inplace=True
)

# Get real pairs

dfOSReal = dfOSInData[
    dfOSInData.source == "EU & OFAC"
][
    [
        "id_EU",
        "id_OFAC"
    ]
].copy()

dfOSReal = dfOSReal.merge(
    dfDataNames[
        dfDataNames.source == "EU"
    ],
    left_on="id_EU",
    right_on="id",
    how = "left"
)

dfOSReal = dfOSReal.drop(
    columns = ["source", "id"]
)

dfOSReal.rename(
    columns = {
        "type": "type_EU",
        "IDX": "IDX_EU",
        "names_whole_name": "names_whole_name_EU",
        "names_whole_name_norm": "names_whole_name_norm_EU"
    },
    inplace=True
)

dfOSReal = dfOSReal.merge(
    dfDataNames[
        dfDataNames.source == "OFAC"
    ],
    left_on="id_OFAC",
    right_on="id",
    how = "left"
)

dfOSReal = dfOSReal.drop(
    columns = ["source", "id"]
)

dfOSReal.rename(
    columns = {
        "type": "type_OFAC",
        "IDX": "IDX_OFAC",
        "names_whole_name": "names_whole_name_OFAC",
        "names_whole_name_norm": "names_whole_name_norm_OFAC"
    },
    inplace=True
)

# Get real pairs distances

candidatePairs = pd.MultiIndex.from_frame(dfOSReal[
    [
        'IDX_EU', 
        'IDX_OFAC'
    ]
])

dfDataNamesComp = dfDataNames.copy()
dfDataNamesComp.set_index("IDX", inplace=True)

compare = recordlinkage.Compare()
compare.string(
    'names_whole_name_norm', 
    'names_whole_name_norm', 
    method='jarowinkler'
)
compare.string(
    'names_whole_name_norm', 
    'names_whole_name_norm', 
    method='cosine'
)
dfCompare = compare.compute(
    candidatePairs, 
    dfDataNamesComp[dfDataNamesComp.source == "EU"], 
    dfDataNamesComp[dfDataNamesComp.source == "OFAC"]
)

dfCompare.reset_index(inplace=True)
dfCompare.rename(
    columns = {
        0: "distance_jw",
        1: "distance_cosine"
    },
    inplace=True
)

dfOSReal = dfOSReal.merge(
    dfCompare,
    on = ["IDX_EU", "IDX_OFAC"],
    how = "left"
)

dfOSReal["distance_max"] = (
    dfOSReal[["distance_jw", "distance_cosine"]].max(axis=1)
)

dfOSRealIds = dfOSReal.copy()

dfOSRealIds.sort_values(
    by = [
        "id_EU",
        "id_OFAC",
        "distance_max",        
        "IDX_EU",
        "IDX_OFAC"
    ],
    ascending=[
        True,
        True,
        False,
        True,
        True
    ],
    inplace = True
)

dfOSRealIds.drop_duplicates(
    subset=[
        "id_EU",
        "id_OFAC"
    ],
    keep="first",
    inplace=True
)

# Get distance percentiles

numReal = len(dfOSRealIds)

dfSummary = (
    dfOSRealIds['distance_max']
    .quantile([i/100 for i in range(101)])
    .rename('distance')
    .reset_index()
    .rename(columns={'index': 'percentile'})
)

dfSummary['num_real_pairs_acu'] = (dfSummary['percentile'] * numReal).round().astype(int)

# Export results

dfSummary.to_excel(
    FILE_RESULTS, 
    sheet_name = "data",
    index = False
)