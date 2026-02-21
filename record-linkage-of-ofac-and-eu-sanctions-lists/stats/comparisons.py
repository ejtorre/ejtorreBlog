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
FILE_RESULTS = os.path.join(PATH_SRC, "comparisons.xlsx")

COMPARISONS = {
    "Type": {
        "groupCols":  [
            "source", 
            "type"
        ],
        "dedupCols": [
            "source", 
            "id",
            "names_whole_name_norm"
        ],
        "missingCols": [
            "type"
        ]
    }#,
    # "Type + Year of birth": {
    #     "groupCols":  [
    #         "source", 
    #         "type",
    #         "dates_of_birth_year"
    #     ],
    #     "dedupCols": [
    #         "source", 
    #         "id", 
    #         "type",
    #         "dates_of_birth_year",
    #         "names_whole_name_norm"
    #     ],
    #     "missingCols": [
    #         "type",
    #         "dates_of_birth_year"
    #     ]
    # },
    # "Type + Address country ISO code": {
    #     "groupCols":  [
    #         "source",
    #         "type", 
    #         "addresses_country_ISO_code"
    #     ], 
    #     "dedupCols": [
    #         "source", 
    #         "id", 
    #         "type", 
    #         "addresses_country_ISO_code", 
    #         "names_whole_name_norm"
    #     ],
    #     "missingCols": [
    #         "type",
    #         "addresses_country_ISO_code"
    #     ]
    # },
    # "Type + Address normalized city + country ISO code": {
    #     "groupCols": [
    #         "source",
    #         "type",
    #         "addresses_city_norm",
    #         "addresses_country_ISO_code"            
    #     ],
    #     "dedupCols": [
    #         "source",
    #         "id",
    #         "type",
    #         "addresses_city_norm",
    #         "addresses_country_ISO_code",            
    #         "names_whole_name_norm"
    #     ],
    #     "missingCols": [
    #         "type",
    #         "addresses_city_norm",
    #         "addresses_country_ISO_code"
    #     ]
    # }
}

###############################
#    Classes and functions    #
###############################

###############################
#          Process            #
###############################

# Data Retrieval

dfData = pd.read_parquet(
    FILE_DATA,
    engine= "fastparquet"
)

# Comparisons

dfData = dfData[dfData.names_strong].copy() # Keep only hight quality names

comparisonsResults = {
    "comparison": [],
    "type": [],
    "num_total_EU": [],
    "num_notna_EU": [],
    "num_na_EU": [],
    "num_total_OFAC": [],
    "num_notna_OFAC": [],
    "num_na_OFAC": [],  
    "comp_num_notna": [],
    "comp_num_na": []
}

for compName, compInfo in COMPARISONS.items():

    dfDataCompare = dfData.copy()
    dfDataCompare.sort_values(
        by=compInfo["dedupCols"],
        inplace=True
    )
    dfDataCompare.drop_duplicates(
        subset=compInfo["dedupCols"],
        keep="first",
        inplace=True
    )
#     dfComp = (
#         dfDataCompare.groupby(
#             compInfo["groupCols"],
#             dropna=False
#         )
#         .size()
#         .unstack('source', fill_value=0)
#     ) 
#     dfComp.reset_index(inplace = True)

#     for compType in dfComp.type.unique():

#         dfCompType = dfComp[dfComp.type == compType].copy()
    
#         compMask = dfCompType[compInfo["missingCols"]].isna().any(axis=1)

#         num_total_EU = dfCompType["EU"].sum()
#         num_na_EU = dfCompType.loc[compMask, "EU"].sum()
#         num_notna_EU = num_total_EU - num_na_EU    

#         num_total_OFAC = dfCompType["OFAC"].sum()    
#         num_na_OFAC = dfCompType.loc[compMask, "OFAC"].sum() 
#         num_notna_OFAC = num_total_OFAC - num_na_OFAC

#         numComparisonsNoNa = (
#             dfCompType.loc[~compMask, "EU"]
#             * dfCompType.loc[~compMask, "OFAC"]
#         ).sum()

#         comparisonsResults["comparison"].append(compName)
#         comparisonsResults["type"].append(compType)
#         comparisonsResults["num_total_EU"].append(num_total_EU)
#         comparisonsResults["num_notna_EU"].append(num_notna_EU)
#         comparisonsResults["num_na_EU"].append(num_na_EU)
#         comparisonsResults["num_total_OFAC"].append(num_total_OFAC)
#         comparisonsResults["num_notna_OFAC"].append(num_notna_OFAC)
#         comparisonsResults["num_na_OFAC"].append(num_na_OFAC)
#         comparisonsResults["comp_num_notna"].append(numComparisonsNoNa)
#         comparisonsResults["comp_num_na"].append(
#             num_na_EU * num_total_OFAC +
#             num_na_OFAC * num_total_EU -
#             num_na_EU * num_na_OFAC
#         )

# dfComparisons = pd.DataFrame(comparisonsResults)

# Export results

# dfComparisons.to_excel(
#     FILE_RESULTS, 
#     sheet_name = "data",
#     index = False
# )