import inoutlists
import os
import pandas as pd
import numpy as np
import csv
from recodLinkange_OFAC_EU_utils import timer
from recodLinkange_OFAC_EU_name_normalization import normalize_name, remove_legal_forms

###############################
#         Parameters          #
###############################

FILES_PATH = os.path.dirname(os.path.abspath(__file__))
FILE_ADDRESSES_CITY_NORMALIZATION = os.path.join(FILES_PATH, "addresses_city_normalization.csv")
FILE_RESULTS = os.path.join(FILES_PATH, "record_linkage_OFAC_EU_data.parquet")
FILE_PROCESS_MEASURES = os.path.join(FILES_PATH, "record_linkage_OFAC_EU_data_preparation_process_measures.xlsx")

DATA_SETS = {
    "EU": {
        "file": os.path.join(FILES_PATH, "20251219-FULL-1_1(xsd).xml"),
        "loader": inoutlists.LoaderEUXML,
        "description": "EU Sanctions list"
    },
    "OFAC": {
        "file": os.path.join(FILES_PATH, "sdn.xml"),
        "loader": inoutlists.LoaderOFACXML,
        "description": "OFAC SDN list"
    }
}

BLOKED_TYPES_COLUMNS = {
    "I": [
        "dates_of_birth_year"
    ],
    "O": [
        "addresses_city_norm",
        "addresses_country_ISO_code"
    ]
}

RELEVANT_COLS = [
    "source",
    "id",
    "IDX",
    "type",
    "names_whole_name",
    "names_whole_name_norm",
    "dates_of_birth_year",
    "dates_of_birth_date_of_birth",
    "addresses_city",
    "addresses_city_norm",
    "addresses_country_ISO_code"
]

processMeasures = []

###############################
#    Classes and functions    #
###############################

###############################
#          Process            #
###############################

# Data Retrieval

with timer("Data Retrieval", processMeasures):

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

with timer("Data Cleaning and normalization", processMeasures):

    # Data Cleaning and normalization

    dfDataCleaned = dfDataRaw.copy()    

    ## Adresses country ISO code missing code normalization

    dfDataCleaned['addresses_country_ISO_code'] = (
        dfDataCleaned['addresses_country_ISO_code']
        .replace("00", np.nan)
    )

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

    ## Keep rows with non empty normalized names

    maskEmptyNamesNorm = (
        (dfDataCleaned.names_whole_name_norm == "") |
        (dfDataCleaned.names_whole_name_norm.isna())
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

    

#     ## Deduplication taking into account blocked columns by type

#     lsDataCleanedDeDup = []

#     for entitityType, columnsBlock in BLOKED_TYPES_COLUMNS.items():

#         dfDataCleanedDeDupType = dfDataCleaned[dfDataCleaned.type == entitityType].copy()        

#         dfDataCleanedDeDupType.sort_values(
#             by=["source", "id", "names_whole_name_norm"] + columnsBlock,
#             inplace=True
#         )

#         dfDataCleanedDeDupType.drop_duplicates(
#             subset=["source", "id", "names_whole_name_norm"] + columnsBlock,
#             keep="first",
#             inplace=True
#         )
#         dfDataCleanedDeDupType = dfDataCleanedDeDupType[RELEVANT_COLS]

#         lsDataCleanedDeDup.append(dfDataCleanedDeDupType)

#     dfDataCleanedDeDup = pd.concat(lsDataCleanedDeDup)

# # Export results

# with timer("Exportation to parquet", processMeasures):

#     dfDataCleanedDeDup.to_parquet(
#         FILE_RESULTS, 
#         index=False, 
#         engine="fastparquet",
#         compression="snappy"
#     )

# # Export process measures

# dfProcessMeasures = pd.DataFrame(processMeasures)
# dfProcessMeasures.to_excel(
#     FILE_PROCESS_MEASURES, 
#     sheet_name = "data",
#     index = False)