import inoutlists
import os
import pandas as pd

###############################
#         Parameters          #
###############################

FILE_PATH = os.path.dirname(os.path.abspath(__file__))

DATA_SETS = {
    "EU": {
        "file": os.path.join(FILE_PATH, "20251219-FULL-1_1(xsd).xml"),
        "loader": inoutlists.LoaderEUXML,
        "description": "EU Sanctions list"
    },
    "OFAC": {
        "file": os.path.join(FILE_PATH, "sdn.xml"),
        "loader": inoutlists.LoaderOFACXML,
        "description": "OFAC SDN list"
    }
}

MISSING_COLUMNS_INFO = {
    "names_whole_name": {
        "dedup": ["id", "type", "names_whole_name"],
        "col": "names_whole_name",
        "replacing_missing": False
    },
    "addresses_country_ISO_code": {
        "dedup": ["id", "type", "addresses_country_ISO_code"],        
        "col": "addresses_country_ISO_code",
        "replacing_missing": True,
        "missing_value": "00"
    },
    "nationalities_country_ISO_code": {
        "dedup": ["id", "type", "nationalities_country_ISO_code"],        
        "col": "nationalities_country_ISO_code",
        "replacing_missing": True,
        "missing_value": "00"
    },
    "dates_of_birth_year": {
        "dedup": ["id", "type", "dates_of_birth_year"],        
        "col": "dates_of_birth_year",
        "replacing_missing": False
    },
    "places_of_birth_country_ISO_code": {
        "dedup": ["id", "type", "places_of_birth_country_ISO_code"],        
        "col": "places_of_birth_country_ISO_code",
        "replacing_missing": True,
        "missing_value": "00"
    },
    "identifications_id": {
        "dedup": ["id", "type", "identifications_id"],        
        "col": "identifications_id",
        "replacing_missing": False
    }
}

###############################
#    Classes and functions    #
###############################

###############################
#          Process            #
###############################

# Data Retrieval

data = {}
dfData = {}
dfDataCleaned = {}

for dataSetName, dataSetInfo in DATA_SETS.items():
    
    data[dataSetName] = inoutlists.load(
        dataSetInfo["file"],
        dataSetInfo["loader"],
        description=dataSetInfo["description"]
    )
    dfData[dataSetName] = inoutlists.dump(
        data[dataSetName],
        inoutlists.DumperPandas        
    )

# Data Normalization

for dataSetName in DATA_SETS.keys():

    dfDataCleaned[dataSetName] = dfData[dataSetName].copy()

    # Keep only strong names

    dfDataCleaned[dataSetName] = dfDataCleaned[dataSetName][dfDataCleaned[dataSetName].names_strong]    

# General Stats

lsGeneralStats = []

for dataSetName in DATA_SETS.keys():

    dfGeneralStatsDataSet = dfDataCleaned[dataSetName].copy()    

    dfGeneralStatsDataSet = dfGeneralStatsDataSet.drop_duplicates(
        subset=["id"],
        keep="first"
    )

    dfGeneralStatsDataSet["data_set"] = dataSetName

    dfGeneralStatsDataSet = dfGeneralStatsDataSet.groupby(
        ["data_set", "type"],
        as_index=False
    ).agg(
        num_records=("id", "size")       
    )

    lsGeneralStats.append(dfGeneralStatsDataSet)

dfGeneralStats = pd.concat(lsGeneralStats, ignore_index=True)
dfGeneralStats.sort_values(["data_set", "type"], inplace=True)

# Different names by type

lsDifferentNames = []

for dataSetName in DATA_SETS.keys():

    dfDifferentNamesDataSet = dfDataCleaned[dataSetName].copy()

    dfDifferentNamesDataSet = dfDifferentNamesDataSet.drop_duplicates(
        subset=["id", "names_whole_name"],
        keep="first"
    )

    dfDifferentNamesDataSet["data_set"] = dataSetName

    dfDifferentNamesDataSet = dfDifferentNamesDataSet.groupby(
        ["data_set", "type"],
        as_index=False
    ).agg(
        num_different_names=("names_whole_name", "size")
    )

    lsDifferentNames.append(dfDifferentNamesDataSet)

dfDifferentNames = pd.concat(lsDifferentNames, ignore_index=True)
dfDifferentNames.sort_values(["data_set", "type"], inplace=True)

# Different normalized names by type



# Different addresses country ISO code by type

lsDifferentAddresses = []

for dataSetName in DATA_SETS.keys():

    dfDifferentAddressesDataSet = dfDataCleaned[dataSetName].copy()

    dfDifferentAddressesDataSet = dfDifferentAddressesDataSet.drop_duplicates(
        subset=["id", "addresses_country_ISO_code"],
        keep="first"
    )

    dfDifferentAddressesDataSet["data_set"] = dataSetName

    dfDifferentAddressesDataSet = dfDifferentAddressesDataSet.groupby(
        ["data_set", "type", "addresses_country_ISO_code"],
        as_index=False
    ).agg(
        num_different_addresses=("id", "size")
    )

    lsDifferentAddresses.append(dfDifferentAddressesDataSet)

dfDifferentAddresses = pd.concat(lsDifferentAddresses, ignore_index=True)
dfDifferentAddresses.sort_values(["data_set", "type", "addresses_country_ISO_code"], inplace=True)

# Different addresses cities

lsDifferentAddressesCities = []

for dataSetName in DATA_SETS.keys():

    dfDifferentAddressesCitiesDataSet = dfDataCleaned[dataSetName].copy()

    dfDifferentAddressesCitiesDataSet = dfDifferentAddressesCitiesDataSet.drop_duplicates(
        subset=["id", "addresses_city", "addresses_country_ISO_code"],
        keep="first"
    )

    dfDifferentAddressesCitiesDataSet["data_set"] = dataSetName

    dfDifferentAddressesCitiesDataSet = dfDifferentAddressesCitiesDataSet.groupby(
        [
            "data_set", 
            "addresses_city", 
            "addresses_country_ISO_code"
        ],
        as_index=False
    ).agg(
        num_different_address_cities=("id", "size")
    )

    lsDifferentAddressesCities.append(dfDifferentAddressesCitiesDataSet)

dfDifferentAddressesCities = pd.concat(lsDifferentAddressesCities, ignore_index=True)
dfDifferentAddressesCities.sort_values(
    [
        "data_set", 
        "addresses_country_ISO_code", 
        "addresses_city"
    ], 
    inplace=True
)

# Missing analysis

lsMissingColumns = []

for missing_column, missing_column_info in MISSING_COLUMNS_INFO.items():    

    for dataSetName in DATA_SETS.keys():

        dfMissingColumnDataSet = dfData[dataSetName].copy()

        if missing_column_info["replacing_missing"]:
            dfMissingColumnDataSet.loc[
                dfMissingColumnDataSet[missing_column_info["col"]] == missing_column_info["missing_value"],
                missing_column_info["col"]
            ] = pd.NA

        dfMissingColumnDataSet = dfMissingColumnDataSet.sort_values(
            by=missing_column_info["dedup"]
        )

        dfMissingColumnDataSet = dfMissingColumnDataSet.drop_duplicates(
            subset=missing_column_info["dedup"],
            keep="first"
        )

        dfMissingColumnDataSet["data_set"] = dataSetName
        dfMissingColumnDataSet["column_name"] = missing_column

        dfMissingColumnDataSet = dfMissingColumnDataSet.groupby(
            ["column_name", "data_set", "type"],
            as_index=False
        )[[missing_column_info["col"]]].agg(
            num_records=(missing_column_info["col"], "size"),
            num_missing=(missing_column_info["col"], lambda x: x.isna().sum()),
            pct_missing=(missing_column_info["col"], lambda x: x.isna().mean() * 100)
        )

        lsMissingColumns.append(dfMissingColumnDataSet)

dfMissingColumnsStats = pd.concat(lsMissingColumns, ignore_index=True)
dfMissingColumnsStats.sort_values(["column_name", "type"], inplace=True)

# Count words in names_whole_name

lsCountNameWords = []

for dataSetName in DATA_SETS.keys():

    dfCountNameWordsDs = dfData[dataSetName][[
        "id", 
        "type",
        "names_whole_name"
    ]].copy()

    dfCountNameWordsDs.sort_values(
        by=["id", "names_whole_name"],
        inplace=True
    )

    dfCountNameWordsDs.drop_duplicates(
        subset=["id", "names_whole_name"],  
        keep="first",
        inplace=True
    )

    dfCountNameWordsDs["data_set"] = dataSetName
    dfCountNameWordsDs["words"] = dfCountNameWordsDs["names_whole_name"].str.split()
    dfCountNameWordsDs = dfCountNameWordsDs.explode("words")   

    lsCountNameWords.append(dfCountNameWordsDs)

dfCountNameWords = pd.concat(lsCountNameWords, ignore_index=True)
dfCountNameWords = (
    dfCountNameWords
    .groupby(["type", "words"], as_index=False)
    .agg(num = ('words', "count"))    
    .sort_values(["type", "num"], ascending=[True, False])
)

# Export results

with pd.ExcelWriter(os.path.join(FILE_PATH, "recodLinkange_OFAC_EU_stats.xlsx")) as writerExcel:
    dfGeneralStats.to_excel(writerExcel, index=False, sheet_name="GeneralStats")
    dfDifferentNames.to_excel(writerExcel, index=False, sheet_name="DifferentNamesStats")
    dfDifferentAddressesCities.to_excel(writerExcel, index=False, sheet_name="DifferentAddressesCitiesStats")
    dfDifferentAddresses.to_excel(writerExcel, index=False, sheet_name="DifferentAddressesStats")
    dfMissingColumnsStats.to_excel(writerExcel, index=False, sheet_name="MissingColumnsStats")
    dfCountNameWords.to_excel(writerExcel, index=False, sheet_name="CountNameWords")