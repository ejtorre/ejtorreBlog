import inoutlists
import os
import pandas as pd
import numpy as np
import recordlinkage
from recodLinkange_OFAC_EU_utils import timer

###############################
#         Parameters          #
###############################

FILES_PATH = os.path.dirname(os.path.abspath(__file__))
FILE_OS = os.path.join(FILES_PATH, "open_sanctions_eu_ofac_id_mapping.parquet")
FILE_DATA = os.path.join(FILES_PATH, "record_linkage_OFAC_EU_data.parquet")
FILE_RESULTS = os.path.join(FILES_PATH, "record_linkage_OFAC_EU_confusion_matrix.xlsx")
FILE_PROCESS_MEASURES = os.path.join(FILES_PATH, "record_linkage_OFAC_EU_confusion_matrix_process_measures.xlsx")

BLOKED_TYPES_COLUMNS = {
    "I": [
        "dates_of_birth_year"
    ],
    "O": [
        "addresses_city_norm",
        "addresses_country_ISO_code"
    ]
}

thresholds = [
    0.70,
    0.75,
    0.80,
    0.85,
    0.90,
    0.95,
    1.00
]

processMeasures = []

###############################
#    Classes and functions    #
###############################

def confusion_matrix(
        dfAssestment,
        dfTot,
        entityType,
        threshold        
    ):

    df = dfAssestment[dfAssestment.type == entityType].copy()    

    TP = (
        (df["distance_max"] >= threshold) &
        (df["real"] == True) &
        (df["in_block"] == True)
    ).sum()
    FP = (
        (df["distance_max"] >= threshold) &
        (df["real"] == False) &
        (df["in_block"] == True)
    ).sum()
    FN_THRESHOLD = (
        (df["distance_max"] < threshold) &
        (df["real"] == True) &
        (df["in_block"] == True)
    ).sum()
    FN_BLOCK = (        
        (df["real"] == True) &
        (df["in_block"] == False)
    ).sum()
    FN_TOT = FN_THRESHOLD + FN_BLOCK
    totEU = (
        (dfTot.source == "EU") &
        (dfTot.type == entityType)
    ).sum()
    totOFAC = (
        (dfTot.source == "OFAC") &
        (dfTot.type == entityType)
    ).sum()    
    TN = (totEU * totOFAC) - TP - FP - FN_TOT

    precision = TP / (TP + FP) if (TP + FP) > 0 else 0
    recall = TP / (TP + FN_TOT) if (TP + FN_TOT) > 0 else 0
    accuracy = (TP + TN) / (TP + FP + FN_TOT + TN) if (TP + FP + FN_TOT + TN) > 0 else 0
    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall) > 0
        else 0
    )

    return {
        "type": entityType,
        "threshold": threshold,
        "TP": TP,
        "FP": FP,
        "FN_THRESHOLD": FN_THRESHOLD,
        "FN_BLOCK": FN_BLOCK,
        "FN_TOT": FN_TOT,        
        "TN": TN,
        "precision": precision,
        "recall": recall,
        "accuracy": accuracy,
        "f1": f1
    }

###############################
#          Process            #
###############################

# Data Retrieval

with timer("Data retrieval", processMeasures):

    dfOS = pd.read_parquet(
        FILE_OS,
        engine = "fastparquet"
    )

    dfData = pd.read_parquet(
        FILE_DATA,
        engine= "fastparquet"
    )

    dfDataIds = dfData.copy()

    dfDataIds.sort_values(
        by = ["source", "id", "IDX"],
        inplace = True
    )

    dfDataIds.drop_duplicates(
        subset = ["source", "id"],
        keep="first",
        inplace = True
    )

# Record Linkage

lsCompare = []

with timer("Record linkange", processMeasures):

    for entityType, columnsBlock in BLOKED_TYPES_COLUMNS.items():
        
        dfDataCompareType = dfData[dfData.type == entityType].copy()
        dfDataCompareType.set_index("IDX", inplace=True)
        
        indexer = recordlinkage.Index()
        indexer.add(
            [
                recordlinkage.index.Block(
                    left_on=columnsBlock,
                    right_on=columnsBlock
                )
            ]
        )

        candidateLinksType = indexer.index(
            dfDataCompareType[dfDataCompareType.source == "EU"],
            dfDataCompareType[dfDataCompareType.source == "OFAC"]
        )    

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
        dfCompareType = compare.compute(
            candidateLinksType, 
            dfDataCompareType[dfDataCompareType.source == "EU"], 
            dfDataCompareType[dfDataCompareType.source == "OFAC"]
        )

        dfCompareType.reset_index(inplace=True)
        dfCompareType.rename(
            columns = {
                0: "distance_jw",
                1: "distance_cosine"
            },
            inplace=True
        )    
        dfCompareType["distance_max"] = (
            dfCompareType[["distance_jw", "distance_cosine"]].max(axis=1)
        )
        lsCompare.append(dfCompareType)      
    
    dfCompare = pd.concat(lsCompare, ignore_index=True)

with timer("Original IDs extraction", processMeasures):

    dfCompare["id_EU"] = dfCompare["IDX_1"].str.split("-", expand = True)[0]
    dfCompare["id_OFAC"] = dfCompare["IDX_2"].str.split("-", expand = True)[0]
    dfCompare.rename(
        columns = {
            "IDX_1": "IDX_EU",
            "IDX_2": "IDX_OFAC"
        },
        inplace = True
    )

# Real match retrieval

with timer("Real match retrieval", processMeasures):

    ## Remove OS eu and ofac ids not in dataset

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

    ## Retrieve real matches

    dfAssestment = dfCompare.copy()

    dfAssestment.sort_values(
        by = [
            "id_EU",
            "id_OFAC",        
            "distance_max"
        ],
        ascending = [True, True, False],
        inplace = True
    )

    dfAssestment.drop_duplicates(
        subset=["id_EU", "id_OFAC"],
        keep="first",
        inplace=True
    )

    dfAssestment = dfAssestment.merge(
        dfOSInData[dfOSInData.source == "EU & OFAC"][
            [         
                "id_EU",           
                "id_OFAC",
            ]
        ],
        on = ["id_EU", "id_OFAC"],
        how ="outer",
        indicator = True
    )

    dfAssestment["real"] = np.where(
        dfAssestment._merge.isin(["both", "right_only"]),
        True,
        False
    )

    dfAssestment["in_block"] = np.where(
        dfAssestment._merge.isin(["both", "left_only"]),
        True,
        False
    )

    dfAssestment = dfAssestment.drop(
        columns = ["_merge"]
    )

    ## Retrieve type for all records

    dfAssestment = dfAssestment.merge(
        dfDataIds[dfDataIds.source == "EU"][
            [
                "id",
                "type"
            ]
        ],
        left_on = "id_EU",
        right_on = "id",
        how = "left"
    )

    dfAssestment = dfAssestment.drop(
        columns = ["id"]
    )

# Confusion matrix

with timer("Get confusion matrix", processMeasures):

    lsCm = []

    for entityType in BLOKED_TYPES_COLUMNS.keys():
        for threshold in thresholds:
            cm = confusion_matrix(
                dfAssestment,
                dfDataIds,
                entityType,
                threshold
            )            
            lsCm.append(cm)

    dfCm = pd.DataFrame(lsCm)

# Export results

dfCm.to_excel(
    FILE_RESULTS, 
    sheet_name = "data",
    index = False
)

dfProcessMeasures = pd.DataFrame(processMeasures)
dfProcessMeasures.to_excel(
    FILE_PROCESS_MEASURES, 
    sheet_name = "data",
    index = False
)