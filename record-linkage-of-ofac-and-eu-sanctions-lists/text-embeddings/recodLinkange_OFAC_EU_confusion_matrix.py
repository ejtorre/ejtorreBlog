import os
import pandas as pd
import numpy as np
import faiss
from recodLinkange_OFAC_EU_utils import timer

###############################
#         Parameters          #
###############################

FILES_PATH = os.path.dirname(os.path.abspath(__file__))
FILE_OS = os.path.join(FILES_PATH, "open_sanctions_eu_ofac_id_mapping.parquet")
FILE_EMB = os.path.join(FILES_PATH, "record_linkage_OFAC_EU_embeddings.parquet")
FILE_RESULTS = os.path.join(FILES_PATH, "record_linkage_OFAC_EU_confusion_matrix.xlsx")
FILE_PROCESS_MEASURES = os.path.join(FILES_PATH, "record_linkage_OFAC_EU_confusion_matrix_process_measures.xlsx")

NEIGHBORS = {
    "I": 6,
    "O": 6
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

def getSimilaritiesByEntityType(dfEmb, entityType, k):    

    dfEmbEU = dfEmb[
        (dfEmb["source"] == "EU") & 
        (dfEmb["type"] == entityType)
    ]

    dfEmbOFAC = dfEmb[
        (dfEmb["source"] == "OFAC") & 
        (dfEmb["type"] == entityType)
    ]

    emb_cols = dfEmbEU.select_dtypes(include=["float32"]).columns.tolist()
    embEUArray = dfEmbEU[emb_cols].to_numpy()
    embOFACArray = dfEmbOFAC[emb_cols].to_numpy()

    # FAISS HNSWF Index    

    dim = embEUArray.shape[1]
    index = faiss.IndexHNSWFlat(dim, 32, faiss.METRIC_INNER_PRODUCT)
    index.hnsw.efConstruction = 200
    index.hnsw.efSearch = 128
    index.add(embEUArray)

    # Search    

    distances, indexes = index.search(embOFACArray, k)
    sims = distances

    similarities = []

    for i_ofac, neighbors in enumerate(indexes):
        for pos, i_ue in enumerate(neighbors):
            sim = sims[i_ofac][pos]            
            similarities.append({
                "id_EU": dfEmbEU.iloc[i_ue]["id"],
                "IDX_EU": dfEmbEU.iloc[i_ue]["IDX"],
                "name_EU": dfEmbEU.iloc[i_ue]["names_whole_name"],
                "name_EU_norm": dfEmbEU.iloc[i_ue]["names_whole_name_norm"],
                "id_OFAC": dfEmbOFAC.iloc[i_ofac]["id"],
                "IDX_OFAC": dfEmbOFAC.iloc[i_ofac]["IDX"],
                "name_OFAC": dfEmbOFAC.iloc[i_ofac]["names_whole_name"],
                "name_OFAC_norm": dfEmbOFAC.iloc[i_ofac]["names_whole_name_norm"],                    
                "type": entityType,
                "similarity": float(sim)
            })

    return pd.DataFrame(similarities)

def confusion_matrix(
        dfAssestment,
        dfTot,
        entityType,
        threshold        
    ):

    df = dfAssestment[dfAssestment.type == entityType].copy()    

    TP = (
        (df["similarity"] >= threshold) &
        (df["real"] == True) &
        (df["in_neighbourhood"] == True)
    ).sum()
    FP = (
        (df["similarity"] >= threshold) &
        (df["real"] == False) &
        (df["in_neighbourhood"] == True)
    ).sum()
    FN_THRESHOLD = (
        (df["similarity"] < threshold) &
        (df["real"] == True) &
        (df["in_neighbourhood"] == True)
    ).sum()
    FN_NEIGHBOURHOOD = (        
        (df["real"] == True) &
        (df["in_neighbourhood"] == False)
    ).sum()
    FN_TOT = FN_THRESHOLD + FN_NEIGHBOURHOOD
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
        "FN_NEIGHBOURHOOD": FN_NEIGHBOURHOOD,
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

# Data retrieval

with timer("Data retrieval", processMeasures):

    dfOS = pd.read_parquet(
        FILE_OS,
        engine = "fastparquet"
    )

    dfEmb = pd.read_parquet(
        FILE_EMB,
        engine= "fastparquet"
    )

    dfEmbIds = dfEmb[
        [
            "source",
            "id",
            "IDX",
            "type",
            "names_whole_name",
            "names_whole_name_norm"
        ]
    ].copy()

    dfEmbIds.sort_values(
        by = ["source", "id", "IDX"],
        inplace = True
    )

    dfEmbIds.drop_duplicates(
        subset = ["source", "id"],
        keep="first",
        inplace = True
    )

# Get similarities

with timer("Get similarities", processMeasures):

    lsSim = []

    for entityType, k in NEIGHBORS.items():

        dfSimType = getSimilaritiesByEntityType(
            dfEmb,
            entityType,
            k
        )
        lsSim.append(dfSimType)

    dfSim = pd.concat(lsSim)

# Real match retrieval

with timer("Real match retrieval", processMeasures):

    ## Remove OS eu and ofac ids not in dataset

    dfOSInData = dfOS.copy()

    maskOsInDataEU = (
        (~dfOSInData.id_ori_eu.isin(dfEmb[dfEmb.source == "EU"].id.unique())) &
        (dfOSInData.source.isin(["EU", "EU & OFAC"]))
    )
    dfOSInData = dfOSInData[~maskOsInDataEU].copy()

    maskOsInDataOFAC = (
        (~dfOSInData.id_ori_ofac.isin(dfEmb[dfEmb.source == "OFAC"].id.unique())) &
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

    dfAssestment = dfSim.copy()

    dfAssestment.sort_values(
        by = [
            "id_EU",
            "id_OFAC",        
            "similarity"
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

    dfAssestment["in_neighbourhood"] = np.where(
        dfAssestment._merge.isin(["both", "left_only"]),
        True,
        False
    )
        
    dfAssestment = dfAssestment.drop(
        columns = ["_merge", "type"]
    )

    ## Retrieve type for all records

    dfAssestment = dfAssestment.merge(
        dfEmbIds[dfEmbIds.source == "EU"][
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

    for entityType, k in NEIGHBORS.items():
        for threshold in thresholds:
            cm = confusion_matrix(
                dfAssestment,
                dfEmbIds,
                entityType,
                threshold
            )
            cm["neighbourhood"] = k
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