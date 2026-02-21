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
import faiss
from support.utils import timer, confusion_matrix

###############################
#         Parameters          #
###############################

FILE_OS = os.path.join(PATH_DATA, "open_sanctions.parquet")
FILE_EMB = os.path.join(PATH_DATA, "data_text_embeddings.parquet")
FILE_CM = os.path.join(PATH_SRC, "text_embeddings_confusion_matrix.xlsx")
FILE_NOT_IN_NEIGH = os.path.join(PATH_DATA, "text_embeddings_confusion_matrix_not_in_minThreshold.xlsx")
FILE_PROCESS_MEASURES = os.path.join(PATH_SRC, "text_embeddings_confusion_matrix_process_measures.xlsx")

NEIGHBORS = {
    "I": 8,
    "O": 8
}

RANGE = {
    "I": 0.80,
    "O": 0.70
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

def getSimilaritiesByNearestNeighbors(dfEmb, entityType, k):

    emb_cols = [c for c in dfEmb.columns if c.startswith('emb_')]

    dfEU = dfEmb[
        (dfEmb["source"] == "EU") & 
        (dfEmb["type"] == entityType)
    ].copy()

    dfOFAC = dfEmb[
        (dfEmb["source"] == "OFAC") & 
        (dfEmb["type"] == entityType)
    ].copy()

    if dfEU.empty or dfOFAC.empty:
        return pd.DataFrame()
    
    # Conversion to NumPy arrays for FAISS (ultra fast access)

    ids_EU = dfEU["id"].values
    idxs_EU = dfEU["IDX"].values
    names_EU = dfEU["names_whole_name"].values
    names_EU_norm = dfEU["names_whole_name_norm_basic"].values

    ids_OFAC = dfOFAC["id"].values
    idxs_OFAC = dfOFAC["IDX"].values
    names_OFAC = dfOFAC["names_whole_name"].values
    names_OFAC_norm = dfOFAC["names_whole_name_norm_basic"].values

    # Preparation of FAISS matrices

    matrix_EU = np.ascontiguousarray(dfEU[emb_cols].values).astype('float32')
    matrix_OFAC = np.ascontiguousarray(dfOFAC[emb_cols].values).astype('float32')

    dim = matrix_OFAC.shape[1]
    index = faiss.IndexFlatIP(dim)    
    index.add(matrix_OFAC)

    # Search

    distances, indexes = index.search(matrix_EU, k)

    similarities = []

    # Iteration over all EU records

    for i in range(len(dfEU)):
        
        curr_id_eu = ids_EU[i]
        curr_idx_eu = idxs_EU[i]
        curr_name_eu = names_EU[i]
        curr_norm_eu = names_EU_norm[i]

        # Iteration over the k neighbors of each EU record

        for j in range(k):

            idx_ofac_neighbor = indexes[i][j]            
            
            if idx_ofac_neighbor == -1:
                continue

            similarities.append({
                "id_EU": curr_id_eu,
                "IDX_EU": curr_idx_eu,
                "name_EU": curr_name_eu,
                "name_EU_norm": curr_norm_eu,
                "id_OFAC": ids_OFAC[idx_ofac_neighbor],
                "IDX_OFAC": idxs_OFAC[idx_ofac_neighbor],
                "name_OFAC": names_OFAC[idx_ofac_neighbor],
                "name_OFAC_norm": names_OFAC_norm[idx_ofac_neighbor],
                "type": entityType,
                "similarity": float(distances[i][j]),
                "rank": j + 1
            })

    return pd.DataFrame(similarities)

def getSimilaritiesByRange(dfEmb, entityType, minThreshold):

    emb_cols = [c for c in dfEmb.columns if c.startswith('emb_')]    
    
    dfEU = dfEmb[
        (dfEmb["source"] == "EU") & 
        (dfEmb["type"] == entityType)
    ].copy()

    dfOFAC = dfEmb[
        (dfEmb["source"] == "OFAC") & 
        (dfEmb["type"] == entityType)
    ].copy()
    
    if dfEU.empty or dfOFAC.empty:
        return pd.DataFrame()

    # Conversion to NumPy arrays for FAISS (ultra fast access)
    
    ids_EU = dfEU["id"].values
    idxs_EU = dfEU["IDX"].values
    names_EU = dfEU["names_whole_name"].values
    names_EU_norm = dfEU["names_whole_name_norm_basic"].values

    ids_OFAC = dfOFAC["id"].values
    idxs_OFAC = dfOFAC["IDX"].values
    names_OFAC = dfOFAC["names_whole_name"].values
    names_OFAC_norm = dfOFAC["names_whole_name_norm_basic"].values

    # Preparation of FAISS matrices
    
    matrix_EU = np.ascontiguousarray(dfEU[emb_cols].values).astype('float32')
    matrix_OFAC = np.ascontiguousarray(dfOFAC[emb_cols].values).astype('float32')

    dim = matrix_OFAC.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(matrix_OFAC)

    # Search

    lims, D, I = index.range_search(matrix_EU, minThreshold)

    similarities = [] 

    # Iteration over all EU records

    for i in range(len(dfEU)):

        start = lims[i]
        end = lims[i+1]        

        curr_id_eu = ids_EU[i]
        curr_idx_eu = idxs_EU[i]
        curr_name_eu = names_EU[i]
        curr_norm_eu = names_EU_norm[i]

        # Iteration over the neighbors of each EU record within the similarity range

        for j in range(start, end):

            idx_ofac_hit = I[j]
            
            similarities.append({
                "id_EU": curr_id_eu,
                "IDX_EU": curr_idx_eu,
                "name_EU": curr_name_eu,
                "name_EU_norm": curr_norm_eu,
                "id_OFAC": ids_OFAC[idx_ofac_hit],
                "IDX_OFAC": idxs_OFAC[idx_ofac_hit],
                "name_OFAC": names_OFAC[idx_ofac_hit],
                "name_OFAC_norm": names_OFAC_norm[idx_ofac_hit],
                "type": entityType,
                "similarity": float(D[j])
            })

    return pd.DataFrame(similarities)

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
            "names_whole_name_norm_basic"
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

    for entityType, minThreshold in RANGE.items():

        dfSimType = getSimilaritiesByRange(
            dfEmb,
            entityType,
            minThreshold
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

    dfAssestment["in_minThreshold"] = np.where(
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

    for entityType, minThreshold in RANGE.items():
        for threshold in thresholds:
            cm = confusion_matrix(
                dfAssestment,
                dfEmbIds,
                entityType,
                "similarity",
                "in_minThreshold",
                threshold
            )
            cm["min_threshold"] = minThreshold
            lsCm.append(cm)

    dfCm = pd.DataFrame(lsCm)

# Not in min threshold analysis

dfEmb_EU = dfEmb[dfEmb.source == "EU"].copy()
dfEmb_EU.rename(
    columns={
        "id": "id_EU"        
    },
    inplace = True
)
dfEmb_OFAC = dfEmb[dfEmb.source == "OFAC"].copy()
dfEmb_OFAC.rename(
    columns={
        "id": "id_OFAC"        
    },
    inplace = True
)

dfNotInNeigh = dfAssestment[
    ~dfAssestment.in_minThreshold
].copy()

dfNotInNeigh = dfNotInNeigh[
    [
        "id_EU",
        "id_OFAC"
    ]
]

dfNotInNeigh = (
    dfNotInNeigh
    .merge(
         dfEmb_EU, 
         on="id_EU",         
         suffixes=("", "_EU"),
         how="left"
    )
    .merge(
         dfEmb_OFAC, 
         on="id_OFAC",         
         suffixes=("_EU", "_OFAC"),
         how="left"
    )
)

emb_cols = dfNotInNeigh.select_dtypes(include=["float32"]).columns.tolist()
emb_cols_EU = [c for c in emb_cols if c.endswith("EU")]
emb_cols_OFAC = [c for c in emb_cols if c.endswith("OFAC")]

emb_EU = dfNotInNeigh[emb_cols_EU].to_numpy(dtype="float32")
emb_OFAC = dfNotInNeigh[emb_cols_OFAC].to_numpy(dtype="float32")

dfNotInNeigh["similarity"] = np.sum(emb_EU * emb_OFAC, axis=1)

dfNotInNeigh = dfNotInNeigh[
    [c for c in dfNotInNeigh.columns if not c.startswith("emb")]
].copy()

# Export results

dfCm.to_excel(
    FILE_CM, 
    sheet_name = "data",
    index = False
)

dfNotInNeigh.to_excel(
    FILE_NOT_IN_NEIGH, 
    sheet_name = "data",
    index = False
)

dfProcessMeasures = pd.DataFrame(processMeasures)
dfProcessMeasures.to_excel(
    FILE_PROCESS_MEASURES, 
    sheet_name = "data",
    index = False
)