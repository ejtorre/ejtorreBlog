import os
import pandas as pd
import numpy as np
from sentence_transformers import util
import faiss

###############################
#         Parameters          #
###############################

FILES_PATH = os.path.dirname(os.path.abspath(__file__))
FILE_OS = os.path.join(FILES_PATH, "open_sanctions_eu_ofac_id_mapping.parquet")
FILE_DATA = os.path.join(FILES_PATH, "record_linkage_OFAC_EU_embeddings.parquet")
FILE_RESULTS = os.path.join(FILES_PATH, "record_linkage_OFAC_EU_similarity_real_pairs_percentiles.xlsx")

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

## Get real pairs

dfOSReal = dfOSInData[
    dfOSInData.source == "EU & OFAC"
][
    [
        "id_ori_eu",
        "id_ori_ofac"
    ]
].copy()

# Get real pairs similarities

dfData_EU = dfData[dfData.source == "EU"]
dfData_OFAC = dfData[dfData.source == "OFAC"]

dfOSRealSim = (
    dfOSReal
    .merge(
         dfData_EU, 
         left_on="id_ori_eu",
         right_on="id",
         suffixes=("", "_EU"),
         how="left"
    )
    .merge(
         dfData_OFAC, 
         left_on="id_ori_ofac",
         right_on="id",
         suffixes=("_EU", "_OFAC"),
         how="left"
    )
)

emb_cols = dfOSRealSim.select_dtypes(include=["float32"]).columns.tolist()
emb_cols_EU = [c for c in emb_cols if c.endswith("EU")]
emb_cols_OFAC = [c for c in emb_cols if c.endswith("OFAC")]

emb_EU = dfOSRealSim[emb_cols_EU].to_numpy(dtype="float32")
emb_OFAC = dfOSRealSim[emb_cols_OFAC].to_numpy(dtype="float32")

dfOSRealSim["similarity"] = np.sum(emb_EU * emb_OFAC, axis=1)

# Get distance percentiles

dfOSRealIds = dfOSRealSim[
    [c for c in dfOSRealSim.columns if not c.startswith("dim")]
].copy()

dfOSRealIds.sort_values(
    by = [
        "id_ori_eu",
        "id_ori_ofac",
        "similarity",
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
        "id_ori_eu",
        "id_ori_ofac"
    ],
    keep="first",
    inplace=True
)

numReal = len(dfOSRealIds)

dfSummary = (
    dfOSRealIds['similarity']
    .quantile([i/100 for i in range(101)])
    .rename('similarity')
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