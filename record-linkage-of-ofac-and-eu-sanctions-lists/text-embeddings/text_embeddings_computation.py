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
from sentence_transformers import SentenceTransformer
from support.utils import timer

###############################
#         Parameters          #
###############################

FILE_DATA = os.path.join(PATH_DATA, "data_EU_OFAC.parquet")
FILE_RESULTS = os.path.join(PATH_DATA, "data_text_embeddings.parquet")
FILE_PROCESS_MEASURES = os.path.join(PATH_SRC, "text_embeddings_computation_process_measures.xlsx")

RELEVANT_COLS = [
    "source",
    "id",
    "IDX",
    "type",
    "names_whole_name",
    "names_whole_name_norm_basic"
]

processMeasures = []

MODELS = {
    "I": "dell-research-harvard/lt-wikidata-comp-multi",
    "O": "dell-research-harvard/lt-wikidata-comp-multi"
}

###############################
#    Classes and functions    #
###############################

###############################
#          Process            #
###############################

# Data retrieval

with timer("Data retrieval", processMeasures):    

    dfData = pd.read_parquet(
        FILE_DATA,
        engine= "fastparquet"
    )

# Data preparation

with timer("Data preparation", processMeasures):

    dfDataEmb = dfData[
        (dfData.type.isin(["I", "O"])) &
        (dfData.names_strong)
    ].copy()

    dfDataEmb.sort_values(
        by=[
            "source", 
            "id", 
            "names_whole_name_norm_basic"
        ],
        inplace=True
    )

    dfDataEmb.drop_duplicates(
        subset=[
            "source", 
            "id", 
            "names_whole_name_norm_basic"
        ],
        keep="first",
        inplace=True
    )

    dfDataEmb = dfDataEmb[RELEVANT_COLS]   

# Embeddings computation

with timer("Embeddings computation", processMeasures):

    lsEmb = []    

    for entityType, modelName in MODELS.items():

        model = SentenceTransformer(modelName)
        dfDataEmbType = dfDataEmb[
            dfDataEmb.type == entityType
        ]
        emb = model.encode(
            dfDataEmbType["names_whole_name_norm_basic"].to_list(),
            batch_size=32, 
            normalize_embeddings=True, 
            show_progress_bar=True
        )
        dfEmbType = pd.DataFrame(emb)
        dfEmbType.columns = [f'emb_{i}' for i in range(dfEmbType.shape[1])]
        dfEmbType = pd.concat([dfDataEmbType.reset_index(drop=True), dfEmbType], axis=1)
        lsEmb.append(dfEmbType)

    dfEmb = pd.concat(lsEmb)

# Export results

with timer("Export results", processMeasures):

    dfEmb.to_parquet(
        FILE_RESULTS, 
        index=False, 
        engine="fastparquet",
        compression="snappy"
    )

dfProcessMeasures = pd.DataFrame(processMeasures)
dfProcessMeasures.to_excel(
    FILE_PROCESS_MEASURES, 
    sheet_name = "data",
    index = False
)