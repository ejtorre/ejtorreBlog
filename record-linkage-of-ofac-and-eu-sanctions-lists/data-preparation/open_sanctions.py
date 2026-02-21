###############################
#            Paths            #
###############################

import os

PATH_SRC = os.path.dirname(os.path.abspath(__file__))
PATH_PROJECT = os.path.normpath(os.path.join(PATH_SRC, ".."))
PATH_DATA = os.path.join(PATH_PROJECT, "data")

###############################
#          Imports            #
###############################

import pandas as pd
import numpy as np

###############################
#         Parameters          #
###############################

FILES_OS = {
    "EU": os.path.join(PATH_DATA, "targets.nested.EU.json"),
    "OFAC": os.path.join(PATH_DATA, "targets.nested.OFAC.json")
}

FILE_RESULTS = os.path.join(PATH_DATA, "open_sanctions.parquet")

###############################
#    Classes and functions    #
###############################

###############################
#          Process            #
###############################

# Read OpenSanctions data

lsOS= []
lsOSOri = []

for source, file in FILES_OS.items():   
    
    df = pd.read_json(file, dtype=True, lines=True)
    df["source"] = source

    lsOSOri.append(df)

    df = df.explode(["referents"])
    if source == "EU":        
        df["referents_first"] = np.where(
            df.referents.str.match(r'^eu-fsf-(.+)$', na=False),
            1,
            0
        )        
    elif source == "OFAC":
        df["referents_first"] = np.where(
            df.referents.str.match(r'^ofac-\d+$', na=False),
            1,
            0
        )        
    df.sort_values(
        by=["id", "referents_first", "referents"],
        ascending=[False, False, False],
        inplace=True
    )

    df = df.drop_duplicates(
        subset=["id"],
        keep="first"
    )

    if source == "EU":
        df["id_ori"] = df.referents.str.extract(r'^eu-fsf-(.+)$')
        df['id_ori'] = (
            df['id_ori']
            .str.replace('-', '.', regex=False)
            .str.replace('eu.', 'EU.', regex=False)
        )
    elif source == "OFAC":
        df["id_ori"] = df.referents.str.extract(r'^ofac-(\d+)$')

    df = df[
        [
            "id",
            "schema",
            "source",
            "target",
            "datasets",
            "caption",
            "referents",
            "id_ori"            
        ]
    ]
    lsOS.append(df)

dfOS = pd.concat(lsOS, ignore_index=True)
dfOSOri = pd.concat(lsOSOri, ignore_index=True)

# Create ID mapping between EU and OFAC based on OpenSanctions referents

dfOSId = dfOS[dfOS.source == "EU"][
    [
        'id', 
        'schema',
        'target', 
        'caption', 
        'referents',
        'id_ori'
    ]
].copy()

dfOSId = dfOSId.merge(
    dfOS[dfOS.source == "OFAC"][
        [
            'id', 
            'schema',
            'target',
            'caption', 
            'referents',
            'id_ori'
        ]
    ],       
    on=['id'],
    how='outer',
    suffixes=('_eu', '_ofac'),
    indicator=True
)

dfOSId['source'] = dfOSId['_merge'].map(
    {
        'left_only': 'EU',
        'right_only': 'OFAC',
        'both': 'EU & OFAC'
    }
)

dfOSId = dfOSId.drop(columns='_merge')

# Export results

dfOSId.to_parquet(
    FILE_RESULTS, 
    index=False, 
    engine="fastparquet",
    compression="snappy"
)