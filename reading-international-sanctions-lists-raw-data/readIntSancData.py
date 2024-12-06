import inoutlists
import pandas as pd
import numpy as np
from pathlib import Path
import os

#####################################
#             Parameters            #
#####################################

pyScriptPath = Path(os.getcwd(),__file__).parent

dataInfo = {
    "OFAC-SDN": {
        "input": {
            "localPath": Path(pyScriptPath, "sdn.xml"),            
            "url": "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML",
            "loader": inoutlists.LoaderOFACXML
        },
        "output": {
            "json": Path(pyScriptPath, "ofac_sdn.json"),
            "csv": Path(pyScriptPath, "ofac_sdn.csv")
        }
    },
    "OFAC-NON-SDN": {
        "input": {   
            "localPath": Path(pyScriptPath, "consolidated.xml"),         
            "url": "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/CONSOLIDATED.XML",
            "loader": inoutlists.LoaderOFACXML
        },
        "output": {
            "json": Path(pyScriptPath, "ofac_consolidated.json"),
            "csv": Path(pyScriptPath, "ofac_consolidated.csv")
        }
    },
    "EU": {
        "input": { 
            "localPath": Path(pyScriptPath, "20240513-FULL-1_1(xsd).xml"),                       
            "url": "https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content?token=dG9rZW4tMjAxNw",                    
            "loader": inoutlists.LoaderEUXML
        },
        "output": {
            "json": Path(pyScriptPath, "eu.json"),
            "csv": Path(pyScriptPath, "eu.csv")
        }
    },
    "UN": {
        "input": {      
            "localPath": Path(pyScriptPath, "un_consolidated.xml"),      
            "url": "https://scsanctions.un.org/resources/xml/en/consolidated.xml",
            "loader": inoutlists.LoaderUNXML
        },
        "output": {
            "json": Path(pyScriptPath, "un.json"),
            "csv": Path(pyScriptPath, "un.csv")
        }
    }
}

#####################################
#             Process               #
#####################################

data = {}
dfLst = []

for source, sourceInfo in dataInfo.items():        
    data[source] = inoutlists.load(
        sourceInfo["input"]["url"], 
        sourceInfo["input"]["loader"], 
        f"{source}"
    )
    dfSource = inoutlists.dump(data[source], dumper=inoutlists.DumperPandas)
    dfSource["source"] = source
    dfLst.append(dfSource)

df = pd.concat(dfLst)

dfId = df.drop_duplicates(["id", "source"])
dfIdFacts = dfId.groupby(
    [
        "source",
        "type"
    ],
    as_index = False,
    dropna = False
).agg(
   num_list_entries = ("source", "count")   
)

dfNationalities = df.drop_duplicates(["id", "nationalities_country_desc"])
dfNationalitiesFacts = dfNationalities.groupby(
    [
        "source",
        "type",
        "nationalities_country_desc"
    ],
    as_index = False,
    dropna = False
).agg(
   num_list_entries = ("source", "count")   
)

dfAddresses = df.drop_duplicates(["id", "addresses_country_desc"])
dfAddressesFacts = dfAddresses.groupby(
    [
        "source",
        "type",
        "addresses_country_desc"
    ],
    as_index = False,
    dropna = False
).agg(
   num_list_entries = ("source", "count")   
)

dfPrograms = df.drop_duplicates(["id", "programs"])
dfProgramsFacts = dfPrograms.groupby(
    [
        "source",
        "type",
        "programs"
    ],
    as_index = False,
    dropna = False
).agg(
   num_list_entries = ("source", "count")   
)

with pd.ExcelWriter(Path(pyScriptPath, "intSancFacts.xlsx")) as writerExcel:
    dfIdFacts.to_excel(
        excel_writer = writerExcel, 
        sheet_name = "IDS", 
        index = False
    )
    dfNationalitiesFacts.to_excel(
        excel_writer = writerExcel, 
        sheet_name = "NATIONALITIES", 
        index = False
    )
    dfAddressesFacts.to_excel(
        excel_writer = writerExcel, 
        sheet_name = "ADDRESSES", 
        index=False
    )
    dfProgramsFacts.to_excel(
        excel_writer = writerExcel, 
        sheet_name = "PROGRAMS", 
        index = False
    )