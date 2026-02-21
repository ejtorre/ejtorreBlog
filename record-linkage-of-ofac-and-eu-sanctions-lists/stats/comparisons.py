import os
import pandas as pd

###############################
#         Parameters          #
###############################

FILES_PATH = os.path.dirname(os.path.abspath(__file__))
FILE_DATA = os.path.join(FILES_PATH, "record_linkage_OFAC_EU_data.parquet")
FILE_RESULTS = os.path.join(FILES_PATH, "record_linkage_OFAC_EU_comparisons.xlsx")

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

comparisonsResults = {
    "comparison_type": [],
    "num_total_EU": [],
    "num_notna_EU": [],
    "num_na_EU": [],
    "num_total_OFAC": [],
    "num_notna_OFAC": [],
    "num_na_OFAC": [],  
    "comp_num_notna": [],
    "comp_num_na": []
}

## FULL

dfCompDims = [
    "source", 
    "type"
]
dfCompDedup = [
    "source", 
    "id", 
    "type", 
    "names_whole_name_norm"
]

dfDataCompare = dfData.copy()
dfDataCompare.sort_values(
    by=dfCompDedup,
    inplace=True
)
dfDataCompare.drop_duplicates(
    subset=dfCompDedup,
    keep="first",
    inplace=True
)

dfCompFull = (
    dfDataCompare.groupby(
        dfCompDims,
        dropna=False
    )
    .size()
    .unstack('source', fill_value=0)
)

dfCompFull.reset_index(inplace=True)
compMask = dfCompFull["type"].notna()

num_total_EU = dfCompFull["EU"].sum()
num_notna_EU = dfCompFull.loc[compMask, "EU"].sum()
num_na_EU = num_total_EU - num_notna_EU

num_total_OFAC = dfCompFull["OFAC"].sum()
num_notna_OFAC = dfCompFull.loc[compMask, "OFAC"].sum()
num_na_OFAC = num_total_OFAC - num_notna_OFAC

numComparisonsNoNa = (
    dfCompFull.loc[compMask, "EU"]
    * dfCompFull.loc[compMask, "OFAC"]
).sum()

comparisonsResults["comparison_type"].append("Full")
comparisonsResults["num_total_EU"].append(num_total_EU)
comparisonsResults["num_notna_EU"].append(num_notna_EU)
comparisonsResults["num_na_EU"].append(num_na_EU)
comparisonsResults["num_total_OFAC"].append(num_total_OFAC)
comparisonsResults["num_notna_OFAC"].append(num_notna_OFAC)
comparisonsResults["num_na_OFAC"].append(num_na_OFAC)
comparisonsResults["comp_num_notna"].append(numComparisonsNoNa)
comparisonsResults["comp_num_na"].append(
    num_na_EU * num_total_OFAC +
    num_na_OFAC * num_total_EU -
    num_na_EU * num_na_OFAC
)

## Individuals + Year of birth

dfCompDims = [
    "source", 
    "type", 
    "dates_of_birth_year"
]
dfCompDedup = [
    "source", 
    "id", 
    "type", 
    "dates_of_birth_year", 
    "names_whole_name_norm"
]

dfDataCompare = dfData[
    dfData['type'] == 'I'
].copy()
dfDataCompare.sort_values(
    by=dfCompDedup,
    inplace=True
)
dfDataCompare.drop_duplicates(
    subset=dfCompDedup,
    keep="first",
    inplace=True
)

dfCompIndividualsYOB = (
    dfDataCompare.groupby(
        dfCompDims,
        dropna=False
    )
    .size()
    .unstack('source', fill_value=0)
)

dfCompIndividualsYOB.reset_index(inplace=True)
compMask = dfCompIndividualsYOB["dates_of_birth_year"].notna()

num_total_EU = dfCompIndividualsYOB["EU"].sum()
num_notna_EU = dfCompIndividualsYOB.loc[compMask, "EU"].sum()
num_na_EU = num_total_EU - num_notna_EU

num_total_OFAC = dfCompIndividualsYOB["OFAC"].sum()
num_notna_OFAC = dfCompIndividualsYOB.loc[compMask, "OFAC"].sum()
num_na_OFAC = num_total_OFAC - num_notna_OFAC

numComparisonsNoNa = (
    dfCompIndividualsYOB.loc[compMask, "EU"]
    * dfCompIndividualsYOB.loc[compMask, "OFAC"]
).sum()

comparisonsResults["comparison_type"].append("Individuals_YOB")
comparisonsResults["num_total_EU"].append(num_total_EU)
comparisonsResults["num_notna_EU"].append(num_notna_EU)
comparisonsResults["num_na_EU"].append(num_na_EU)
comparisonsResults["num_total_OFAC"].append(num_total_OFAC)
comparisonsResults["num_notna_OFAC"].append(num_notna_OFAC)
comparisonsResults["num_na_OFAC"].append(num_na_OFAC)
comparisonsResults["comp_num_notna"].append(numComparisonsNoNa)
comparisonsResults["comp_num_na"].append(
    num_na_EU * num_total_OFAC +
    num_na_OFAC * num_total_EU -
    num_na_EU * num_na_OFAC
)

## Companies + addresss ISO country code

dfCompDims = [
    "source", 
    "type", 
    "addresses_country_ISO_code"
]
dfCompDedup = [
    "source", 
    "id", 
    "type", 
    "addresses_country_ISO_code", 
    "names_whole_name_norm"
]

dfDataCompare = dfData[
    dfData['type'] == 'O'
].copy()
dfDataCompare.sort_values(
    by=dfCompDedup,
    inplace=True
)
dfDataCompare.drop_duplicates(
    subset=dfCompDedup,
    keep="first",
    inplace=True
)

dfCompCompaniesCountry = (
    dfDataCompare.groupby(
        dfCompDims,
        dropna=False
    )
    .size()
    .unstack('source', fill_value=0)
)

dfCompCompaniesCountry.reset_index(inplace=True)
compMask = dfCompCompaniesCountry["addresses_country_ISO_code"].notna()

num_total_EU = dfCompCompaniesCountry["EU"].sum()
num_notna_EU = dfCompCompaniesCountry.loc[compMask, "EU"].sum()
num_na_EU = num_total_EU - num_notna_EU

num_total_OFAC = dfCompCompaniesCountry["OFAC"].sum()
num_notna_OFAC = dfCompCompaniesCountry.loc[compMask, "OFAC"].sum()
num_na_OFAC = num_total_OFAC - num_notna_OFAC

numComparisonsNoNa = (
    dfCompCompaniesCountry.loc[compMask, "EU"]
    * dfCompCompaniesCountry.loc[compMask, "OFAC"]
).sum()

comparisonsResults["comparison_type"].append("Companies_Country")
comparisonsResults["num_total_EU"].append(num_total_EU)
comparisonsResults["num_notna_EU"].append(num_notna_EU)
comparisonsResults["num_na_EU"].append(num_na_EU)
comparisonsResults["num_total_OFAC"].append(num_total_OFAC)
comparisonsResults["num_notna_OFAC"].append(num_notna_OFAC)
comparisonsResults["num_na_OFAC"].append(num_na_OFAC)
comparisonsResults["comp_num_notna"].append(numComparisonsNoNa)
comparisonsResults["comp_num_na"].append(
    num_na_EU * num_total_OFAC +
    num_na_OFAC * num_total_EU -
    num_na_EU * num_na_OFAC
)

## Companies + addresss ISO country code + city normalized

dfCompDims = [
    "source",
    "type",
    "addresses_country_ISO_code",
    "addresses_city_norm"
]
dfCompDedup = [
    "source",
    "id",
    "type",
    "addresses_country_ISO_code",
    "addresses_city_norm",
    "names_whole_name_norm"
]

dfDataCompare = dfData[
    dfData['type'] == 'O'
].copy()
dfDataCompare.sort_values(
    by=dfCompDedup,
    inplace=True
)
dfDataCompare.drop_duplicates(
    subset=dfCompDedup,
    keep="first",
    inplace=True
)

dfCompCompaniesCountryCity = (
    dfDataCompare.groupby(
        dfCompDims,
        dropna=False
    )
    .size()
    .unstack('source', fill_value=0)
)

dfCompCompaniesCountryCity.reset_index(inplace=True)
compMask = (
    (dfCompCompaniesCountryCity["addresses_country_ISO_code"].notna()) &
    (dfCompCompaniesCountryCity["addresses_city_norm"].notna())
)

num_total_EU = dfCompCompaniesCountryCity["EU"].sum()
num_notna_EU = dfCompCompaniesCountryCity.loc[compMask, "EU"].sum()
num_na_EU = num_total_EU - num_notna_EU

num_total_OFAC = dfCompCompaniesCountryCity["OFAC"].sum()
num_notna_OFAC = dfCompCompaniesCountryCity.loc[compMask, "OFAC"].sum()
num_na_OFAC = num_total_OFAC - num_notna_OFAC

numComparisonsNoNa = (
    dfCompCompaniesCountryCity.loc[compMask, "EU"]
    * dfCompCompaniesCountryCity.loc[compMask, "OFAC"]
).sum()

comparisonsResults["comparison_type"].append("Companies_Country_City")
comparisonsResults["num_total_EU"].append(num_total_EU)
comparisonsResults["num_notna_EU"].append(num_notna_EU)
comparisonsResults["num_na_EU"].append(num_na_EU)
comparisonsResults["num_total_OFAC"].append(num_total_OFAC)
comparisonsResults["num_notna_OFAC"].append(num_notna_OFAC)
comparisonsResults["num_na_OFAC"].append(num_na_OFAC)
comparisonsResults["comp_num_notna"].append(numComparisonsNoNa)
comparisonsResults["comp_num_na"].append(
    num_na_EU * num_total_OFAC +
    num_na_OFAC * num_total_EU -
    num_na_EU * num_na_OFAC
)

dfComparisonsResults = pd.DataFrame(comparisonsResults)

# Export results

dfComparisonsResults.to_excel(
    FILE_RESULTS, 
    sheet_name = "data",
    index = False
)