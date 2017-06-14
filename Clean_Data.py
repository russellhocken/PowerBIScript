import pandas as pd

pd.options.mode.chained_assignment = None  # default='warn'
import csv
import requests
from io import BytesIO
from zipfile import ZipFile
import fnmatch


def download_data(data_url):
    r = requests.get(data_url, stream=True)
    r.raise_for_status()

    z = ZipFile(BytesIO(r.content))
    z.extractall()


# Country Codes
download_data("http://www.who.int/entity/healthinfo/statistics/country_codes.zip")

# WHO Part 1
download_data("http://www.who.int/entity/healthinfo/statistics/Morticd10_part1.zip")

# WHO Part 2
download_data("http://www.who.int/entity/healthinfo/statistics/Morticd10_part2.zip")

# IHME
download_data(
    "http://www.s3.healthdata.org/querytool-2015-prod/e3f763c85541ed99829f92215856e892_files/IHME-GBD_2015_DATA-e3f763c8-1.zip")


def create_causes_list(base_causes):
    causes = []

    for cause in base_causes:
        causes.append(cause)

        for i in range(0, 10):
            causes.append(cause + str(i))

    return causes


base_causes = ["W65", "W66", "W67", "W68", "W69", "W70", "W71", "W72", "W73", "W74",
               "X36", "X37", "X38", "X39", "X71", "X92",
               "V90", "V92",
               "Y21"]

causes = create_causes_list(base_causes)

cols_to_delete_who = ["Admin1", "SubDiv", "List", "Frmat", "IM_Frmat", "Deaths1"]
cols_to_delete_ihme = ["measure", "metric", "upper", "lower"]

relevant_ages = ["<1",
                 "1 to 4", "5 to 9",
                 "10 to 14", "15 to 19",
                 "20 to 24", "25 to 29",
                 "30 to 34", "35 to 39",
                 "40 to 44", "45 to 49",
                 "50 to 54", "55 to 59",
                 "60 to 64", "65 to 69",
                 "70 to 74", "75 to 79",
                 "80 plus"]

who_rename_dict = {"Deaths2": "Deaths at Age 0 Year",
                   "Deaths3": "Deaths at Age 1 Year",
                   "Deaths4": "Deaths at Age 2 Years",
                   "Deaths5": "Deaths at Age 3 Years",
                   "Deaths6": "Deaths at Age 4 Years",
                   "Deaths7": "Deaths at Age 5-9 Years",
                   "Deaths8": "Deaths at Age 10-14 Years",
                   "Deaths9": "Deaths at Age 15-19 Years",
                   "Deaths10": "Deaths at Age 20-24 Years",
                   "Deaths11": "Deaths at Age 25-29 Years",
                   "Deaths12": "Deaths at Age 30-34 Years",
                   "Deaths13": "Deaths at Age 35-39 Years",
                   "Deaths14": "Deaths at Age 40-44 Years",
                   "Deaths15": "Deaths at Age 45-49 Years",
                   "Deaths16": "Deaths at Age 50-54 Years",
                   "Deaths17": "Deaths at Age 55-59 Years",
                   "Deaths18": "Deaths at Age 60-64 Years",
                   "Deaths19": "Deaths at Age 65-69 Years",
                   "Deaths20": "Deaths at Age 70-74 Years",
                   "Deaths21": "Deaths at Age 75-79 Years",
                   "Deaths22": "Deaths at Age 80-84 Years",
                   "Deaths23": "Deaths at Age 85-89 Years",
                   "Deaths24": "Deaths at Age 90-94 Years",
                   "Deaths25": "Deaths at Age 95 Years and Above",
                   "Deaths26": "Deaths at Age Unspecified",
                   "IM_Deaths1": "Infant Deaths at Age 0 Days",
                   "IM_Deaths2": "Infant Deaths at Age 1-6 Days",
                   "IM_Deaths3": "Infant Deaths at Age 7-27 Days",
                   "IM_Deaths4": "Infant Deaths at Age 28-364 Days"}

ihme_rename_dict = {"sex": "Sex",
                    "age": "Age",
                    "cause": "Cause",
                    "year": "Year",
                    "val": "Total_Deaths"}

who_1_data = pd.read_csv("Morticd10_part1", low_memory=False)
who_1_data_clean = who_1_data

for col in cols_to_delete_who:
    del who_1_data_clean[col]

who_1_data_clean = who_1_data_clean[who_1_data_clean.Cause.isin(causes)]
who_1_data_clean.Sex.replace([1, 2], ["Male", "Female"], inplace=True)
who_1_data_clean = who_1_data_clean.rename(columns=who_rename_dict)
who_1_data_clean = pd.melt(who_1_data_clean, id_vars=["Country", "Year", "Cause", "Sex"], var_name="Age",
                           value_name="Total_Deaths")

for i, row in who_1_data_clean.iterrows():
    who_1_data_clean.set_value(i, "Cause", row.Cause if row.Cause in base_causes else row.Cause[:-1])

who_1_data_clean["Source"] = "WHO"

who_2_data = pd.read_csv("Morticd10_part2", low_memory=False)
who_2_data_clean = who_2_data

for col in cols_to_delete_who:
    del who_2_data_clean[col]

who_2_data_clean = who_2_data_clean[who_2_data_clean.Cause.isin(causes)]
who_2_data_clean.Sex.replace([1, 2], ["Male", "Female"], inplace=True)
who_2_data_clean = who_2_data_clean.rename(columns=who_rename_dict)
who_2_data_clean = pd.melt(who_2_data_clean, id_vars=["Country", "Year", "Cause", "Sex"], var_name="Age",
                           value_name="Total_Deaths")

for i, row in who_2_data_clean.iterrows():
    who_2_data_clean.set_value(i, "Cause", row.Cause if row.Cause in base_causes else row.Cause[:-1])

who_2_data_clean["Source"] = "WHO"

who_data_clean = pd.concat([who_1_data_clean, who_2_data_clean]).reset_index(drop=True)

ihme_data = pd.read_csv("IHME-GBD_2015_DATA-e3f763c8-1.csv", low_memory=False)
ihme_data_clean = ihme_data

ihme_data_clean = ihme_data_clean[ihme_data_clean.metric == "Number"]

for col in cols_to_delete_ihme:
    del ihme_data_clean[col]

ihme_data_clean = ihme_data_clean[ihme_data_clean.cause == "Drowning"]

ihme_data_clean = ihme_data_clean[ihme_data_clean.age.isin(relevant_ages)]
ihme_data_clean.age.replace(["<1",
                             "1 to 4", "5 to 9",
                             "10 to 14", "15 to 19",
                             "20 to 24", "25 to 29",
                             "30 to 34", "35 to 39",
                             "40 to 44", "45 to 49",
                             "50 to 54", "55 to 59",
                             "60 to 64", "65 to 69",
                             "70 to 74", "75 to 79",
                             "80 plus"],
                            ["Deaths at Age <1 Years",
                             "Deaths at Age 1-4 Years",
                             "Deaths at Age 5-9 Years",
                             "Deaths at Age 10-14 Years",
                             "Deaths at Age 15-19 Years",
                             "Deaths at Age 20-24 Years",
                             "Deaths at Age 25-29 Years",
                             "Deaths at Age 30-34 Years",
                             "Deaths at Age 35-39 Years",
                             "Deaths at Age 40-44 Years",
                             "Deaths at Age 45-49 Years",
                             "Deaths at Age 50-54 Years",
                             "Deaths at Age 55-59 Years",
                             "Deaths at Age 60-64 Years",
                             "Deaths at Age 65-69 Years",
                             "Deaths at Age 70-74 Years",
                             "Deaths at Age 75-79 Years",
                             "Deaths at Age 80+ Years"], inplace=True)
ihme_data_clean = ihme_data_clean.rename(columns=ihme_rename_dict)

ihme_data_clean["Source"] = "IHME"
ihme_data_clean = ihme_data_clean.reset_index(drop=True)

country_codes = pd.read_csv("country_codes", low_memory=False)

who_data_clean = who_data_clean.merge(country_codes, left_on="Country", right_on="country")

who_data_clean = who_data_clean.rename(columns={"Country": "Country_Code", "name": "Country_Name"})

del who_data_clean["country"]

ihme_data_clean = ihme_data_clean.merge(country_codes, left_on="location", right_on="name")

ihme_data_clean = ihme_data_clean.rename(columns={"location": "Country_Name", "country": "Country_Code"})

del ihme_data_clean["name"]

order = ["Source", "Country_Name", "Country_Code", "Year", "Cause", "Sex", "Age", "Total_Deaths"]

who_data_clean = who_data_clean[order]
ihme_data_clean = ihme_data_clean[order]

total_data_clean = pd.concat([who_data_clean, ihme_data_clean])
total_data_clean = total_data_clean[total_data_clean.Total_Deaths.notnull()].reset_index(drop=True)

filename = "total_who_ihme_data_clean.csv"

try:
    f = open(filename, "w+")
    f.close()
except:
    print("No filefound: {}".format(filename))

with open(filename, "a") as openFile:
    total_data_clean.to_csv(openFile)
