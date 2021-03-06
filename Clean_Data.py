import pandas as pd
import argparse
pd.options.mode.chained_assignment = None  # default='warn'

print("Loading arguments")

parser = argparse.ArgumentParser()
parser.add_argument("-w1", "--who_1", action="store", dest="who_1", help="Please enter PATH of WHO 1 data", required=True)
parser.add_argument("-w2", "--who_2", action="store", dest="who_2", help="Please enter PATH of WHO 2 data", required=True)
parser.add_argument("-i", "--ihme", action="store", dest="ihme", help="Please enter PATH of IHME data", required=True)
parser.add_argument("-c", "--codes", action="store", dest="country_codes", help="Please enter PATH of Country Code data", required=True)
args = parser.parse_args()

print("Arguments loaded:")
print("WHO 1 PATH: ", args.who_1)
print("WHO 2 PATH: ", args.who_2)
print("IMHE PATH: ", args.ihme)
print("Country Codes PATH: ", args.country_codes)

def create_causes_list(base_causes):
    print("Creating causes list")
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

print("Causes list: ", causes)

cols_to_delete_who = ["Admin1", "SubDiv", "List", "Frmat", "IM_Frmat", "Deaths1"]
cols_to_delete_ihme = ["measure", "metric", "upper", "lower"]

relevant_ages_ihme = [
    "<1",
    "1 to 4", "5 to 9",
    "10 to 14", "15 to 19",
    "20 to 24", "25 to 29",
    "30 to 34", "35 to 39",
    "40 to 44", "45 to 49",
    "50 to 54", "55 to 59",
    "60 to 64", "65 to 69",
    "70 to 74", "75 to 79",
    "80 plus"
]

who_rename_dict = {
    "Deaths2": "Deaths at Age 0 Year",
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
    "IM_Deaths4": "Infant Deaths at Age 28-364 Days"
}

ihme_rename_dict = {
    "sex": "Sex",
    "age": "Age",
    "cause": "Cause",
    "year": "Year",
    "val": "Total_Deaths"
}

ihme_wrongly_named_countries = [
    "United States",
    "Alaska",
    "French Guana",
    "Congo (DRC)",
    "Tanzania",
    "Libya",
    "Syria",
    "Iran",
    "Russia",
    "North Korea",
    "South Korea",
    "Taiwan",
    "Laos",
    "Vietnam",
    "Greenland"
]

# TODO get the right correct names for the line below
ihme_corrected_names = [
    "United States of America",
    "Alaska",
    "French Guana",
    "Congo (DRC)",
    "Tanzania",
    "Libya",
    "Syria",
    "Iran",
    "Russia",
    "North Korea",
    "South Korea",
    "Taiwan",
    "Laos",
    "Vietnam",
    "Greenland"
]

print("Cleaning WHO 1")
who_1_data = pd.read_csv(args.who_1, low_memory=False)
who_1_data_clean = who_1_data

print("Deleting columns")
for col in cols_to_delete_who:
    del who_1_data_clean[col]

print("Getting relavant causes")
who_1_data_clean = who_1_data_clean[who_1_data_clean.Cause.isin(causes)]
print("Replacing sex")
who_1_data_clean.Sex.replace([1, 2], ["Male", "Female"], inplace=True)
print("Renaming columns")
who_1_data_clean = who_1_data_clean.rename(columns=who_rename_dict)
print("Melting dataframe")
who_1_data_clean = pd.melt(who_1_data_clean, id_vars=["Country", "Year", "Cause", "Sex"], var_name="Age",
                           value_name="Total_Deaths")

print("Merging causes back into base causes")
for i, row in who_1_data_clean.iterrows():
    who_1_data_clean.set_value(i, "Cause", row.Cause if row.Cause in base_causes else row.Cause[:-1])

print("Adding 'WHO' Source column")
who_1_data_clean["Source"] = "WHO"

print("WHO 1 CLEAN")

print("Cleaning WHO 2")
who_2_data = pd.read_csv(args.who_2, low_memory=False)
who_2_data_clean = who_2_data

print("Deleting columns")
for col in cols_to_delete_who:
    del who_2_data_clean[col]

print("Getting relavant causes")
who_2_data_clean = who_2_data_clean[who_2_data_clean.Cause.isin(causes)]
print("Replacing sex")
who_2_data_clean.Sex.replace([1, 2], ["Male", "Female"], inplace=True)
print("Renaming columns")
who_2_data_clean = who_2_data_clean.rename(columns=who_rename_dict)
print("Melting dataframe")
who_2_data_clean = pd.melt(who_2_data_clean, id_vars=["Country", "Year", "Cause", "Sex"], var_name="Age",
                           value_name="Total_Deaths")

print("Merging causes back into base causes")
for i, row in who_2_data_clean.iterrows():
    who_2_data_clean.set_value(i, "Cause", row.Cause if row.Cause in base_causes else row.Cause[:-1])

print("Adding 'WHO' Source column")
who_2_data_clean["Source"] = "WHO"

print("WHO 2 CLEAN")

who_data_clean = pd.concat([who_1_data_clean, who_2_data_clean]).reset_index(drop=True)

print("Cleaning IHME")

ihme_data = pd.read_csv(args.ihme, low_memory=False)
ihme_data_clean = ihme_data

print("Selecting 'Number' Metric")
ihme_data_clean = ihme_data_clean[ihme_data_clean.metric == "Number"]

print("Deleting columns")
for col in cols_to_delete_ihme:
    del ihme_data_clean[col]

print("Selecting 'Drowning' as cause")
ihme_data_clean = ihme_data_clean[ihme_data_clean.cause == "Drowning"]
print("Selecting relavent ages")
ihme_data_clean = ihme_data_clean[ihme_data_clean.age.isin(relevant_ages_ihme)]

print("Renaming countries")
ihme_data_clean.location.replace(ihme_wrongly_named_countries, ihme_corrected_names, inplace=True)

print("Replacing age descriptions")
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
print("Renaming columns")
ihme_data_clean = ihme_data_clean.rename(columns=ihme_rename_dict)

print("Adding 'IHME' Source column")
ihme_data_clean["Source"] = "IHME"
ihme_data_clean = ihme_data_clean.reset_index(drop=True)

print("IHME CLEAN")

country_codes = pd.read_csv(args.country_codes, low_memory=False)

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