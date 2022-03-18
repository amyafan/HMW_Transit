"""
1-2022 Amy Fan 

Cleaning the demographic and geographic files that we'll be using in the future and merging them into one file.

inputs (See the README for a more detailed description of the data):

    - student_demographics.csv

        This has all the student demographics

    - start_end_times.xlsx

        We read the second sheet to get each school's end time. 
    
    - school_geography.csv
        
        This gives the coordinates of each school. 

    - name_match_final.csv

        This is the key that matches the school names in start_end_times to school_geography

    - Google Maps API

        This is not a dataset in the folder, but we will be merging in data from the Google Maps API, namely 
        the places_id. This will allow us to calculate distances in Google Maps more effectively 

outputs (in cleaned_data): 

    - cleaned_data/school_demo_geo.csv

        cleaned dataset of demographic, geographic, and relevant Google Maps data 
    
    - raw_data/HMW_info.csv
        
        one line with relevant information about Hattie Mae White -- to be used later

We will also be dropping the following schools for the following reasons: 

    Alternative Schools: 
        - "HARRIS CO J J A E P"
        - "EL DAEP"
    Online: 
        - "SOAR Academy"
    Shut down: 
        - "YOUNG SCHOLARS ACADEMY FOR EXCELLE"
        - "ENERGIZED FOR STEM ACADEMY SOUTHEAST MIDDLE" 
    Pre-K (at multiple locations): https://www.houstonisd.org/Page/32496
        - "YOUNG LEARNERS"

"""

#########
# SETUP #
#########

import pandas as pd
import requests
import dask.dataframe as dd

# directories
raw_data = "//Users//afan//Desktop//Misc//HMW_Transit//prep//raw_data//"
int_data = "//Users//afan//Desktop//Misc//HMW_Transit//prep//int_data//"
cleaned_data = "//Users//afan//Desktop//Misc//HMW_Transit//cleaned_data//"

# API subscription keys for Google Maps
gm_api_key = GM_API_KEY

# FUNCTION FOR GOOGLE MAPS API


def get_place_id(row, api_key):
    """
    function to get the Place ID from the Google Maps API 

    Documentation for the API can be found here: https://developers.google.com/maps/documentation/places/web-service/search-find-place

    input: 
    _____

        row: Pandas Series (or is it a list?)

            In this case, we use a row of a DataFrame. The indices are as follows:

            row[1]: String

                School Name

            row[2]: String

                School Address

            row[4]: float

                Longitude of the school

            row[5]: float

                Latitude of the school  

        api_key: String 

            Provided by Google. Needed to access the database

    output: 
    ______

        p_id: String

            The Google Place ID 
    """

    s_name = row[1]

    # school address
    s_addr = row[2]

    # For some reason, X and Y are longitude and latitude, respectively
    s_lat = row[5]
    s_long = row[4]

    url = f"https://maps.googleapis.com/maps/api/place/findplacefromtext/json?fields=place_id&input={s_name}%20School%20{s_addr}&inputtype=textquery&locationbias=circle%3A200%{s_lat}2%2C{s_long}&key={api_key}"

    req = requests.get(url)

    print(s_name, ":", req.status_code)

    p_id = pd.DataFrame(req.json())['candidates'].apply(pd.Series).iloc[0, 0]

    return p_id

############################
# student_demographics.csv #
############################


print("\nStudent Demographics:")

# read in the file
sd_str = raw_data + "student_demographics.csv"
sd = pd.read_csv(sd_str)
print(sd.shape)
print(sd.head())

# delete all the columns that were about percentages
sd_c = sd.iloc[:, [item[-1] != 'P' for item in sd.columns]]

# rename columns
sd_final = sd_c.rename(columns={'CPET504C': 'D504',
                                'CPETALLC': 'All',
                                'CPETASIC': 'Asian',
                                'CPETATTC': 'Attrition',
                                'CPETATTD': 'Attrition_Denom',
                                'CPETBLAC': 'Black',
                                'CPETDISC': 'DAEP',
                                'CPETDSLC': 'Dyslexia',
                                'CPETECOC': 'Econ_Disadv',
                                'CPETFEMC': 'Female',
                                'CPETFOSC': 'Foster_Care',
                                'CPETHISC': 'Hispanic',
                                'CPETHOMC': 'Homeless',
                                'CPETIMMC': 'Immigrant',
                                'CPETINDC': 'Am_Ind',
                                'CPETLEPC': 'Eng_Learner',
                                'CPETMALC': 'Male',
                                'CPETMIGC': 'Migrant',
                                'CPETMLCC': 'Military_Conn',
                                'CPETNEDC': 'Non_Ed_Disadv',
                                'CPETPCIC': 'Pacific_Is',
                                'CPETRSKC': 'At_Risk',
                                'CPETTT1C': 'Title_I',
                                'CPETTWOC': 'Two_Or_More',
                                'CPETWHIC': 'White'})

print(sd_final.head())

########################
# school_geography.csv #
########################

print("\nSchool Geography:")

# read in the file
sg_str = raw_data + "school_geography.csv"
sg = pd.read_csv(sg_str)
print(sg.shape)
print(sg.head())

# just keep the relevant columns
sg_cols = ['School_Num', 'School_Nam', 'Place_addr', 'Grade_Rang', 'X', 'Y']
sg_final = sg[sg_cols]
print(sg_final.head())

########################
# start_end_times.xlsx #
########################

print("\nSchool Start and End Times:")

# excel sheet with schools end times -- the information on all the schools is on the second sheet
et_str = raw_data + "start_end_times.xlsx"
et_cols = ['Campus Short Name', 'End Time']
et = pd.read_excel(et_str, 1, usecols=et_cols, parse_dates=['End Time'])

print(et.head())

###########################
# MERGING THINGS TOGETHER #
###########################

# Now, we should merge all three of these datasets together.
print("\n\nStarting the merge!")

# To match school names to the TEA names though, we will have to read in the key.
key_str = int_data + "name_match_final.csv"
key = pd.read_csv(key_str)

# We're going to do this one step at a time
print("\nMerge 1: Merging the  key with the geographic information")

stud_1 = sg_final.merge(key, how='outer', indicator=True)
print(stud_1.head())

print("\nMerge 2: Merging in the school end times")

stud_2 = stud_1.drop("_merge", axis=1).merge(et, how='outer', indicator=True)
print(stud_2.head())

print("\nFinal merge: Merging in demographic information")

stud_final = stud_2.drop("_merge", axis=1).merge(sd_final, how='outer', left_on=[
    'School_Num'], right_on=['CAMPUS'], indicator=True)
print(stud_final.head())

##############################################
# IDENTIFYING AND DELETING UNMATCHED SCHOOLS #
##############################################

# I explained above the schools that will be deleted and why they're being deleted
# The geography information and the school end time information is from 2021-2022
# The demographic information is from 2020-2021
# Thus, there might be some discrepancies

# merging geographic information with the key
print("\nUnmatched from merge 1")
print(stud_1[stud_1._merge != "both"])

# merging in school end time information
print("\nUnmatched from merge 2")
print(stud_2[stud_2._merge != "both"])

# merging in demographic data
print("\nUnmatched from final merge")
print(stud_final[stud_final._merge != "both"])

# HERE WE START DELETING

# delete ENERGIZED FOR STEM ACADEMY SOUTHEAST MIDDLE because not in the HISD list and no demographic information available
stud_final = stud_final[stud_final.School_Nam !=
                        "ENERGIZED FOR STEM ACADEMY SOUTHEAST MIDDLE"]

# delete SOAR Academy since there is no school end time
stud_final = stud_final[stud_final['Campus Short Name'] != "SOAR Center"]

# delete JJAEP and DAEP because no demographic information (also alternative campuses)
stud_final = stud_final[~stud_final.School_Nam.isin(
    ["HARRIS CO J J A E P", "EL DAEP"])]

# Young Scholars Academy and Young Learners have been shut down/are a pre-K chain respectively (https://www.houstonisd.org/Page/32496), so we'll delete them
stud_final = stud_final[~stud_final.CAMPNAME.isin(
    ["YOUNG SCHOLARS ACADEMY FOR EXCELLE", "YOUNG LEARNERS"])]

#########################
# GOOGLE MAPS API + HMW #
#########################

# We need to find the place_id in order to give Google Maps accurate information location
# First, we'll do it for Hattie Mae White and then save this information locally
HMW_row = ['n/a', "Hattie Mae White", "4400 W 18th St, Houston, TX 77092", 'n/a',
           29.802759908899148, -95.45410037006431]
HMW_row.append(get_place_id(HMW_row, gm_api_key))

HMW_str = int_data + "HMW.csv"
pd.DataFrame(HMW_row).to_csv(HMW_str)

# Now, we'll convert the existing df to a dask dataframe and do the API calls from there
ddf = dd.from_pandas(stud_final, npartitions=8)
ddf['Place_Id'] = ddf.apply(lambda row: get_place_id(
    row, gm_api_key), axis=1, meta=('Place_Id', 'string'))

stud_final = ddf.compute()

##############################
# SAVE FINAL CLEANED DATASET #
##############################

# convert the endtimes column into a time columnn
stud_final['End Time'] = [dt.time() for dt in stud_final['End Time']]

print(stud_final.head())

stud_final_str = cleaned_data + "school_demo_geo.csv"
stud_final.to_csv(stud_final_str)
