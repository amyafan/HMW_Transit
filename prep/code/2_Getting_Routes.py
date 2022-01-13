"""
Amy Fan 1-2022

This file uses the Google Maps API to find all the routes and route information from each school in HISD to Hattie Mae White:

API documentation: https://developers.google.com/maps/documentation/directions/get-directions

I use Dask Delayed to run the API calls asychronously. This file takes less than 30 seconds to compute

inputs:

    - cleaned_data/school_demo_geo.csv

        cleaned from 1_Cleaning_Sch.py. Contains all the information about the schools and the Google Place ID information

outputs:

    - prep/int_data/{School_Num}_{School_Nam}_{arr/leav}.json

        two json files for each school:

            1) the routes leaving from when school starts
            2) the routes that arrive before the board meeting

NOTES ON TIME ZONES:

I am currently running this in Eastern Time Zone, which is one hour ahead of Houston (Central Time)

There are two times that are relevant here:
    - School End time

"""

# import all the necessary libraries
import numpy as np
import pandas as pd
import requests
import datetime
import time
import json
import pytz
from dask import delayed

# working directory
cleaned_data = "//Users//afan//Desktop//Misc//HMW_Transit//cleaned_data//"
int_data = "//Users//afan//Desktop//Misc//HMW_Transit//prep//int_data//"
json_loc = "//Users//afan//Desktop//Misc//HMW_Transit//prep//int_data//bus_routes//"

# API subscription keys for Google Maps
gm_api_key = "AIzaSyDkb0cS70Cmk5aQ-p4QZ_DccGHgGqc7eu4"

# Time Zone
cst = pytz.timezone('US/Central')

#########
# TIMER #
#########

# Just because I'm curious

start = time.perf_counter()

###############################
# THE MOST IMPORTANT FUNCTION #
###############################


def get_write_routes(row, board_meeting_start_time):

    # school number
    school_num = row[0]

    # school name
    school_name = row[1]
    school_name = ''.join(e for e in school_name if e.isalnum())

    # end time
    school_end_time = int(row[2].timestamp())

    # Google Place Id
    school_place_id = row[3]

    ########################################
    # API CALL 1: LEAVE AT SCHOOL END TIME #
    ########################################

    routes_1_url = f"https://maps.googleapis.com/maps/api/directions/json?departure_time={school_end_time}&alternatives=true&destination=place_id:{hmw_place_id}&mode=transit&origin=place_id:{school_place_id}&key={gm_api_key}"
    routes_1 = requests.get(routes_1_url)

    # save output as .json
    api_1_str = json_loc + school_num + "_" + school_name + "_leav.json"

    with open(api_1_str, 'w') as outfile:
        json.dump(routes_1.json(), outfile, sort_keys=True)

    ############################################
    # API CALL 2: ARRIVE AT BOARD MEETING TIME #
    ############################################

    routes_2_url = f"https://maps.googleapis.com/maps/api/directions/json?arrival_time={board_meeting_start_time}&alternatives=true&destination=place_id:{hmw_place_id}&mode=transit&origin=place_id:{school_place_id}&key={gm_api_key}"
    routes_2 = requests.get(routes_2_url)

    # save output as .json
    api_2_str = json_loc + school_num + "_" + school_name + "_arr.json"

    with open(api_2_str, 'w') as outfile:
        json.dump(routes_2.json(), outfile, sort_keys=True)

    ##########
    # OUTPUT #
    ##########

    # we need the function to return something so that we can compute it

    end = pd.Series(
        [school_num, school_name, routes_1.status_code, routes_2.status_code])

    return(end)

##################################
# BRINGING IN SCHOOL INFORMATION #
##################################


df_str = cleaned_data + "school_demo_geo.csv"
df_cols = ['School_Num', 'School_Nam', 'End Time', 'Place_Id']
df = pd.read_csv(df_str, usecols=df_cols, parse_dates=['End Time'])
df['End Time'] = df['End Time'] + datetime.timedelta(hours=6)

################################
# HATTIE MAE WHITE INFORMATION #
################################

# in this case, we really just need the place ID
hmw_str = int_data + "HMW.csv"
hmw = pd.read_csv(hmw_str, usecols=[1]).T
hmw_place_id = hmw.iloc[0, 6]

# board meeting time
bm_start_time = int((datetime.datetime.combine(
    datetime.datetime.now(cst).date(), datetime.time(17, 00)).timestamp()))

#################################
# PULLING THE ROUTE INFORMATION #
#################################

results = []

# loop through each school
for nrow in range(0, len(df)):

    a = delayed(get_write_routes)(df.iloc[nrow, :], bm_start_time)
    results.append(a)

# bring together all the concatenated results
total = delayed(pd.concat)(results, axis=1)

# compute everything (THIS IS WHEN ALL THE ACTUAL CODE RUNS)
final = total.compute()

# print all the final codes
print(final.T)

end = time.perf_counter()

###################
# CLOSING ACTIONS #
###################

# Let's create a readme text in the folder that explains when all these files were written
readme_str = json_loc + "README_" + \
    str(datetime.datetime.now().date()) + ".txt"

with open(readme_str, 'w') as f:

    f.write("The date in the name of this file is when all these file were writtens")

print(f"Everything happened in {end - start:0.4f} seconds!")
