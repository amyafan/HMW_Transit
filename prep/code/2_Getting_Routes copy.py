"""
Amy Fan 1-2022

This file uses the Metro API to find all the routes and route information from each school in HISD to Hattie Mae White: 

API documentation: https://api-portal.ridemetro.org/api-details#api=53f1db24ee3f340e949f15c7&operation=53f1db25ee3f34058c6da3cc

inputs:

    - school_demo_geo.csv

        cleaned from 1_Cleaning_Sch.py. Contains all the information about the schools 

outputs: 

    - metro_routes.csv

        for each school in school_info.csv, this file has all the routes to HMW 

    - metro_legs.csv 

        for each route in metro_routes, this file has all information for each leg of the trip 
"""

# import all the necessary libraries
import numpy as np
import pandas as pd
import openpyxl
import requests
import time
import dateutil
import datetime

# working directory
direc = "//Users//afan//Desktop//Misc//HMW_Transit//"

# API keys. We will use this to connect to the API
api_key = "4e7902d1a2ae42df978fe20387049854"
api_key2 = "c45886cc797b47319bf15f4d7f84c3e4"

# Hattie Mae White coordinates. I got these from Google Maps
HMW_lat = 29.802759908899148
HMW_long = -95.45410037006431

# board meeting start time
bm_start_time = (datetime.datetime.combine(datetime.datetime.today(), datetime.time(
    17, 00)) + datetime.timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ")


######################
# RELEVANT FUNCTIONS #
######################

def append_api_output_to_routelist(t, rl, school_num, school_name):
    """
    wrangles output from the Metro API to add to the routelist (which will then be concatenated above)

    Also adds two columns with the school_num and the school_name 

    input: 
    _____

        - t: API output (.json)

            direct output from the Metro API

        - rl: list

            list with all the routes for each school

        - school_num: String

            School number (this is stored as a string)

        - school_name: String

            Name of the school

    output: 
    ______

        - rl_appended: list

            rl with the output from t appended, along with columns for the school number and the school name
    """

    # data wrangling to get the data into a nice DataFrame
    t_int = pd.DataFrame(t.json())
    t_df = t_int['value'].apply(pd.Series)
    t_df['School_Num'] = school_num
    t_df['School_Nam'] = school_name

    # add the dataframe to the list
    rl_appended = rl.append(t_df)

    # return the output
    return rl_appended

# function to clean and wrangle the legs out of a dataframe:


def get_route_leg_list(rl):
    """
    function that returns a list with two dataframes with the routes from the API and the legs from the API, respectively

    input: 
    _____

        rl: Pandas Dataframe

            Each row is a route that the Metro API returned that includes information about the legs and the trips 

    output: 
    ______

        route_leg: list 

            route_leg[0]: routes

            route_leg[1]: legs 

    """
    # First, concatenate all the routelists into one dataframe
    routes = pd.concat(rl, ignore_index=True)

    # convert the start and end times into datetime format
    routes['AdjustedStartTime'] = routes.AdjustedStartTime.apply(
        dateutil.parser.isoparse)
    routes['AdjustedEndTime'] = routes.AdjustedEndTime.apply(
        dateutil.parser.isoparse)

    # create a variable for the length of each trip
    routes['length'] = routes['AdjustedEndTime'] - routes['AdjustedStartTime']

    # At this point, we have finished creating the dataframe that will be route_leg[0]
    # Now, we will work on creating route_leg[1]

    # all the leg information was stored in the "legs" column of routes. Thus, we will unstack this by looping through each list
    all_legs = routes['Legs'].apply(pd.Series)

    # loop through all the routes and append all the legs to a list
    leglist = []
    for route in range(0, len(all_legs)):

        temp = all_legs.iloc[route, :].apply(pd.Series)
        leglist.append(temp)

    # concat all the legs in the list
    legs = pd.concat(leglist)

    # drop all the rows that aren't actually associated with a trip
    legs = legs[~np.isnan(legs['Ordinal'])]

    # Final steps
    # drop the "legs" column of routes, since it's a repeat and takes up memory
    routes = routes.drop('Legs', axis=1)

    # create list for output
    route_leg = [routes, legs]

    # print stuff
    print("Shape of legs after removing NaNs:", legs.shape)
    print("")
    print(f"Number of routes: {len(routes)}")
    print(f"Number of legs: {len(legs)}")

    return route_leg

#######################
# READING INPUT FILES #
#######################


# cleaned dataset
df_str = direc + "cleaned_data//" + "school_demo_geo.csv"

# relevant columns
df_cols = ['School_Num', 'School_Nam', 'X', 'Y', 'End Time']
df = pd.read_csv(df_str, usecols=df_cols, parse_dates=['End Time'])

print(df.head())

#################################
# PULLING THE ROUTE INFORMATION #
#################################

# Next, we'll pull out all the routes to Hattie Mae White for each school. For now, we'll store them in this list to concatenate
routelist_leave = []
routelist_arr = []

# loop through each school
for nrow in range(0, 3):  # len(df)

    # school number
    s_num = df.iloc[nrow, 0]

    # school name
    s_name = df.iloc[nrow, 1]

    # school end time
    s_endtime = (df.iloc[nrow, 4] + datetime.timedelta(hours=6)
                 ).strftime("%Y-%m-%dT%H:%M:%SZ")

    # X and Y are longitude and latitude, respectively
    s_lat = df.iloc[nrow, 3]
    s_long = df.iloc[nrow, 2]

    ########################################
    # API CALL 1: LEAVE AT SCHOOL END TIME #
    ########################################

    # get the trip information for that school
    req_url_1 = f"https://api.ridemetro.org/data/CalculateItineraryByPoints?lat1={s_lat}&lon1={s_long}&lat2={HMW_lat}&lon2={HMW_long}&startTime={s_endtime}&$orderby=EndTime&$expand=Legs&subscription-key={api_key}"
    trip_1 = requests.get(req_url_1)

    print("API CALL 1:", trip_1.status_code)

    # append output to list
    routelist_leave = append_api_output_to_routelist(
        trip_1, routelist_leave, s_num, s_name)

    ############################################
    # API CALL 2: ARRIVE AT BOARD MEETING TIME #
    ############################################

    # get the trip information for that school
    req_url_2 = f"https://api.ridemetro.org/data/CalculateItineraryArrivingAt?lat1={s_lat}&lon1={s_long}&lat2={HMW_lat}&lon2={HMW_long}&endTime=datetime'{bm_start_time}'&$orderby=EndTime&$expand=Legs&subscription-key={api_key}"
    trip_2 = requests.get(req_url_2)

    print("API CALL 2:", trip_2.status_code)

    # append output to list
    routelist_arr = append_api_output_to_routelist(
        trip_2, routelist_arr, s_num, s_name)

    print(f"Done with school #{nrow + 1}: {s_name}")

    # The API will return errors if I make too many requests at once. Thus, we will pause for 5 seconds after each loop
    time.sleep(5)

# concatenating routelist routelist

leave_route_leg = get_route_leg_list(routelist_leave)
arr_route_leg = get_route_leg_list(routelist_arr)

##########
# OUTPUT #
##########

leave_route_leg[0].to_csv(
    direc + "prep//raw_data//" + "metro_routes_leave.csv")
leave_route_leg[1].to_csv(direc + "prep//raw_data//" + "metro_legs_leave.csv")

arr_route_leg[0].to_csv(direc + "prep//raw_data//" + "metro_routes_arr.csv")
arr_route_leg[1].to_csv(direc + "prep//raw_data//" + "metro_legs_arr.csv")
