"""
Amy Fan 1-2022

This file uses the Metro API to find all the routes and route information from each school in HISD to Hattie Mae White: 

API documentation: https://api-portal.ridemetro.org/api-details#api=53f1db24ee3f340e949f15c7&operation=53f1db25ee3f34058c6da3cc

inputs:

    - school_demo_geo.csv

        cleaned from 1_Cleaning_Sch.py. Contains all the information about the schools 

outputs: 

    - routes_raw.csv

        
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
