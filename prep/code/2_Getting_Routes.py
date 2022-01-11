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
    5, 00)) + datetime.timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ")


# function to wrangle the legs out of a dataframe:

get_legs()

#######################
# READING INPUT FILES #
#######################

# cleaned dataset
df_str = direc + "cleaned_data//" + "school_demo_geo.csv"

# relevant columns
df_cols = ['School_Num', 'School_Nam', 'X', 'Y', 'End Time']
df = pd.read_csv(endtimes_str, usecols=df_cols, parse_dates=['End Time'])

print(df.head())

#################################
# PULLING THE ROUTE INFORMATION #
#################################

# Next, we'll pull out all the routes to Hattie Mae White for each school. For now, we'll store them in this list to concatenate
routelist = []

# loop through each school
for nrow in range(0, 3):  # len(df)

    # school name
    s_name = df.iloc[nrow, 2]

    # school end time
    s_endtime = (df.iloc[nrow, 4] + timedelta(hours=6)
                 ).strftime("%Y-%m-%dT%H:%M:%SZ")

    # X and Y are longitude and latitude, respectively
    s_lat = df.iloc[nrow, 3]
    s_long = df.iloc[nrow, 2]

    ########################################
    # API CALL 1: LEAVE AT SCHOOL END TIME #
    ########################################

    # get the trip information for that school
    req_url = f"https://api.ridemetro.org/data/CalculateItineraryByPoints?lat1={s_lat}&lon1={s_long}&lat2={HMW_lat}&lon2={HMW_long}&startTime=datetime'{s_endtime}'&$orderby=EndTime&$expand=Legs&subscription-key={api_key}"
    trip = requests.get(req_url)

    print(trip.status_code)

    # data wrangling to get the data into a nice DataFrame
    trip_int = pd.DataFrame(trip.json())
    trip_df = trip_int['value'].apply(pd.Series)
    trip_df['school'] = s_name
    trip_df.head()

    # add the dataframe to the list
    routelist.append(trip_df)
    print(f"Done with school #{nrow}: {s_name}")

    # The API will return errors if I make too many requests at once. Thus, we will pause for 5 seconds after each loop
    time.sleep(5)

# After going through the loop, we can concatenate all the routelists into one dataframe
routes = pd.concat(routelist, ignore_index=True)

# convert the start and end times into datetime format
routes['AdjustedStartTime'] = routes.AdjustedStartTime.apply(
    dateutil.parser.isoparse)
routes['AdjustedEndTime'] = routes.AdjustedEndTime.apply(
    dateutil.parser.isoparse)

# create a variable for the length of each trip
routes['length'] = routes['AdjustedEndTime'] - routes['AdjustedStartTime']

# At this point, we have finished creating the dataframe that will be metro_routes.csv
# Now, we will work on creating metro_legs.csv

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
print("Shape of legs after removing NaNs:", legs.shape)
print("")
print(f"Number of schools: {len(df)}")
print(f"Number of routes: {len(routes)}")
print(f"Number of legs: {len(legs)}")

##########
# OUTPUT #
##########

routes.to_csv(direc + "prep//raw_data//" + "metro_routes_test.csv")
legs.to_csv(direc + "prep//raw_data//" + "metro_legs_test.csv")
