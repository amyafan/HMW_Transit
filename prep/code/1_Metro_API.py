"""
Amy Fan 11-2021

This file uses the Metro API to find all the routes and route information from each school in HISD to Hattie Mae White: 

API documentation: https://api-portal.ridemetro.org/api-details#api=53f1db24ee3f340e949f15c7&operation=53f1db25ee3f34058c6da3cc

inputs:

    - school_info.csv (to get the latitudes and longitudes of all the schools)

outputs: 

    - metro_routes.csv

        for each school in school_info.csv, this file has all the routes to HMW 

    - metro_legs.csv 

        for each route in metro_routes, this file has all information for each leg of the trip 
"""

# import all the necessary libraries
import numpy as np
import pandas as pd
import requests
import time
import dateutil

# working directory
direc = "//Users//afan//Desktop//Misc//HMW_Transit//"

# API key. We will use this to connect to the API
key = "4e7902d1a2ae42df978fe20387049854"

# Hattie Mae White coordinates. I got these from Google Maps
HMW_lat = 29.802759908899148
HMW_long = -95.45410037006431

# read in the file with all the school information
sch_str = direc + "data//school_info.csv"
# select only the columns we want
columns = ['School Name', 'Latitude [Public School] 2019-20', 'Longitude [Public School] 2019-20',
           'School ID - NCES Assigned [Public School] Latest available year']
sch_info = pd.read_csv(sch_str, usecols=columns)
sch_info.columns = ['name', 'lat', 'long', 'nces_id']
prints(sch_info.head())

# Next, we'll pull out all the routes to Hattie Mae White for each school. For now, we'll store them in this list to concatenate
routelist = []

# loop through each school
for nrow in range(0, len(schools)):

    # get the latitude and longitude of the school
    s_lat = schools.iloc[nrow, 1]
    s_long = schools.iloc[nrow, 2]

    # get the trip information for that school
    trip = requests.get(
        f"https://api.ridemetro.org/data/CalculateItineraryByPoints?lat1={s_lat}&lon1={s_long}&lat2={HMW_lat}&lon2={HMW_long}&$orderby=EndTime&$expand=Legs&subscription-key={key}")

    # data wrangling to get the data into a nice DataFrame
    trip1 = pd.DataFrame(trip.json())
    trip_df = trip1['value'].apply(pd.Series)

    # add in columns for the school and school ID
    trip_df['school'] = schools.iloc[nrow, 0]
    trip_df['school_id'] = schools.iloc[nrow, 3]

    # add the dataframe to the list
    routelist.append(trip_df)
    print(f"Done with school #{nrow}: {schools.iloc[nrow, 0]}")

    # The API will return errors if I make too many requests at once. Thus, we will pause for 2 seconds after each loop
    time.sleep(2)

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
print("Shape of legs: ", legs.shape)

# drop all the rows that aren't actually associated with a trip
legs = legs[~np.isnan(legs['Ordinal'])]
print("Shape of legs after removing NaNs:", legs.shape)
print("")
print(f"Number of schools: {len(schools)}")
print(f"Number of routes: {len(routes)}")
print(f"Number of legs: {len(legs)}")

##########
# OUTPUT #
##########

routes.to_csv(direc + "data//metro_routes.csv")
legs.to_csv(direc + "data//metro_legs.csv")
