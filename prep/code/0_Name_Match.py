"""
Amy Fan 1-2022 

Since the FOIA document from HISD and the TEA documents had different school names, I made this to match the school names properly. 

Notes:
        **** I WENT THROUGH AND MANUALLY EDITED ENTRIES AFTER RUNNING THIS. This matched about 85% of all the entries properly. 
		* Liberty High School has both day and night classes. Both are mapped to the Liberty High School classes
		* "ENERGIZED FOR STEM ACADEMY SOUTHEAST MIDDLE" was not included in the HISD FOIA request

Inputs (from the raw_data folder): 
- start_end_times.xlsx
- school_geography.csv

Outputs (also in the raw_data folder): 
- name_match_raw.csv

"""

# import libraries

import numpy as np
import pandas as pd
import openpyxl
from fuzzywuzzy import process

# working directory

direc = "//Users//afan//Desktop//Misc//HMW_Transit//"

# First, we'll read in the excel sheet with schools end times -- the information on all the schools is on the second sheet
endtimes_str = direc + "prep//raw_data//" + "start_end_times.xlsx"

# we only want the column with the names
endtimes = pd.read_excel(endtimes_str, 1, usecols=['Campus Short Name'])
print(endtimes.shape)

# now, we'll read in the school geographic information. This is from the Texas Education Agency and has different names
coord_str = direc + "prep//raw_data//" + "school_geography.csv"

coord = pd.read_csv(coord_str, usecols=['School_Nam'])
print(coord.shape)

# now, for each element in endtimes, we'll look for the closest element in coord. The function process.extractOne returns tuples, which is kind of annoying to deal with
endtimes_merge = endtimes.apply(
    lambda row: process.extractOne(row[0], coord.School_Nam), axis=1)

# add in the result as a column
endtimes['School_Nam'] = [item[0] for item in endtimes_merge]
print("Head of final dataframe:")
print(endtimes.head())

# We'll then write this to a csv
name_match_str = direc + "prep//raw_data//" + "name_match_raw.csv"
endtimes.to_csv(name_match_str)
