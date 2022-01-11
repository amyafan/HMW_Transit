"""
1-2022 Amy Fan 

Cleaning the demographic and geographic files that we'll be using in the future.

inputs (See the README for a more detailed description of the data):

    - student_demographics.csv

        This has all the student demographics

    - start_end_times.xlsx

        We read the second sheet to get each school's end time. 
    
    - school_geography.csv
        
        This gives the coordinates of each school. 

    - name_match_final.csv

        This is the key that matches the school names in start_end_times to school_geography

outputs: 

    - school_demo_geo.csv

        cleaned dataset

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

import numpy as np
import pandas as pd
import datetime

raw_data = "//Users//afan//Desktop//Misc//HMW_Transit//prep//raw_data//"
cleaned_data = "//Users//afan//Desktop//Misc//HMW_Transit//cleaned_data//"

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
key_str = raw_data + "name_match_final.csv"
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

##############################
# SAVE FINAL CLEANED DATASET #
##############################

print(stud_final.head())

stud_final_str = cleaned_data + "school_demo_geo.csv"
stud_final.to_csv(stud_final_str)
