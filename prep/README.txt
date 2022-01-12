About the datasets: 

raw_data:

	student_demographics.csv

		This data is from the Texas Academic Performance Reports (TAPR) from the Texas Education Agency: https://tea.texas.gov/texas-schools/accountability/academic-accountability/performance-reporting/texas-academic-performance-reports

			"The Texas Academic Performance Reports (TAPR) pull together a wide range of information on the performance of students in each school and district in Texas every year. Performance is shown disaggregated by student groups, including ethnicity and socioeconomic status. The reports also provide extensive information on school and district staff, programs, and student demographics."

		The data is downloaded for the 2020-2021 school year for Houston ISD: 
			https://rptsvr1.tea.texas.gov/perfreport/tapr/2021/xplore/CampByCountyDist.html

		The following categories and variables were selected here 
			Campus Name/District Name
			Student Membership: Counts/Percents
				This document explains the difference between membership and enrollment:https://rptsvr1.tea.texas.gov/perfreport/tapr/2021/glossary.pdf
				variable descriptions: https://rptsvr1.tea.texas.gov/perfreport/tapr/2021/xplore/cstud.html

	school_geography.csv
		
		This dataset is also from the Texas Education Agency and has campus data from the 2021-2022 school year. Here, I downloaded all the data for all schools in Houston ISD 

		https://schoolsdata2-93b5c-tea-texas.opendata.arcgis.com/datasets/7d8ba6b96ee14a6d882f6f35f7d27828_0/about

	start_end_times.xlsx 

		This is a spreadsheet of all the school start and end times.

		I received this through a public records request from Houston ISD with the following text: 
		
			"I would like to know the school end times for all schools in HISD for the 2021 - 2022 school year (including the schools that don't follow the standardized times). I would like this information in electronic format, ideally a spreadsheet." 

int_data: 

	This is data that was generated from the data in raw_data or manually created (but isn't quite cleaned_data)

	name_match_raw.csv and name_match_final.csv
		
		Since the FOIA document from HISD and the TEA documents had different school names, I made this to match the school names properly. I first used a fuzzy merge in notebook 2, and then manually checked the entries. 

		the file in code/0_Name_Match.py produces name_match_raw.csv. I then went through and manually adjusted to create name_match_final.csv

		Notes: 
			* Liberty High School has both day and night classes. Both are mapped to the Liberty High School classes
			* "ENERGIZED FOR STEM ACADEMY SOUTHEAST MIDDLE" was not included in the HISD FOIA request
