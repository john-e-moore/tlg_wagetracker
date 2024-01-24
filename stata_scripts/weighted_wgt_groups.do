
********************
****PROGRAM INFO****
********************

*ATLANTA FED: LAST UPDATED 01/06/2020
*Authors: John Robertson & Ellyn Terry
*This program creates the Weighted wage growth tracker series: 'overall weighted' and 'overall weighted, 1997 characteristics'

*NOTE: THIS VERSION OF THE PROGRAM IS FOR THE "Harmonized Variable and Longitudinally Matched [Atlanta Federal Reserve] (1976-Present) DOWNLOADED FROM CADRE: https://cps.kansascityfed.org/cps*/

**********************************
****SET PATHS AND READ IN DATA****
**********************************

set more off, permanently
display "$S_TIME"

global programspath "C:\WageGrowthTracker\Programs"  /* store programs here */
global rawdatapath "C:\WageGrowthTracker\Data\rawdata" /*save cadre data file and group dataset here*/
global processeddatapath "C:\WageGrowthTracker\Data\processeddata" /* save ouput here */


*********AFTER PATHS ARE SET ABOVE, RUN THE CODE BELOW************

*************************
****IMPORT CADRE DATA****
*************************
*https://cps.kansascityfed.org/cpsdata/full/CPS_harmonized_variable_longitudinally_matched_age16plus.dta.gz*/

use personid weightern82 recession76 age76 mlr76 employer89 occupation76 wageperhrclean82* wagegrowthtracker83 date if date >= mdy(1,1,1982) & age76>=16 using "${rawdatapath}\CPS_harmonized_variable_longitudinally_matched_age16plus.dta", clear

// run "${programspath}\Create_WGT_groups_usingcadre.do" /* this program must be run first to create the groups */

/* merge on the WGT group data created by Create_WGT_groups_usingcadre.do */
merge 1:1 personid date using "${rawdatapath}\WGT_groups"

// generate various date variables
cap gen _year = year(date)
cap gen month = month(date)
cap gen date_monthly = ym(_year,month(date))
cap format date_monthly %tm
tsset personid date_monthly, monthly

***********************
***RESTRICT DATASET****
***********************

// Restrict to nonAg employed Wage&Salary earners from 1997 onwards
keep if inlist(mlr76,1,2) & inrange(employer89,1,2) & inrange(occupation76,11,34) & _year >=1997
// drop observations that are missing in each group
drop if missing(agegroup)|missing(edgroup2)|missing(gengroup)|missing(secgroup)|missing(occgroup)

************************************
****DEFINE ELEMENTS TO WEIGHT BY****
************************************

local grouplist_demo = "agegroup edgroup2 gengroup"
local grouplist_job = "secgroup occgroup"

*********************
***CREATE WEIGHTS****
*********************
/*Count the # of W&S earners in each group in each month*/
bysort date_monthly: egen obsempinmonth=total(weightern82) 
bysort date_monthly `grouplist_demo' `grouplist_job': egen obsingroup_demojob_empinmo= total(weightern82)  

/*Share of W&S earners in each group in each month*/
bysort `grouplist_demo' `grouplist_job': gen distribution_demojob_empinmo= obsingroup_demojob_empinmo/obsempinmonth
gen baseyr = 1997

/*Count the # of W&S earners in each group in base year*/
bysort _year: egen obsinbaseyr=total(weightern82) if _year==baseyr
bysort _year `grouplist_demo' `grouplist_job': egen obsingroup_demojob_baseyr= total(weightern82)  if _year==baseyr

/*Share of W&S earners in each group in base year*/
bysort `grouplist_demo' `grouplist_job': egen distribution_demojob_baseyr= max(obsingroup_demojob_baseyr/obsinbaseyr)

/*after share of W&S workers in each group created, drop anyone w/ missing wage growth obs*/
drop if wagegrowthtracker83==. 

/*count the # of WGT ppl in each group in each month */
bysort date_monthly: egen obsinmonth=total(weightern82) 
bysort date_monthly `grouplist_demo' `grouplist_job': egen obsingroup_demojob_month= total(weightern82) 

/*weight 1997+ observations by base year shares: NOTE Total of weightern82_pct will equal 1 in every year. Total of fixedweight_* will equal 1 in every MONTH*/
egen obsempinmonth2=max(obsempinmonth)
drop obsempinmonth 
rename obsempinmonth2 obsempinmonth

/*share of the group each person represents multiplied by the share distribution of the group in the month's population = the share of the population for the month that person represents*/
gen weight_demojob=obsinmonth*(weightern82/obsingroup_demojob_month)*distribution_demojob_empinmo

/*weight 1997+ observations by base year shares: NOTE Total of weightern82_pct will equal 1 in every year. Total of fixedweight_* will equal 1 in every MONTH*/
egen obsinbaseyr2=max(obsinbaseyr)
drop obsinbaseyr 
rename obsinbaseyr2 obsinbaseyr

/*share of the demo_job group that person represents multiplied by the share distribution of the age group in the base year population= the share of the populatoin for the year that person represents*/
gen weight_97_demojob=obsinmonth*(weightern82/obsingroup_demojob_month)*distribution_demojob_baseyr

/*VERIFY WEIGHT TOTALS!!!!!! Once the percentages are multipled by the obsinmonth we should have the same sum of weights across all weight types)*/
table date_monthly,c(sum weightern82 sum weight_demojob sum weight_97_demojob)

/* Create number of zero wage changes */
gen wagegrowth_zero = 0 
replace wagegrowth_zero = 100 if abs(wagegrowthtracker83) < 0.5

keep date personid date_monthly month _year weight_demojob weight_97_demojob wagegrowthtracker83 wagegrowth_zero
save "${processeddatapath}\wage-growth-data_weighted.dta", replace /* these are the weights for the individual level WGT observations */

**************************************
***Export Weighted WGT time series****
**************************************

cap program drop create_weighted
program define create_weighted
	
	use "${processeddatapath}\wage-growth-data_weighted.dta", clear
	rename wagegrowthtracker83 WGT
	rename wagegrowth_zero WGT_zero
	collapse (median) WGT_`2'=WGT (count) count=personid [pweight=`1'], by(date_monthly month _year)
	save "${processeddatapath}\wage-growth-data_`2'_collapsed.dta", replace  /* these are the unsmoothed versions of the weighted median WGT time series */
	tostring WGT_`2', replace format(%9.1f) force
    destring WGT_`2', replace
	tsset date_monthly, monthly
	tssmooth ma WGT_`2'_3mma = WGT_`2', window(2 1 0) replace   
	tostring WGT_`2'_3mma, replace format(%9.1f) force
	destring WGT_`2'_3mma, replace
	replace WGT_`2'_3mma=. if _n<=2
	replace WGT_`2'_3mma=. if (_year==1995 & inlist(month,6,7)) | (_year==1996 & inlist(month,9,10)) 
	tssmooth ma WGT_`2'_12mma = WGT_`2', window(11 1 0) replace   
	tostring WGT_`2'_12mma, replace format(%9.1f) force
	destring WGT_`2'_12mma, replace
	replace WGT_`2'_12mma=. if _n<=11
    replace WGT_`2'_12mma=. if (_year==1995 & inrange(month,6,12)) | (_year==1996) | (_year==1997 & inrange(month,1,7)) 
	list date WGT_`2'_3mma WGT_`2'_12mma if _year >= 1997
	keep date _year month date_monthly WGT* 
	save "${processeddatapath}\wage-growth-data_`2'_smoothed.dta", replace /* these are the smoothed versions of the weighted median WGT time series */

end

//Run program above plugging in the following values for `1', `2' 
create_weighted "weight_demojob" "weighted"  // weighted to employed pop in current month 
create_weighted "weight_97_demojob" "weighted_97"  // weighted to employed pop in 1997
