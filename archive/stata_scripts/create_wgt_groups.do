
********************
****PROGRAM INFO****
********************

*ATLANTA FED: LAST UPDATED 07/23/2019
*Authors: John Robertson & Ellyn Terry
*This program creates a data set of the various groups used for the Atlanta Fed's Wage Growth Tracker series that can be merged onto the CADRE dataset in Weighted_WGT.do & Unweighted_WGT.do
*This program should be run FIRST 

*NOTE: THIS VERSION OF THE PROGRAM IS FOR THE "Harmonized Variable and Longitudinally Matched [Atlanta Federal Reserve] (1976-Present) DOWNLOADED FROM CADRE: https://cps.kansascityfed.org/cps*/

**********************************
****SET PATHS AND READ IN DATA****
**********************************

set more off, permanently
display "$S_TIME"

global programspath "C:\WageGrowthTracker\programs"
global rawdatapath "C:\WageGrowthTracker\Data\rawdata" /*save cadre file here*/
global processeddatapath "C:\WageGrowthTracker\Data\processeddata" 

*********AFTER PATHS ARE SET ABOVE, RUN THE CODE BELOW************

*************************
****IMPORT CADRE DATA****
*************************
*https://cps.kansascityfed.org/cpsdata/full/CPS_harmonized_variable_longitudinally_matched_age16plus.dta.gz*/

use paidhrly82* metstat78 censusdiv76 lfdetail94 educ92 race76 employer89 occupation76 industry76 sameemployer94 sameactivities94 recession76 personid wageperhrclean82* wagegrowthtracker83 date age76 female76 if date >= mdy(1,1,1982) & age76>=16 using "${rawdatapath}\CPS_harmonized_variable_longitudinally_matched_age16plus.dta", clear

/* create some date variables */
cap gen _year = year(date)
cap gen month = month(date)
cap gen date_monthly = ym(_year,month(date))
cap format date_monthly %tm

*********************
****CREATE GROUPS****
*********************
/* Create 12 month lags of labor force detail, occupation, industry, sameemployer, same activity, and wage per hour*/
tsset personid date_monthly, monthly
gen lfdetail94_tm12 = l12.lfdetail94
gen occupation76_tm12 = l12.occupation76
gen industry76_tm12 = l12.industry76
gen sameemployer94_tm1 = l1.sameemployer94
gen sameemployer94_tm2 = l2.sameemployer94
gen sameactivities94_tm1 = l1.sameactivities94
gen sameactivities94_tm2 = l2.sameactivities94
gen wageperhrclean82_tm12= l12.wageperhrclean82

// 2 paid hourly groups 
gen hrlygroup = "Hourly" if paidhrly82==1 & paidhrly82_tm12==1 // hourly in both periods
replace hrlygroup = "Non-Hourly" if (paidhrly82==0 & paidhrly82_tm12==0) | (paidhrly82==0 & paidhrly82_tm12==1) | (paidhrly82==1 & paidhrly82_tm12==0)  //non-hourly or switched into or out of hourly
 
//New definition of job switcher based on ind/occ diff or responding no to sameemployer94 question or no to sameactivities94 question
// this is new definition used for website starting with Nov 2018 data.  
// Previously it was based on diff occ or ind or sameemployer94 !=1 or sameemployer94_tm1!=1 or sameemployer94_tm2!=1. But that includes sameemployer94==-1 (blank). There is also a discrete increase in the share of blanks starting in 2009
gen jstayergroup = "Job Stayer"
replace jstayergroup = "Job Switcher" if occupation76 !=occupation76_tm12 | industry76 !=industry76_tm12 | sameemployer94==2 | sameemployer94_tm1==2 | sameemployer94_tm2==2 | sameactivities94==2 | sameactivities94_tm1==2 | sameactivities94_tm2==2
drop sameemployer94_tm1 sameemployer94_tm2 sameactivities94_tm1 sameactivities94_tm2 industry76_tm12 occupation76_tm12 lfdetail94_tm12 

// 3 age76 groups
gen agegroup="16-24" if inrange(age76,16,24)
replace agegroup="25-54" if inrange(age76,25,54)
replace agegroup="55+" if age76>=55 & age76 ~=. 

// 2 gender groups 
gen gengroup="Male" if female76==0
replace gengroup="Female" if female76==1

// 2 sector groups 
gen secgroup="Goods" if inlist(industry76,1,2,3,13)
replace secgroup="Services" if inlist(industry76,4,5,6,7,8,9,10,11,12)

// 3 education groups (for 12 month avg cuts)
gen edgroup3="Nodegree" if inrange(educ92,1,3)
replace edgroup3="Bachelor+" if inrange(educ92,6,7)
replace edgroup3="Associates" if inrange(educ92,4,5)

// 2 education groups (for 3 month avg cuts) 
gen edgroup2="Nodegree" if inrange(educ92,1,3)
replace edgroup2="Degree" if inrange(educ92,4,7)

// 2 occupation groups 
gen occgroup="Professional" if inlist(occupation76,11,12,13)
replace occgroup="Nonprofessional" if inlist(occupation76,21,22,23,31,32,33,34) 

// 2 ft/pt groups
gen ftptgroup="Full-time" if lfdetail94==6|inrange(lfdetail94,8,20)
replace ftptgroup="Part-time" if lfdetail94==7|inrange(lfdetail94,21,32)

// low, mid and high skill occupations 
gen skillgroup = "Low" if inlist(occupation76,34,32,33)
replace skillgroup= "Middle" if inlist(occupation76,21,22,23,31)
replace skillgroup = "High" if inlist(occupation76,11,12,13)

// 7 industry groups 
gen indgroup = "Construction & Mining" if inlist(industry76,1,2)
replace indgroup = "Education & Health" if industry76 == 9
replace indgroup = "Finance and Business Services" if inlist(industry76,6,7,8)
replace indgroup = "Leisure & Hospitality" if inlist(industry76,10,11)
replace indgroup = "Manufacturing" if industry76 == 3
replace indgroup = "Public Administration" if industry76 == 12
replace indgroup = "Trade & Transportation" if inlist(industry76,4,5)

// 2 race groups (White Non-Hispanic, Other)
gen racegroup = "White" if race76 == 1
replace racegroup = "Nonwhite" if inrange(race76,2,3)

//2 metro groups
gen msagroup = "MSA" if metstat78==1 
replace msagroup = "NonMSA" if metstat78==2

// 9 census divisions
gen cdivgroup = "pac" if censusdiv76 == 1
replace cdivgroup = "esc" if censusdiv76 == 2
replace cdivgroup = "wsc" if censusdiv76 == 3
replace cdivgroup = "mnt" if censusdiv76 == 4
replace cdivgroup = "nen" if censusdiv76 == 5
replace cdivgroup = "sat" if censusdiv76 == 6
replace cdivgroup = "wnc" if censusdiv76 == 7
replace cdivgroup = "enc" if censusdiv76 == 8
replace cdivgroup = "mat" if censusdiv76 == 9

// create groups for average wage quartiles 
sort personid date_monthly
gen wage_hr_avg = (wageperhrclean82+wageperhrclean82_tm12)/2 if wagegrowthtracker83 !=. | inlist(_year,1995,1996,1985,1986)
// Compute distribution of average wage obs in WGT sample 
bysort date_monthly : egen p50_a = pctile(wage_hr_avg) , p(50)
bysort date_monthly : egen p25_a = pctile(wage_hr_avg) , p(25)
bysort date_monthly : egen p75_a = pctile(wage_hr_avg) , p(75)
// allocate WGT obs to each of the wage_hr_avg quartiles */
gen wagegroup = "1st" if wage_hr_avg < p25_a 
replace wagegroup = "2nd" if wage_hr_avg >= p25_a & wage_hr_avg<p50_a
replace wagegroup = "3rd" if wage_hr_avg >= p50_a & wage_hr_avg<p75_a
replace wagegroup = "4th" if wage_hr_avg >= p75_a & wage_hr_avg !=.

/*Only create groups for 16+*/
foreach var of varlist *group{
replace `var'="" if age76<16
}

keep date personid *group*
save "${rawdatapath}\WGT_groups.dta", replace
