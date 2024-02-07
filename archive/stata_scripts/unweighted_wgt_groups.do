
********************
****PROGRAM INFO****
********************

*ATLANTA FED: LAST UPDATED 07/24/2019
*Authors: John Robertson & Ellyn Terry
*This program creates 'unweighted' cuts of the wage growth tracker (cuts using the 3-mo moving avg data AND cuts produced from 12-mo moving avg data). 

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

use personid recession76 age76 wageperhrclean82* wagegrowthtracker83 date if date >= mdy(1,1,1982) & age76>=16 using "${rawdatapath}\CPS_harmonized_variable_longitudinally_matched_age16plus.dta", clear

// run "${programspath}\Create_WGT_groups_usingcadre.do" /* this program must be run first to create the groups */

/* merge on the WGT group data created by Create_WGT_groups_usingcadre.do */
merge 1:1 personid date using "${rawdatapath}\WGT_groups"

// generate various date variables
cap gen _year = year(date)
cap gen month = month(date)
cap gen date_monthly = ym(_year,month(date))
cap format date_monthly %tm
tsset personid date_monthly, monthly

// rename wagegrowthtracker to simplify
rename wagegrowthtracker83 WGT

// Create number of zero wage changes 
gen WGT_zer = 0 if WGT !=. 
replace WGT_zer = 100 if abs(WGT) < 0.5

*****************************************
****Create unweighted WGT time series****
*****************************************
// 1st, drop missing WGT observations from dataset except for 85-86 and 95-96 when ALL WGT obs are missing for some months due to Census masking of identifiers (need to keep those missing months for collapsed dataset)
keep if WGT !=. | inlist(_year,1995,1996,1985,1986) 

/* For unsmoothed version */
gen WGT_raw = WGT 
// average wage quartiles
gen WGT_q1 = WGT if wagegroup == "1st" 
gen WGT_q2 = WGT if wagegroup == "2nd"
gen WGT_q3 = WGT if wagegroup == "3rd"
gen WGT_q4 = WGT if wagegroup == "4th"
/* metro and non-metro */
gen WGT_ym = WGT if msagroup == "MSA" 
gen WGT_nm = WGT if msagroup=="NonMSA"
/* 3 age groups */
gen WGT_ya = WGT if agegroup=="16-24" 
gen WGT_pa = WGT if agegroup=="25-54"
gen WGT_oa = WGT if agegroup=="55+"
/* usually ft/usually pt */
gen WGT_ft = WGT if ftptgroup=="Full-time"
gen WGT_pt = WGT if ftptgroup=="Part-time"
/* male/female */
gen WGT_ms = WGT if gengroup=="Male"
gen WGT_ws = WGT if gengroup=="Female"
/* degree  education */
gen WGT_he = WGT if edgroup3=="Bachelor+" | edgroup3=="Associates"
/*three ed groups*/
gen WGT_de = WGT if edgroup3=="Bachelor+" 
gen WGT_ae = WGT if edgroup3=="Associates"
gen WGT_le = WGT if edgroup3=="Nodegree"
// skill 
gen WGT_lo = WGT if skillgroup=="Low"
gen WGT_mo = WGT if skillgroup=="Middle"
gen WGT_ho = WGT if skillgroup=="High"
/* service and goods industries */
gen WGT_si = WGT if secgroup=="Services"
gen WGT_gi = WGT if secgroup=="Goods"
/* White and other race */
gen WGT_wr = WGT if racegroup=="White"
gen WGT_or = WGT if racegroup=="Nonwhite"
/* job stayer/switcher */
gen WGT_jst = WGT if jstayergroup == "Job Stayer"
gen WGT_jsw = WGT if jstayergroup == "Job Switcher" 
// census division
gen WGT_pac = WGT if cdivgroup == "pac"
gen WGT_esc = WGT if cdivgroup == "esc"
gen WGT_wsc = WGT if cdivgroup == "wsc"
gen WGT_mnt = WGT if cdivgroup == "mnt"
gen WGT_nen = WGT if cdivgroup == "nen"
gen WGT_sat = WGT if cdivgroup == "sat"
gen WGT_wnc = WGT if cdivgroup == "wnc"
gen WGT_enc = WGT if cdivgroup == "enc"
gen WGT_mat = WGT if cdivgroup == "mat"
// industries
gen WGT_cmi = WGT if indgroup=="Construction & Mining"
gen WGT_ehi = WGT if indgroup=="Education & Health"
gen WGT_fpi = WGT if indgroup=="Finance and Business Services"
gen WGT_lhi = WGT if indgroup=="Leisure & Hospitality"
gen WGT_mni = WGT if indgroup=="Manufacturing"
gen WGT_pai = WGT if indgroup=="Public Administration"
gen WGT_tti = WGT if indgroup=="Trade & Transportation"
// hourly
gen WGT_yhr = WGT if hrlygroup=="Hourly"
gen WGT_nhr = WGT if hrlygroup=="Non-Hourly"

keep personid _year month date_monthly WGT* *group* recession76
save "${processeddatapath}\wage-growth-data_unweighted.dta", replace  /* these are the unweighted individual level WGT observations for the various cuts */

/* collapse WGT dataset into a time series */
#delimit ;
collapse (median) WGT WGT_raw WGT_ya WGT_pa WGT_oa WGT_ft WGT_pt WGT_ms WGT_ws WGT_he WGT_de WGT_ae WGT_le WGT_si WGT_gi WGT_wr WGT_or WGT_ho WGT_lo WGT_mo
WGT_jst WGT_jsw WGT_cmi WGT_ehi WGT_fpi WGT_lhi WGT_mni WGT_pai WGT_tti WGT_pac WGT_esc WGT_wsc WGT_mnt WGT_nen WGT_sat WGT_wnc WGT_enc WGT_mat WGT_ym WGT_nm WGT_q1 WGT_q2 WGT_q3 WGT_q4
(mean) WGT_avg=WGT ZERO=WGT_zer REC=recession76 (p25) WGT_p25=WGT (p75) WGT_p75=WGT WGT_yhr WGT_nhr
(count) WGT_n=WGT WGT_jst_n=WGT_jst WGT_jsw_n=WGT_jsw WGT_ft_n=WGT_ft WGT_pt_n=WGT_pt WGT_he_n=WGT_he WGT_si_n=WGT_si WGT_pa_n=WGT_pa WGT_ws_n=WGT_ws WGT_ms_n=WGT_ms , by(date_monthly _year month)
;
#delimit cr

/*Create date variable*/
gen date = mdy(month,1,_year)
format date %tdnn/dd/YY
save "${processeddatapath}\wage-growth-data_unweighted_collapsed.dta", replace  /* these are the unsmoothed versions of the various unweighted cuts */

******************************************************************************************
****Create smoothed versions of unweighted WGT time series rounded to 1 decimal place ****
******************************************************************************************
use "${processeddatapath}\wage-growth-data_unweighted_collapsed.dta", clear
/* 3mma overall series from 1983 and 3mma and 12mma cuts from 1997 */
keep if _year>=1983
tsset date_monthly, monthly
tostring WGT_raw, replace format(%9.1f) force
destring WGT_raw, replace
foreach var of varlist WGT WGT_ym WGT_nm WGT_gi WGT_si WGT_ft WGT_pt WGT_de WGT_he WGT_le WGT_ae WGT_ya WGT_oa WGT_pa WGT_ws WGT_ms WGT_jst WGT_jsw WGT_wr WGT_or WGT_cmi WGT_ehi WGT_fpi WGT_lhi WGT_mni WGT_pai WGT_tti WGT_ho WGT_lo WGT_mo WGT_nen WGT_mat WGT_enc WGT_wnc WGT_sat WGT_esc WGT_wsc WGT_mnt WGT_pac WGT_avg WGT_p25 WGT_p75 ZERO WGT_q1 WGT_q2 WGT_q3 WGT_q4 {
	tssmooth ma `var'_3mma = `var', window(2 1 0) replace   
	tostring `var'_3mma, replace format(%9.1f) force
	destring `var'_3mma, replace
	replace `var'_3mma=. if _n<=2
	replace `var'_3mma=. if (_year==1995 & inlist(month,6,7)) | (_year==1996 & inlist(month,9,10)) | (_year==1985 & inlist(month,7,8)) | (_year==1986 & inlist(month,10,11))
	tssmooth ma `var'_12mma = `var', window(11 1 0) replace
	tostring `var'_12mma, replace format(%9.1f) force
	destring `var'_12mma, replace
	replace `var'_12mma=. if _n<=11
	replace `var'_12mma=. if (_year==1995 & inrange(month,6,12)) | (_year==1996) | (_year==1997 & inrange(month,1,7)) | (_year==1985 & inrange(month,7,12)) | (_year==1986) | (_year==1987 & inrange(month,1,8))
}

list date WGT_raw WGT_3mma WGT_12mma WGT_n if _year > 1982

keep date _year month date_monthly WGT_raw *3mma *12mma REC
save "${processeddatapath}\wage-growth-data_unweighted_smoothed.dta", replace  /* these are the smoothed version of the various unweighted cuts */

