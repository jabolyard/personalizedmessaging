/*******************************************************************************
File name: disA_randomization
Purpose: Randomly assigns treatment and control status to eligible enrolled students
*******************************************************************************/
 // Topmatter
{
	// Set Up
	{
		clear all
		set more off, permanently
		set seed 12345
	}

 //Globals
	{
	global enrollment "R:\Proving_Ground\data\raw\Incoming Transfers\20180723 - disA 2019 School Enroll Data\fy19_school_enroll_pseudo_ids.csv"
	global analysis "R:\Proving_Ground\data\clean_disA\clean\disA_analysis_file.dta"
	global cepr_dw_stu_xwalk "R:\Proving_Ground\data\clean\CDW\APT\Prod\disA\CEPRID_Student_Mapping_Extract_valid_ids.csv"
	global cepr_dw_sch_xwalk "R:\Proving_Ground\data\clean\CDW\APT\Prod\disA\CEPRID_School_Mapping_Extract_valid_ids.csv"
	global clean_daily "R:\Proving_Ground\data\clean_disA\clean\disA_daily_absence_2018.dta"
	global school_year "R:\Proving_Ground\data\raw\Incoming Transfers\20180807 - disA 2018 EOY Data\Student_School_Year_FY2018_EOY_v2.csv"
	global schools "R:\Proving_Ground\data\clean_disA\clean\disA_schools.dta"
	global aux "R:\Proving_Ground\programs_new\aux_dos_analysis"
	}
	
// Global ouputs
	{
	global data "R:\Proving_Ground\data\pilots\disA_messaging_fall2019\targeted_messaging"
	}
}

//run randomization program 
do "$aux/pg_randomize.do"

//load the 2019 enrollment file from disA
import delimited using "$enrollment", clear varnames(1)

//check uniqueness of file and rename id variable for merge with CDW crosswalk
rename pseudo_id localstudentid
rename loc_id localschoolid
rename schl_yr school_year //this is just the preferred variable name at CEPR 
isid localstudentid 
count  //

//get student IDs for enrollment file from CDW crosswalk 
preserve
	import delimited "$cepr_dw_stu_xwalk", clear varnames(1)
	rename anonymizedstudentid sid
	tempfile cdw_student
	save `cdw_student'
restore

merge 1:1 localstudentid using `cdw_student', keep(1 3)
assert _merge == 3 
drop _merge 
isid sid
count //

//get school IDs for enrollment file from CDW crosswalk 
preserve
	import delimited using "$cepr_dw_sch_xwalk", clear varnames(1)
	rename anonymizedschoolid school_code 
	tempfile cdw_school
	save `cdw_school'
restore

merge m:1 localschoolid using `cdw_school', keep(1 3)
assert _merge == 3
drop _merge 

//get school names from schools file 
preserve 
use school_code school_name school_year region using "$schools" if school_year == 2018, clear
drop school_year
tempfile schools
save `schools'
restore 
merge m:1 school_code using `schools', keepusing(school_name region)
tab _merge
keep if _merge == 3
drop _merge 

//drop students who did not enroll and those from virtual schools
drop if withdrawal_cd == "[REMOVED]"
drop with*
count // 
gen virtual = region == "[REMOVED]"
tab virtual
drop if virtual
count //

//drop unnecessary grades and modify grade variable for merge with analysis file
keep if inlist(grade_level, "01", "08", "09")
destring grade_level, replace

//merge with school year file to drop disabled students
preserve
import delimited using "$school_year", clear varnames(1)
rename pseudo_id localstudentid
tempfile school_year
save `school_year'
restore
merge 1:1 localstudentid using `school_year', keepusing(stu_primary_ese_cd)
tab _merge
keep if _merge == 3
drop _merge 
count //

//drop disabled students 
drop if stu_primary_ese_cd == "[REMOVED]" // [REMOVED] is the code for disabled 
drop stu_primary_ese_cd
count //

//add suffix _19 to all variables to not confuse with 2018 analysis variables
renvars _all, suffix(_19)
rename sid_19 sid 

		
// bring in 2018 observations from the analysis file
preserve
use "$analysis" if school_year == 2018, clear
renvars _all, suffix(_18)
rename sid_18 sid 
tempfile analysis
save `analysis'
restore 

//merge analysis 18 file with enrollment 19 file 
merge 1:1 sid using `analysis'
tab _merge
keep if _merge == 3
drop _merge 
count // 

//drop schools that disA requested to be dropped
gen exclusion_flag = inlist(school_name_19, "[REMOVED]", "[REMOVED]", "[REMOVED]", ///
	"[REMOVED]", "[REMOVED]", "[REMOVED]", "[REMOVED]", "[REMOVED]", "[REMOVED]")
replace exclusion_flag = 1 if inlist(school_name_19, "[REMOVED]", "[REMOVED]", "[REMOVED]", "[REMOVED]", "[REMOVED]")
replace exclusion_flag = 1 if inlist(school_name_19, "[REMOVED]", "[REMOVED]", "[REMOVED]", "[REMOVED]", "[REMOVED]", "[REMOVED]", "[REMOVED]")
replace exclusion_flag = 1 if inlist(school_name_19, "[REMOVED]", "[REMOVED]", "[REMOVED]", "[REMOVED]")
tab school_name_19 if exclusion_flag
drop if exclusion_flag
count // 
isid sid 



//bring in period level absence data 
preserve 
use "$clean_daily", clear
isid sid date

//drop if all day absence is due to religious reasons
drop if abs_code  == [REMOVED]

//create dummy variables indicating period level absence (excluding field trips)
replace partialday_abs_periodabsencecode = "______________" if partialday_abs_periodabsencecode == ""
gen length = length(partialday_abs_periodabsencecode)
assert length == 14
foreach num of numlist 1/14 {
	gen pr_abs`num' = substr(partialday_abs_periodabsencecode, `num', 1)
	gen per_abs`num' = .
	replace per_abs`num' = 1 if inlist(pr_abs`num', "[REMOVED]")
	replace per_abs`num' = 0 if inlist(pr_abs`num', "[REMOVED]")
	replace per_abs`num' = 0 if inlist(pr_abs`num', "[REMOVED]")
	tab pr_abs`num' per_abs`num', mi
	assert !mi(per_abs`num' )
}

//create dummy variable for home bound students 
gen homebound = 0
foreach num of numlist 1/14 {
	replace homebound = 1 if pr_abs`num' == "[REMOVED]"
}	
bys sid: egen max_homebound = max(homebound)
drop homebound
rename max_homebound homebound 
tab homebound


//generate days absent based on 4 absent periods
gen periods_missed = per_abs1 + per_abs2 + per_abs3 + per_abs4 + per_abs5 + per_abs6 ///
	+ per_abs7 + per_abs8 + per_abs9 + per_abs10 +per_abs11 + per_abs12 + per_abs13 + per_abs14
assert !mi(periods_missed)
gen missed_day = periods_missed >= 4 | absent == 1 
collapse (sum) missed_day absent homebound, by(sid)
renvars _all, suffix(_18)
rename sid_18 sid
tempfile daily
save `daily'
restore
		
//merge with enrollment 19 and 18 analysis file 
merge 1:1 sid using `daily'
tab _merge 
keep if _merge == 3
drop _merge
count //

//drop homebound students 
drop if homebound > 0
count //

//drop students who do not meet 11 day threshold and those who were absent 100 or more days
gen eleven_days = missed_day_18 >= 11
keep if eleven_days
drop eleven_days
drop if missed_day_18 >= 100
count //

//randomly assign different treatment(s) and control to students within grades
pg_randomize treatment, idvar(sid) seed(12345) blockvars(region_19 school_code_19 grade_level_19) arms(6)

//check the randomization
tab grade_level_19 treatment 

//drop 9th graders from conservatory school
drop if inlist(school_name_19, "[REMOVED]", "[REMOVED]") & grade_level_19 == 9
count // 

//check to see that the days absent variable from the analysis file mostly corresponds to the sum from daily absence (minus religious days)
count if days_absent_18 != absent_18 // 

//generate string variable for interventions/treatment 
gen intervention = ""
replace intervention = "robocall" if treatment == 5
replace intervention = "email" if treatment == 4
replace intervention = "text" if treatment == 3
replace intervention = "backpack_letter" if treatment == 2
replace intervention = "mail_letter" if treatment == 1
replace intervention = "control" if treatment == 0

// confirm intervention variable corresponds with treatment variable
tab intervention treatment

//create days of learning lost variable
gen days_learning_lost_18 = missed_day_18 * 3

//save stata file 
save "$data/draft_targeted_disA_randomization_analysis.dta", replace
preserve 
//keep variables and observations needed to send to disA 
keep localstudentid localschoolid school_name_19 treatment intervention missed_day_18 days_learning_lost_18

//rename variables for the convenience of disA 
label var localstudentid_19 pseudo_id
label var localschoolid_19 loc_id
label var school_name_19 current_school
label var missed_day_18 missed_days_18
drop treatment
rename intervention treatment 

//order variables for the convenience of disA
order localstudentid localschoolid school_name_19 treatment missed_day_18 days_learning_lost_18


//export treatment file to excel 
export excel "$data/draft_targeted_disA_treatment_group_20180808.xls" if treatment != "control", firstrow(varlabels) sheet(Targeted, replace) 
//export control file to excel 
export excel "$data/draft_targeted_disA_control_group_20180808.xls" if treatment == "control", firstrow(varlabels) sheet(Targeted, replace)

restore 


