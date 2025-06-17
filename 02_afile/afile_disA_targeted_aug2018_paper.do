/*******************************************************************************
	File: 				afile_disA_targeted_aug2018.do
	Purpose: 			Produce analysis file for district A targeted pilot Fall 2018
		
	Inputs:
		1) Analysis file
		2) Randomization file from pilot
		3) Clean Daily Attendance data
	Outputs:
		1) Analysis file for pilot
	Steps:
		
*******************************************************************************/

// Topmatter
{
	// Set Up
	{
		clear all
		cap log close
		set more off, permanently
		set seed 12345
		set matsize 11000
		//macro drop _all
	}
	
	/*
	// Globals - Inputs
	{
	*/
	global randomization	"${inputs}/disA/final_targeted_disA_randomization_analysis.dta" 
		global start_date 	29aug2018
		global end_date 	04feb2019
		global pre_start 	13aug2018
		global pre_end		28aug2018
	// Global - Switches
		global new_daily 1
		global get_weekly 1
		global reg 0
	
}

//log the process
log using "${logs}/disA/afile_disA_targeted_aug2018.log", replace 


//Load clean daily absence file
if $new_daily {
	use sid school_code school_year date absent using "${inputs}\disA\disA_dw_daily_absences.dta" if school_year == 2019, clear

	//temporarily save the file 
	drop if date == mdy(8, 28, 2018) | date == mdy(9, 14, 2018)
	gisid sid school_code date
	tempfile daily_data
	save `daily_data'

	//load the raw daily to get period level absence
	import delimited using "${inputs}\disA\studentdailyattendance_FY2019", clear varnames(1)

	//fix the date
	gen proper_date = date(date, "YMD")
	format proper_date %td
	order proper_date, after(date)
	drop date
	rename proper_date date
	drop if date == mdy(8, 28, 2018) | date == mdy(9, 14, 2018)

	//calculate missed day with period absence
	foreach num of numlist 1/10 {
		gen per_abs`num' = .
		replace per_abs`num' = 1 if inlist(pr_abs`num', "[REMOVED]")
		replace per_abs`num' = 0 if inlist(pr_abs`num', "[REMOVED]")
		replace per_abs`num' = 0 if inlist(pr_abs`num', "[REMOVED]")
		assert !mi(per_abs`num' )
	}

	gen periods_missed = per_abs1 + per_abs2 + per_abs3 + per_abs4 + per_abs5 + per_abs6 ///
		+ per_abs7 + per_abs8 + per_abs9 + per_abs10
	assert !mi(periods_missed)

	gen missed_day = periods_missed >= 4
	tab alldayabsencecode missed_day

	//rename the id variable to prepare for merge 
	keep stud_pseudo_id schoolid date missed_day

	//get ids from crosswalk
	pg_xwalk, pid(2) student_id(stud_pseudo_id)
	pg_xwalk, pid(2) school_id(schoolid)

	//temporarily save the file 
	tempfile missed_day_data
	save `missed_day_data'

	//merge the missed day back ro the clean daily
	merge 1:1 sid school_code date using `daily_data'
	tab _merge
	dis "Number of students with missed day on the raw daily who are not in clean daily:"
	count if _merge != 3
	keep if inlist(_merge, 2, 3) 
	replace missed_day = 0 if mi(missed_day)
	drop _merge

	//fix the absent variable to include missed days
	replace absent = 1 if absent == 0 & missed_day == 1

	//temporarily save the file
	save "${inputs}/disA/daily_formatted.dta", replace
}


//Load the randomization file
{
	use "$randomization", clear 
	keep sid grade_level_19 school_code_19 region_19 school_name_19 ///
			treatment intervention _rand* bin ///
			male_18 sped_18 ell_18 frpl_18 race_18 ///
			days_absent_18 abs_rate_18 days_enrolled_18 chronic_absent_18
		* blocking done by region, school, grade
	
	// add school level variable for expected school (getting from afile)
	gen school_year = 2019
	rename school_code_19 school_code
	preserve
		use school_code school_level school_year using "${inputs}\disA\disA_dw_analysis.dta" if school_year == 2019, clear
		duplicates drop 
		tempfile school_level
		save `school_level'
	restore
	merge m:1 school_code school_year using `school_level', assert(2 3) keep(3) nogen
	rename school_level exp_school_level 
	// renames
	rename (school_code grade_level_19 school_name_19 region_19) ///
			(exp_school exp_grade exp_school_name region)
	rename intervention treatment_name	
	
	// merge on analysis file
	merge 1:1 sid school_year using "${inputs}\disA\disA_dw_analysis.dta", keep(1 3) keepusing(school_code grade_level)
	assert school_year == 2019
	tab _merge
		* 642 people who were randomized aren't in the 2019 afile
	gen not_in_2019_afile = _merge == 1
	drop _merge	
	
	//temporarily save the file 
	isid sid
	tempfile randomization
	save `randomization'
}

//Get Weekly
if $get_weekly {
	// set up 
		//load 2019 daily data
		use "${inputs}/disA/daily_formatted.dta" if school_year == 2019, clear
		
		//merge on treatment data
		merge m:1 sid using `randomization', keep(3) nogen
		
	// post treatment weekly
	preserve
		pg_get_weekly, startdate_dmy("$start_date") enddate_dmy("$end_date") save("${interim}\disA\weekly_cumulative_adj_abs.dta")
	restore
		
	// pre-treatment weekly
	preserve
		pg_get_weekly, startdate_dmy("$pre_start") enddate_dmy("$pre_end") save("${interim}\disA\pre_treat_adj_abs.dta") pre_treat
	restore	
}


// merge weekly vars
use `randomization', clear
cap drop _merge  
merge 1:1 sid using "${interim}\disA\weekly_cumulative_adj_abs.dta"
	tab _merge
	gen attrit_pre_treat = (_merge==1)
	dis "Number of students who were randomized but do not have weekly abs rates"
	count if _merge == 1
merge 1:1 sid using "${interim}\disA\pre_treat_adj_abs.dta", gen(_merge_pre)
	tab _merge_pre
	tab _merge _merge_pre 
	drop _m*

	// check whether pre treat attrition differs by treatment assignment
	tab treatment attrit_pre_treat, row

	// add missing indicator for pre_treat - if no missings set to 1
	gen mi_pre_treat = mi(pre_treat_adj_abs_rate)
	foreach var of varlist pre_treat* {
		di "`var'"
		replace `var' = 0 if mi_pre_treat == 1
	}


/*
// check missingness among students who matriculated
foreach var of varlist * {
	cap assert !mi(`var') if attrit_pre_treat==0
	if _rc != 0 {
		di "`var' is sometimes missing: "
		tempvar `var'_missing
		gen ``var'_missing' = mi(`var') if attrit_pre_treat==0
		label var ``var'_missing' "`var' Missing"
		tab ``var'_missing'
	}
}
*/

// calculate school and school-grade averages of 2018 absence rates
{
	preserve
		use "${inputs}\disA/disA_dw_analysis.dta" if school_year == 2018, clear
		drop if days_enrolled < 20
		
		bysort grade_level : egen mean_gl_abs_rate = mean(abs_rate)
		gen adj_abs_rate = abs_rate / mean_gl_abs_rate
		bys school_code : egen float sch_prior_adj_abs_rate = mean(adj_abs_rate)
		bys school_code : egen sch_prior_avg_days_absent = mean(days_absent)
		bys school_code grade_level : egen float schgr_prior_adj_abs_rate = mean(adj_abs_rate)
		bys school_code grade_level : egen schgr_prior_avg_days_absent = mean(days_absent)
		
		gcollapse (mean) schgr_prior* sch_prior* (count) N = sid, by(school_code grade_level)
		summ N
		di "The minimum school-grade size is `r(min)' and the maximum school-grade size is `r(max)'"
		drop N
		
		// rename to merge based on expected school and grade
		bysort school_code : egen temp_sch = max(sch_prior_adj_abs_rate)
		replace sch_prior_adj_abs_rate = temp_sch
		rename grade_level exp_grade
		rename school_code exp_school
		tempfile sch_temp
		save `sch_temp'
		
		// save grade and school-grade priors
		keep if inlist(exp_grade,1, 8, 9)
		cap drop sch_prior_adj_abs_rate
		tempfile gr_schgr_avg_prior_adj_rates
		save `gr_schgr_avg_prior_adj_rates'
		
		// save school priors
		use `sch_temp', clear
		bysort exp_school : keep if _n==1
		keep exp_school sch_prior*
		tempfile sch_avg_prior_adj_rates
		save `sch_avg_prior_adj_rates'
	restore

	merge m:1 exp_school exp_grade using `gr_schgr_avg_prior_adj_rates', keep(1 3)
	dis "PAY ATTENTION HERE:"
	tab _merge
	keep if _merge == 3
	drop _merge
	merge m:1 exp_school using `sch_avg_prior_adj_rates', keep(1 3)
	tab _merge 
	drop _merge

	// dummy out missings
	gen mi_schgr_prior = mi(schgr_prior_adj_abs_rate)
	replace schgr_prior_adj_abs_rate = 0 if mi_schgr_prior==1
	replace schgr_prior_avg_days_absent = 0 if mi_schgr_prior == 1
	gen mi_sch_prior = mi(sch_prior_adj_abs_rate)
	replace sch_prior_adj_abs_rate = 0 if mi_sch_prior==1	
	replace sch_prior_avg_days_absent = 0 if mi_sch_prior == 1

	assert !mi(sid)
	tempfile data
	save `data'
}


// Load analysis file
use "${inputs}\disA\disA_dw_analysis.dta" if school_year == 2018, clear
keep if school_year==2018
keep sid grade_level
rename grade_level prior_grade_level 

	// Get prior abs-rates adjusted by mean of last-year's observed grade-level
	merge 1:1 sid using `data', keep(2 3) nogen
	assert mi(abs_rate_18) if mi(prior_grade_level) 
	rename (abs_rate_18 days_absent_18 days_enrolled_18 chronic_absent_18) ///
			(prior_abs_rate prior_days_absent prior_days_enrolled prior_chronic_absent)
	bysort prior_grade_level : egen avg_prior_abs_rate = mean(prior_abs_rate)
	gen prior_adj_abs_rate = prior_abs_rate/avg_prior_abs_rate
	
	// treating student enrolled < 20 days prior year as missing prior absences
	replace prior_adj_abs_rate = . if prior_days_enrolled < 20
	replace prior_days_absent = . if prior_days_enrolled < 20
	replace prior_abs_rate = . if prior_days_enrolled < 20
	
	// dummy out missings
	gen mi_prior = mi(prior_adj_abs_rate) 
	assert mi_prior == 0
	*replace prior_adj_abs_rate = 0 if mi_prior==1
	*replace prior_days_absent = 0 if mi_prior==1
	*replace prior_abs_rate = 0 if mi_prior==1
	*replace prior_days_enrolled = 0 if mi_prior==1
		* eligibility was based on prior year absences so don't expect missings

// Sort treatment into arms
gen arm = 1 + treatment
replace treatment = treatment >= 1

// mark attrition
gen attrit_post_treat = (mi(adj_abs_rate) & attrit_pre_treat == 0)



//tab attrition variables 
gen attrit_pre_or_post_treat = attrit_pre_treat + attrit_post_treat
tab1 attrit_pre_treat attrit_post_treat attrit_pre_or_post_treat
tab arm attrit_pre_or_post_treat


// Get data ready for export 
{
	// make sure to put pid on file
	cap gen pid = 2
	replace pid = 2 if mi(pid)
	
	// create pr_prior vars
	clonevar pr_prior_days_absent = prior_days_absent
	clonevar pr_prior_days_enrolled = prior_days_enrolled
	clonevar pr_prior_adj_abs_rate = prior_adj_abs_rate
	gen mi_pr_prior = mi(pr_prior_days_absent)
	cap drop prior_abs_rate
	
	//label the arms
	label define arm_name 1 "Control" 2 "Mail" 3 "Backpack" 4 "Text" 5 "E-mail" 6 "Robocall"
	label values arm arm_name

	//create dummy variables for arms
	levelsof treatment_name, local(levels)
	foreach treat of local levels {
		gen `treat' = (treatment_name == "`treat'")
		tab treatment_name `treat'
	}

	//make region variable numeric in order for programs below to run
	gen region_num = 0
	replace region_num = 1 if regexm(region, "[REMOVED]")
	replace region_num = 2 if regexm(region, "[REMOVED]")
	replace region_num = 3 if regexm(region, "[REMOVED]")
	replace region_num = 4 if regexm(region, "[REMOVED]")

	//label the regions
	label define region 0 "[REMOVED]" 1 "[REMOVED]" 2 "[REMOVED]" 3 "[REMOVED]" 4 "[REMOVED]"
	label values region_num region

	// consolidate race vars
	tab race
	recode race (2 4 6 = 7)
	
	 //create and rename necessary variables
	gen pilot_type = "targeted"
	clonevar xmale = male_18
	clonevar xrace = race_18
	clonevar xfrpl = frpl_18 
	clonevar xell = ell_18
	clonevar xsped = sped_18
	gen round = 1
	egen block = group(region exp_school exp_grade)
	egen schgr = group(exp_school exp_grade)
	clonevar rand_unit = sid	
	gen rand_level = 1 // student 	
		
	// created block vars	
	clonevar block_stu_region = region
	clonevar block_stu_exp_school = exp_school
	clonevar block_stu_exp_grade = exp_grade
	pg_randgroup block_stu_region block_stu_exp_school block_stu_exp_grade, bin(bin)	
		
	// label variables 
	lab var exp_school "School at time of randomization (August 2018)"
	lab var exp_grade "Grade level at time of randomization (August 2018)"
	lab var xrace "Race at time of randomization (August 2018)"	
	lab var xfrpl "FRPL status at time of randomization (August 2018)"
	lab var xell "ELL status at time of randomization (August 2018)"
	lab var xsped "SPED status at time of randomization (August 2018)"
	lab var grade_level "Grade level in SY 2019"
	lab var school_code "School code in SY 2019"
	lab var prior_grade_level "Grade level in SY 2018"
	lab var prior_chronic_absent "Chronic absent status in SY 2018"
	lab var prior_days_absent "Days absent in SY 2018"
	lab var prior_days_enrolled "Days enrolled in SY 2018"
	lab var prior_adj_abs_rate "Adjusted absence rate in SY 2018"
	lab var mi_prior "Missing prior adjusted absence rate"
	lab var schgr_prior_adj "School-grade adjusted absence rate in SY 2018"
	lab var schgr_prior_avg "School-grade average absences in SY 2018"
	lab var mi_schgr_prior "Missing prior school-grade adjusted absence rate"
	lab var sch_prior_adj "School adjusted absence rate in SY 2018"
	lab var sch_prior_avg "School average absences in SY 2018"
	lab var mi_sch_prior "Missing prior school adjusted absence rate"
	lab var adj_abs_rate "Adjusted absence rate in full post-treat period"
	lab var cum_abs_rate "Unadjusted absence rate in full post-treat period"
	lab var cum_days_absent "Days absent in full post-treat period"
	lab var cum_days_enrolled "Days enrolled in full post-treat period"
	
	// model cehcks
	assert !mi(pre_treat_adj_abs_rate)
	assert !missing(prior_days_absent) if mi_prior == 0
	assert prior_days_absent == 0 if mi_prior == 1
	assert !missing(pre_treat_days_absent) if mi_pre_treat == 0
	assert pre_treat_days_absent == 0 if mi_pre_treat == 1
	assert !mi(pre_treat_adj_abs_rate)
	
	// check missingness on vars
	foreach var of varlist pid sid school_year exp* x* treat* arm {
		assert !mi(`var') 
	}
	
}


//save full file
cap drop _*
save "${outputs}/disA/disA_targeted_aug2018_full_data.dta", replace

//load the full file 
use "${outputs}/disA/disA_targeted_aug2018_full_data.dta", clear 

//Drop attriters
keep if attrit_pre_treat==0 //& attrit_post_treat==0 [REMOVED] changed 2/27/20
drop attrit_pre_treat attrit_post_treat has_all_weeks 


//drop observations from special schools (they were blocked seperately)
drop if region_num == 0
//assert `r(N_drop)' == 52

//temp save
tempfile model_data
save `model_data'

*/
//save afile for models
pg_check_data, filetype(model)
if "`r(num_errors)'" == "0" {
	save "${outputs}/disA/disA_targeted_aug2018_model_data.dta", replace
 }
 else {
	exit
 }

//close the log 
cap log close
