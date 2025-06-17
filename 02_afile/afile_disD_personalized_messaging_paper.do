/******************************************************************************

	File: 						afile_disD_messaging_oct2019.do
	Purpose: 					Produce analysis file for disD messaging pilot
								
	Inputs:
		1) disD randomization file 
		2) Analysis file 
		3) Clean daily data
	
	Outputs:
		1) Full model data
		2) Model data
		
	Notes: - Removed non-public schools students due to inaccurate calendar info on 12/09/2019
		   Updated to keep post-treatment attriters
		   - Added end date on 07/06/2020
		

*******************************************************************************/

// Topmatter
{
	// Set Up
	clear all
	//macro drop _all
	cap log close
	set type double 

	// Set date
	local currdate = string(date(c(current_date), "DMY"), "%tdCCYYNNDD")
	global today = `currdate'
	di "$today"

	
	// Log
	log using "${logs}/disD/disD_messaging_oct2019_afile_log_${today}.log", replace 
	
	// Files paths 

		// day-month-year
		global start_date 	"06oct2019" 
		global end_date 	"13mar2020" 
		global pre_start 	"19aug2019"
		global pre_end		"05oct2019"
	
	
	
	// Switches
	global get_weekly 0
	global extras 1
	
}

// Load randomization file
use "$inputs/disD\FINAL_disD_messaging_student_20191002.dta", clear
	
	// xfrpl var not created in randomization file
	gen xfrpl = frpl 
	
	// Drop the ten students from non-public schools
	drop if exp_school==[REMOVED]
	// Make tempfile
	tempfile updated_trt
	save `updated_trt'
	
// calculate school and school grade average prior absences
// calculate school and school-grade averages of 2019 absence rates
{
	preserve
		use "${inputs}\disD\disD_dw_analysis.dta" if school_year == 2019, clear
		drop if days_enrolled < 20
		
		bysort grade_level : egen mean_gl_abs_rate = mean(abs_rate)
		gen adj_abs_rate = abs_rate / mean_gl_abs_rate
		bysort school_code : egen sch_prior_adj_abs_rate = mean(adj_abs_rate)
		bysort school_code : egen sch_prior_avg_days_absent = mean(days_absent)
		bysort school_code grade_level : egen schgr_prior_adj_abs_rate = mean(adj_abs_rate)
		bysort school_code grade_level : egen schgr_prior_avg_days_absent = mean(days_absent)
		
		collapse (mean) schgr_prior* sch_prior*, by(school_code grade_level)
		
		// rename to merge based on expected school and grade
		bysort school_code : egen temp_sch = max(sch_prior_adj_abs_rate)
		replace sch_prior_adj_abs_rate = temp_sch
		rename grade_level exp_grade
		rename school_code exp_school
		tempfile sch_temp
		save `sch_temp'
		
		// save grade and school-grade priors
		drop sch_prior*
		tempfile gr_schgr_avg_prior_adj_rates
		save `gr_schgr_avg_prior_adj_rates'
		
		// save school priors
		use `sch_temp', clear
		bysort exp_school : keep if _n==1
		keep exp_school sch_prior*
		tempfile sch_avg_prior_adj_rates
		save `sch_avg_prior_adj_rates'
	restore
	
	merge m:1 exp_school using `sch_avg_prior_adj_rates', nogen keep(1 3)
	merge m:1 exp_school exp_grade using `gr_schgr_avg_prior_adj_rates', nogen keep(1 3)

		// confirm no school or school-grade priors are 0
		assert sch_prior_avg_days_absent != 0
		assert schgr_prior_avg_days_absent != 0

		// dummy out missings
		* school-grade
		gen mi_schgr_prior = mi(schgr_prior_adj_abs_rate)
		replace schgr_prior_adj_abs_rate = 0 if mi_schgr_prior==1
		replace schgr_prior_avg_days_absent = 0 if mi_schgr_prior == 1
		* school
		gen mi_sch_prior = mi(sch_prior_adj_abs_rate)
		replace sch_prior_adj_abs_rate = 0 if mi_sch_prior==1
		replace sch_prior_avg_days_absent = 0 if mi_sch_prior == 1
		
		tempfile sch_priors
		save `sch_priors'
}


// Calculate weekly absence rates
{
	// Set up
		// load 2020 daily data
		use sid absent school_year date using "${inputs}\disD\disD_dw_daily_absences.dta" if school_year == 2020, clear
		
        //removed section specifying dates not in school 
		
		// merge on treatment data
		merge m:1 sid using `updated_trt', keep(3) nogen
	
	// Post-treatment weekly 
	preserve
		pg_get_weekly, startdate_dmy("$start_date") enddate_dmy("$end_date") save("${interim}/disD/weekly_cumulative_adj_abs.dta")
	restore
		
	// Pre-treatment weekly 
	preserve
		pg_get_weekly, startdate_dmy("$pre_start") enddate_dmy("$pre_end") save("${interim}/disD/pre_treat_adj_abs.dta") pre_treat
	restore

	// Merge on weekly data and mark attrition
	use `sch_priors', clear
	merge 1:1 sid using "${interim}/disD/pre_treat_adj_abs.dta"
		tab _merge // 92% of students merged
		tab _merge treatment, col
			* similar proportions of students don't merge across arms 
		drop _merge
	 
	merge 1:1 sid using "${interim}/disD/weekly_cumulative_adj_abs.dta"
		// mark pre treatment attrition
		gen attrit_pre_treat = _merge == 1
		drop _merge
		tab attrit_pre_treat treatment, col
		
		// add missing indicator for pre_treat
		gen mi_pre_treat = mi(pre_treat_abs_rate)	
		replace pre_treat_adj_abs_rate =  0 if mi_pre_treat == 1 	
		replace pre_treat_days_absent = 0 if mi_pre_treat == 1
		replace pre_treat_days_enrolled = 0 if mi_pre_treat == 1 
		
		// mark post treatment attrition
		gen attrit_post_treat = mi(adj_abs_rate) & attrit_pre_treat == 0
		tab attrit_post_treat treatment, col
		
		tempfile data
		save `data'
}

// Get prior abs-rates adjusted by mean of last-year's observed grade-level
{
	// Get 2019 grade levels from analysis file 
	use "${inputs}\disD\disD_dw_analysis.dta" if school_year == 2019, clear
	keep sid grade_level
	rename grade_level prior_grade_level

	merge 1:1 sid using `data'
	tab _merge
	tab exp_grade _merge, m // merge rates make sense
	tab prior_grade_level _merge, m
	keep if inlist(_merge, 2, 3)
	
	// treat those enrolled < 20 days in prior year as missing prior info
	replace prior_days_absent = . if prior_days_enrolled < 20
	replace prior_abs_rate = . if prior_days_enrolled < 20
	replace prior_days_enrolled = . if prior_days_enrolled < 20
	
	// calculate adjusted averages 
	bysort prior_grade_level : egen avg_prior_abs_rate = mean(prior_abs_rate)
	gen prior_adj_abs_rate = prior_abs_rate/avg_prior_abs_rate
	
	// create missing indicator and inpute missings
	gen mi_prior = mi(prior_abs_rate) 
	foreach var in prior_days_absent prior_days_enrolled prior_abs_rate prior_adj_abs_rate {
		replace `var' = 0 if mi_prior == 1
	}
	
	// create pr variables
	clonevar mi_pr_prior = mi_prior
	foreach var in prior_days_absent prior_days_enrolled prior_abs_rate prior_adj_abs_rate {
		clonevar pr_`var' = `var'
	}
}		
	

	
// Prepare data for saving
{	
	// consolidate race categories
	recode xrace (2 4 6 = 7)
		
	// Make missing vars
	foreach var of varlist x* {
		qui count if mi(`var')
		di "`r(N)' students missing `var'"
	}
	gen mi_xmale = mi(xmale)
	replace xmale = 0 if mi_xmale == 1
	foreach var in xrace xell xsped xfrpl {
		assert !mi(`var')
	}
	
	// label variables 
	lab var prior_grade_level "Grade level in SY 2019"
	lab var prior_adj_abs_rate "Adjusted absence rate in SY 2019"
	lab var pr_prior_adj_abs_rate "Adjusted absence rate in SY 2019"
	lab var pr_prior_abs_rate "Abs rate in SY 2019"
	lab var mi_prior "Missing prior absence information"
	lab var mi_pr_prior "Missing pre-randomization prior absence information"
	lab var schgr_prior_adj "School-grade adjusted absence rate in SY 2019"
	lab var schgr_prior_avg_days "School-grade average days absent in SY 2019"
	lab var mi_schgr_prior "Missing prior school-grade absence information"
	lab var sch_prior_adj "School adjusted absence rate in SY 2019"
	lab var sch_prior_avg_days "School average days absent in SY 2019"
	lab var mi_sch_prior "Missing prior school absence information"
	lab var adj_abs_rate "Adjusted absence rate in full post-treatment period"
	lab var cum_abs_rate "Unadjusted absence rate in full post-treatment period"
	lab var cum_days_absent "Days absent in full post-treat period"
	lab var cum_days_enrolled "Days enrolled in full post-treat period"
	
}	
	
// Save data
{	
	// save out full data
	cap drop _*
	compress
	save "${outputs}/disD/disD_messaging_oct2019_full_data.dta", replace
	
	// save out model data
	keep if attrit_pre_treat==0
		
	pg_check_data, filetype(model)
	assert `r(num_errors)' == 0
	save "${outputs}/disD/disD_messaging_oct2019_model_data.dta", replace
}
