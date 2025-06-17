/*******************************************************************************

	File name: disF_robocalls_sep2019_afile.do
	Purpose: create full and model data files for disF's Fall '19 robocalls pilot	

*******************************************************************************/


// Current date and log
	// Set Up
	{
		clear all
		//macro drop _all
		cap log close
		set more off
		version 15.1
		set type double
	}

local currdate = string(date(c(current_date), "DMY"), "%tdCCYYNNDD")
global today = `currdate'
di "$today"
log using "${logs}/disF/disF_robocalls_sep2019_afile_log_${today}.log", replace

// Topmatter
{

	
	global randomized_file "${inputs}/disF/UPDATE_disF_robocalls_sep2019_students_20191218.dta"
	global analysis_file "${inputs}/disF/disF_dw_analysis.dta"
	global daily_data "${inputs}/disF/disF_dw_daily_absences.dta"
	global full_data			"${outputs}/disF/disF_robocalls_sep2019_full_data.dta"
	global model_data			"${outputs}/disF/disF_robocalls_sep2019_model_data.dta"
	global pre_start			"26aug2019" 
	global pre_end				"03oct2019" 
	global start_date			"04oct2019" 
	global end_date				"20dec2019"


	
}



// Make sure all randomization-related variables are present
{
	// Load student-level randomized file
	use "$randomized_file", clear

	// Add rand_bin, rand_bin_vars, and block_group
	pg_randgroup block_hhld_school block_hhld_grade, bin(bin)

	// Add other important randomization variables: rand_unit, rand_level, arm
	clonevar rand_unit = hhld
	gen rand_level = 2
	gen arm = 1
	replace arm = 2 if treatment == 1
	assert !missing(arm)
	assert arm == 1 if treatment == 0
	assert arm == 2 if treatment == 1
	
	// Drop the prior_days_absent and pr_prior_days_absent vars from the file;
	// these were created earlier during randomization, and were not based on
	// the analysis file
	drop prior_days_absent pr_prior_days_absent
	
}



// Merge DW analysis file to get prior year attendance and enrollment data
{

	// Merge analysis file
	merge 1:1 sid school_year using "$analysis_file"
	* Recall that disF refers to school years locally by the fall calendar year,
	* unlike on PG, where we use the spring calendar year. So disF considers this
	* pilot to be run in the 2019, but we say 2020. The randomized file reflects
	* the 2020 convention so that merging the analysis file works properly.

	// Explore merge results
	tab school_year _merge
	tab grade_level _merge 
	tab grade_level exp_grade
	
	// Drop things that are going to be redundant
	drop male sped race ell grade_level school_code
	* There are probably some other redundant variables from the merge
	* that you can drop.
	
	// Keep only those kids who were in the randomized and/or analysis file
	keep if inlist(_merge, 1, 3) 
	drop _merge
	
}



// Calculate school and school-grade prior absences
{
preserve
	// Load analysis file
	use "$analysis_file", clear
	isid sid school_year
	
	// Restrict to prior year, so 2019 for this 2020 school year pilot
	keep if school_year == 2019
	
	// Drop students enrolled less than 20 days
	drop if days_enrolled < 20
	
	// Calculate adjusted absence rate at student level
	bys grade_level: egen gr_prior_average_abs_rate = mean(abs_rate)
	gen adj_abs_rate = abs_rate / gr_prior_average_abs_rate
	
	// Calculate school and school-grade prior average adjusted absence rates
	bys school_code: egen sch_prior_adj_abs_rate = mean(adj_abs_rate)
	bys school_code grade_level: egen schgr_prior_adj_abs_rate = mean(adj_abs_rate)
	
	// Calculate school and school-grade prior average days absent
	bys school_code: egen sch_prior_avg_days_absent = mean(days_absent)
	bys school_code grade_level: egen schgr_prior_avg_days_absent = mean(days_absent)
	
	// Keep only what is needed
	keep school_code grade_level sch_prior_adj_abs_rate schgr_prior_adj_abs_rate ///
		 sch_prior_avg_days_absent schgr_prior_avg_days_absent
	
	// Rename variables for merge with student-level randomized file
	rename school_code exp_school
	rename grade_level exp_grade
	
	// Save both school and school-grade priors as a tempfile
	tempfile sch_schgr_priors
	save `sch_schgr_priors'
	
	// Save school priors as a tempfile
	keep exp_school sch_*
	duplicates drop
	tempfile sch_priors
	save `sch_priors'
	
	// Save school-grade priors as a tempfile
	use `sch_schgr_priors', clear
	keep exp_school exp_grade schgr_*
	duplicates drop
	tempfile schgr_priors
	save `schgr_priors'

restore

	// Merge school priors back onto randomized file
	merge m:1 exp_school using `sch_priors'
	keep if inlist(_merge, 1, 3)
	drop _merge
	
	// Merge school-grade priors back onto randomized file
	merge m:1 exp_school exp_grade using `schgr_priors'
	keep if inlist(_merge, 1, 3)
	drop _merge		
	
	// Treat any school or school-grade priors of 0 as missing
	foreach v in sch_prior_avg_days_absent sch_prior_adj_abs_rate ///
				 schgr_prior_avg_days_absent schgr_prior_adj_abs_rate {
		di "`v'"
		assert `v' != 0
	}
	* None of these are 0, so no need to replace to missing.
	
	// Dummy out missings for school-grade priors and set to 0
	assert missing(schgr_prior_adj_abs_rate) if missing(schgr_prior_avg_days_absent)
	assert missing(schgr_prior_avg_days_absent) if missing(schgr_prior_adj_abs_rate)
	gen mi_schgr_prior = missing(schgr_prior_adj_abs_rate)
	replace schgr_prior_adj_abs_rate = 0 if mi_schgr_prior == 1
	replace schgr_prior_avg_days_absent = 0 if mi_schgr_prior == 1
	fre mi_schgr_prior
	
	// Dummy out missings for school priors and set to 0
	assert missing(sch_prior_adj_abs_rate) if missing(sch_prior_avg_days_absent)
	assert missing(sch_prior_avg_days_absent) if missing(sch_prior_adj_abs_rate)
	gen mi_sch_prior = missing(sch_prior_adj_abs_rate)
	replace sch_prior_adj_abs_rate = 0 if mi_sch_prior == 1
	replace sch_prior_avg_days_absent = 0 if mi_sch_prior == 1
	fre mi_sch_prior
	
	// Save the current student-level randomized file temporarily so you 
	// can move on to calculating absence outcomes for merging
	tempfile student_data
	save `student_data'
	
}



// Calculate weekly absence rates
{

	// Load daily attendance data from the current school year
	use sid absent school_year date using "$daily_data", clear
	keep if school_year == 2020

	// Merge on treatment status from original randomized file
	merge m:1 sid using "$randomized_file", keepusing(treatment exp_school exp_grade)
	keep if _merge == 3
	drop _merge

	// Calculate post-treatment weekly absence outcomes
	preserve
		pg_get_weekly, startdate_dmy($start_date) enddate_dmy($end_date) ///
					   save("${interim}/disF/weekly_cumulative_adj_abs.dta")
	restore
	
	// Because pilot started after the school year began, calculate
	// pre-treatment weekly absence outcomes
	preserve
		pg_get_weekly, startdate_dmy($pre_start) enddate_dmy($pre_end) ///
			  		   save("${interim}/disF/pre_treat_adj_abs.dta") pre_treat
	restore
	
	// Merge pre-treat and post-treat absence outcomes back on to 
	// student-level randomized file
	use `student_data', clear
	merge 1:1 sid using "${interim}/disF/pre_treat_adj_abs.dta"
	drop _merge
	merge 1:1 sid using "${interim}/disF/weekly_cumulative_adj_abs.dta"
	
	// Mark pre-treatment attrition
	gen attrit_pre_treat = _merge == 1
	drop _merge
	
	// Add missing indicator for pre_treat and zero-out pre_treat vars
	gen mi_pre_treat = missing(pre_treat_abs_rate)
	
	// Replace missings with 0
	replace pre_treat_adj_abs_rate = 0 if mi_pre_treat == 1 	
	replace pre_treat_days_absent = 0 if mi_pre_treat == 1
	replace pre_treat_days_enrolled = 0 if mi_pre_treat == 1	
	
	// Mark post-treatment attrition
	gen attrit_post_treat = missing(adj_abs_rate) & attrit_pre_treat == 0
	fre attrit_post_treat

	// Save tempfile
	tempfile data
	save `data'
	
}



// Calculate prior adjusted absence rates, adjusted by mean of last year's
// observed grade level
{


	// Get 2019 grade levels from analysis file
	use "$analysis_file", clear
	keep if school_year == 2019
	keep sid grade_level
	rename grade_level prior_grade_level
	
	merge 1:1 sid using `data'
	keep if _merge == 2 | _merge == 3
	drop _merge
	assert missing(prior_abs_rate) if missing(prior_grade_level) // this should hold

	// Set students enrolled <20 days to not have priors
	replace prior_days_absent = . if prior_days_enrolled < 20
	replace prior_abs_rate = . if prior_days_enrolled < 20
	
	// Calculate adjusted averages
	bys prior_grade_level: egen avg_prior_abs_rate = mean(prior_abs_rate)
	gen prior_adj_abs_rate = prior_abs_rate/avg_prior_abs_rate

	// Deal with missingness
	gen mi_prior = missing(prior_adj_abs_rate)
	replace prior_adj_abs_rate = 0 if mi_prior==1
	replace prior_abs_rate = 0 if mi_prior==1
	replace prior_days_absent = 0 if mi_prior==1
	replace prior_days_enrolled = 0 if mi_prior==1

}


// Prepare data for saving
{

	// Make sure pid is present for everyone
	replace pid = [REMOVED] if missing(pid)
	assert pid == [REMOVED]

	// Create pr_prior variables, which are all just copies of 
	// prior vars because this pilot randomized and implemented
	// during the same school year
	clonevar pr_prior_days_absent = prior_days_absent
	clonevar pr_prior_days_enrolled = prior_days_enrolled
	clonevar pr_prior_adj_abs_rate = prior_adj_abs_rate
	clonevar mi_pr_prior = mi_prior

	// Generate a race missing indicator and recode these folks to 7
	gen mi_xrace = missing(xrace)	
	replace xrace = 7 if mi_xrace == 1
	
	// Consolidate race categories
	recode xrace (2 4 6 = 7)	
	
	// Make missing vars for demographic variables and recode to 0
	foreach var in xell xfrpl xsped xmale {
		gen mi_`var' = missing(`var')
		replace `var' = 0 if mi_`var' == 1
	}
	
}



// Save out full and model data
{

	// Order vars
	order pid sid school_year exp_school exp_grade

	// Save out full data
	cap drop _* // making sure no temp vars left
	compress
	save "$full_data", replace

	// Drop attriters
	keep if attrit_pre_treat == 0
	drop attrit*
	
	// Check against specs for model data file
	pg_check_data, filetype(model)
	assert `r(num_errors)' == 0
	
	// Save out model data
	compress
	save "$model_data", replace

}
