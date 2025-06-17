/*******************************************************************************
	File: 						afile_disB_energybill_oct2018.do
								
	Purpose: 					Produce analysis file for disB's 
								Energy Bill Letter Pilot
		
	Inputs:
		1) DW disB analysis file
		2) Randomization file 
		3) DW disB Daily Attendance data
	
	Outputs:
		1) Pilot analysis file for disB's Energy Bill pilot
				
*******************************************************************************/

// Topmatter
{
	// Set Up
		clear all
		cap log close
		set more off, permanently
		set seed 12345
		
	// Set date
		local currdate = string(date(c(current_date), "DMY"), "%tdCCYYNNDD")
		global today = `currdate'
		di "$today"	
	
	// Dates
		global pre_start 	"06sep2018"
		global pre_end 		"30oct2018" 
		
		global start_date 	"31oct2018" 
		global end_date 	"17jun2019"	
	
		}

//log the process
	log using "${logs}/disB/disB_energybill_oct2018_afile_log_${today}.log", replace 

// Daily absence and summary files
{
	use "${inputs}\disB\disB_dw_daily_absences.dta" if school_year == 2019, clear
	keep sid school_year school_code withdraw_code date absent abs_schday
	drop if date == mdy(2,12,2019)
	drop if date > mdy(6,17,2019)
	save "${inputs}/disB/disB_daily_data_revised", replace
	
	global daily_data 	"${inputs}/disB/disB_daily_data_revised"

}	

// Load radomized list
{
	use "${inputs}/disB/FINAL_disB_energybill_student_20181011_updated", clear
	keep household* treatment* address unit studentid grade_level sped ell ///
		 schoolid enroll* schoolname sid school_code male race bin hh_school_code hh_grade_level
	isid sid
	count // 16,265
	//pg_label_race

// Rename
	rename (ell sped male race) (xell xsped xmale xrace) 
	rename (school_code schoolname grade_level)(exp_school exp_school_name exp_grade)

// Only include students from households with known addresses
	keep if household_block == 1 // 102 dropped
	
// Get prior grade level 
	preserve
		use "${inputs}/disB/disB_dw_analysis.dta", clear
		keep if school_year == 2018
		keep sid school_year grade_level abs_rate
		rename grade_level prior_grade_level
		rename abs_rate prior_abs_rate
		tempfile afile_info_2018
		save `afile_info_2018'
	restore
	
	merge 1:1 sid using `afile_info_2018', keep(1 3) gen(m_prior_grade)
	* 1: 722, 3: 15,441
	
	gen arm = 1 if treatment_name == "Control"
	replace arm = 2 if treatment_name == "energybill"
	replace arm = 3 if treatment_name == "lostlearning"

	order treatment* arm*
	replace treatment = 1 if treatment >= 1
	
	gen arm_names = treatment_name
	
	// Combine [REMOVED] schools
	replace exp_school_name = "[REMOVED]" if regexm(exp_school_name, "[REMOVED]")
	replace exp_school = [REMOVED] if regexm(exp_school_name, "[REMOVED]")
	replace schoolid = [REMOVED] if regexm(exp_school_name, "[REMOVED]")
	
	// Combine [REMOVED] and [REMOVED]
	replace exp_school_name = "[REMOVED]" if regexm(exp_school_name, "[REMOVED]")
	replace exp_school = [REMOVED] if regexm(exp_school_name, "[REMOVED]")
	replace schoolid = [REMOVED] if regexm(exp_school_name, "[REMOVED]")
}	

// Calculate school and school-grade averages of 2018 absence rates
{
	// Get prior abs-rates adjusted by mean of last-year's observed grade-level/ other expected values
	preserve
		use "${inputs}/disB/disB_dw_analysis.dta", clear 
		keep if school_year==2018 
		drop if days_enrolled < 20 // 49
		
		// [REMOVED]
		replace school_name = "[REMOVED]" if regexm(school_name, "[REMOVED]") 
		replace school_code = [REMOVED] if regexm(school_name, "[REMOVED]") 
		// [REMOVED]
		replace school_name = "[REMOVED]" if regexm(school_name, "[REMOVED]")
		replace school_code = [REMOVED] if regexm(school_name, "[REMOVED]")
		
		bysort grade_level : egen mean_gl_abs_rate = mean(abs_rate)
		gen adj_abs_rate = abs_rate / mean_gl_abs_rate // abs_rate adjusted within the grade level in the entire district
		
		bysort school_code : egen sch_prior_adj_abs_rate = mean(adj_abs_rate) // school-wide abs_rate
		bysort school_code : egen sch_prior_avg_days_absent = mean(days_absent)
		
		bysort school_code grade_level : egen schgr_prior_adj_abs_rate = mean(adj_abs_rate) // school-grade abs_rate
		bysort school_code grade_level : egen schgr_prior_avg_days_absent = mean(days_absent)
		
		collapse (mean) schgr_prior_adj_abs_rate sch_prior_adj_abs_rate ///
					schgr_prior_avg_days_absent sch_prior_avg_days_absent (count) sid, by(school_code grade_level school_year)
		drop sid
	
		merge m:1 school_code school_year using "${inputs}/disB/disB_dw_schools", keep(3) nogen

		// rename to merge based on expected school and grade
		bysort school_code : egen temp_sch = max(sch_prior_adj_abs_rate) 
		replace sch_prior_adj_abs_rate = temp_sch 
		drop temp_sch
		rename grade_level exp_grade // This is labeled "exp" just to merge with student level data later
		rename school_code exp_school
		rename school_name exp_school_name

		// [REMOVED]
		replace exp_school_name = "[REMOVED]" if regexm(exp_school_name, "[REMOVED]") 
		replace exp_school = [REMOVED] if regexm(exp_school_name, "[REMOVED]") 
		drop if !inrange(exp_grade, 0, 12)
	
		tempfile sch_temp
		save `sch_temp'
		
		// save grade and school-grade priors
		drop sch_prior_adj_abs_rate sch_prior_avg_days_absent
		tempfile gr_schgr_avg_prior_adj_rates
		drop if mi(exp_grade)
		save `gr_schgr_avg_prior_adj_rates'
		
		// save school priors
		use `sch_temp', clear
		bysort exp_school : keep if _n==1
		keep exp_school sch_prior_adj_abs_rate exp_school_name sch_prior_avg_days_absent
		tempfile sch_avg_prior_adj_rates
		save `sch_avg_prior_adj_rates'
	restore		

	merge m:1 exp_school exp_grade using `gr_schgr_avg_prior_adj_rates', keep (1 3) gen(m_schgr_prior)
		
	merge m:1 exp_school using `sch_avg_prior_adj_rates', keep(1 3) gen(m_sch_prior)

	// Dummy out missings
		gen mi_sch_prior = mi(sch_prior_adj_abs_rate)
		replace sch_prior_adj_abs_rate = 0 if mi_sch_prior==1
	
		gen mi_sch_prior_avg_days_absent = mi(sch_prior_avg_days_absent)
		replace sch_prior_avg_days_absent = 0 if mi_sch_prior_avg_days_absent == 1 
		
		gen mi_schgr_prior = mi(schgr_prior_adj_abs_rate)
		replace schgr_prior_adj_abs_rate = 0 if mi_schgr_prior==1
		
		gen mi_schgr_prior_avg_days_absent = mi(schgr_prior_avg_days_absent)
		replace schgr_prior_avg_days_absent = 0 if mi_schgr_prior_avg_days_absent==1
		
		 
		assert mi(prior_abs_rate) if mi(prior_grade_level) // We do expect this to hold

	// Drop 
		drop m_* 
}	

// Merge 2019 absence data
{
	drop prior_abs_rate
	preserve
		use "${inputs}/disB/disB_dw_analysis.dta", clear 
		keep if school_year==2019
		keep sid school_year days_enrolled prior* days_absent chronic_absent frpl
		// For students who were enrolled less than 20 days in prior year, consider their records missing.
		replace prior_days_absent = . if prior_days_enrolled < 20 
		replace prior_abs_rate = . if prior_days_enrolled < 20 
		replace prior_chronic_absent = . if prior_days_enrolled < 20 
		replace prior_days_enrolled = . if prior_days_enrolled < 20 
		rename frpl xfrpl
		tempfile 2019_abs_info
		save `2019_abs_info'
	restore
	
	merge 1:1 sid using `2019_abs_info', keep (1 3) gen(m_abs19)
		* 1: 165 without 2019 abs records
	
	bysort prior_grade_level : egen avg_prior_abs_rate = mean(prior_abs_rate) 
	gen prior_adj_abs_rate = prior_abs_rate/avg_prior_abs_rate
	
	// Dummy out missings
	gen mi_prior = mi(prior_adj_abs_rate) 
	replace prior_adj_abs_rate = 0 if mi_prior==1
	
	save "${inputs}/disB/clean_energybill_student", replace
}

// Merge on weekly cumulative absence rates
{
	// post-treatment
	preserve
		use "${inputs}\disB\disB_dw_daily_absences" if school_year == 2019, clear
		drop if date == mdy(2,12,2019)
		merge m:1 sid using "${inputs}\disB/clean_energybill_student", keep(3) nogen
		pg_get_weekly, startdate_dmy("$start_date") enddate_dmy("$end_date") save("${interim}\disB/weekly_cumulative_adj_abs.dta")	
	restore

	// pre-treatment
	preserve
		// load 2019 daily absences
		use "${inputs}/disB/disB_dw_daily_absences" if school_year == 2019, clear
		merge m:1 sid using "${inputs}\disB/clean_energybill_student", keep(3) nogen
		pg_get_weekly, startdate_dmy("$pre_start") enddate_dmy("$pre_end") save("${interim}\disB/pre_treat_adj_abs.dta") pre_treat
	restore	
	
	merge 1:1 sid using "${interim}/disB/weekly_cumulative_adj_abs.dta", keep(1 3) 
	merge 1:1 sid using "${interim}/disB/pre_treat_adj_abs.dta", keep(1 3) nogen
}

// Attrition
{
	// Mark student who were assigned to treatment but didn't enroll
		gen attrit_pre_treat = _merge==1 // 441
	// Post treatment attrition
		gen attrit_post_treat = mi(adj_abs_rate) & attrit_pre_treat != 1 // 583-441 = 142
		tab1 attrit* // 
}

// Only get students of interest and save
{
	drop if regexm(exp_school_name, "[REMOVED]") 
	rename household_prera hhld
	unique hhld // 9270
	count // 16161
	replace pid = [REMOVED] if mi(pid)

}

// Creating necessaery vars
{
	//pre_treat vars
	gen mi_pre_treat = mi(pre_treat_adj_abs_rate)
	replace pre_treat_abs_rate = 0 if mi_pre_treat == 1 
	replace pre_treat_adj_abs_rate = 0 if mi_pre_treat == 1 
	replace pre_treat_days_absent = 0 if mi_pre_treat == 1 
	replace pre_treat_days_enrolled = 0 if mi_pre_treat == 1 

	// prior vars
	replace mi_prior = 1 if prior_days_enrolled < 20 
	replace prior_days_enrolled = 0 if mi_prior == 1
	replace prior_days_absent = 0 if mi_prior == 1
	replace prior_adj_abs_rate = 0 if mi_prior == 1

	// pr_prior
	clonevar mi_pr_prior = mi_prior	
	clonevar pr_prior_days_absent =	prior_days_absent
	clonevar pr_prior_days_enrolled = prior_days_enrolled
	clonevar pr_prior_adj_abs_rate =  prior_adj_abs_rate
	
	// other 
	gen mi_prior_chronic_absent = 0
	replace mi_prior_chronic_absent = 1 if mi(prior_chronic_absent)
	replace schgr_prior_adj_abs_rate = 0 if mi_schgr_prior==1
	replace schgr_prior_avg_days_absent = 0 if mi_schgr_prior == 1
	
	gen block_hhld_address = household_block 
	gen block_hhld_school = hh_school_code 
	gen block_hhld_grade = hh_grade_level
	
	
	pg_randgroup block_hhld_address block_hhld_school block_hhld_grade, bin(bin)
	clonevar rand_unit = hhld
	gen rand_level = 2 

	// Collapse race
	replace xrace = 7 if inlist(xrace, 2,4,6)

	// missing vars
	foreach var of varlist x* {
		gen mi_`var' = mi(`var')
		tab mi_`var'
		replace `var' = 0 if mi_`var' == 1
	}

	tab school_year,m 
	
	drop school_year
	gen school_year = 2019
	save "${outputs}\disB/disB_energybill_oct2018_full_data", replace	
}



// Save Model Data
{
	drop  if attrit_pre_treat==1 
	count // 15514

	count // 15546
	unique hhld // 8970
	drop household_block
	
	pg_check_data, filetype(model)
	
	save "${outputs}\disB/disB_energybill_oct2018_model_data", replace
}
	
cap log close
