/*******************************************************************************
	File: 						afile_disE_energybill_oct2018.do
	Purpose: 					Produce analysis file for disE's 
								Energy Bill Letter Pilot
		
		
*******************************************************************************/

// Topmatter
		clear all
		cap log close
		set more off, permanently
		set seed 12345
	global today = string(td($S_DATE),"%tdCYND")	
	log using "${logs}/disE/afile_disE_personalized_messaging_${today}.log", replace


global rand_var hhld



// Load radomized list

	// Message content changed with back-and-forth with disE, but randomization never did
	use "${inputs}\disE\DRAFT_disE_energybill_parent_20181019.dta", clear
	
	merge 1:1 sid using "${inputs}\disE\bin_intermediate_parent.dta"
	assert _m==3
	drop _m
	
	assert treatment == t_treatment
	drop t_treatment
	rename bin parent_letter_bin
	
	// grab student letter assignment too
	preserve
		use "${inputs}\disE\DRAFT_disE_energybill_student_20181019.dta", clear
		keep sid treatment treatment_name
		
		merge 1:1 sid using "${inputs}\disE\bin_intermediate_student.dta"
		assert _m==3
		assert treatment==t_treatment
		drop _merge
		drop t_treatment
		
		rename treatment student_letter_treatment
		rename treatment_name student_letter_treatment_name
		rename bin student_letter_bin
		
		tempfile stu
		save `stu'
	restore
	merge 1:1 sid using `stu', assert(1 3) nogen
	
	keep sid household household_grade homeless_hh ///
		treatment treatment_name student_letter_treatment student_letter_treatment_name ///
		frpl sped ell male race ///
		schoolcode schoolname [REMOVED] ///
		student_letter_bin parent_letter_bin 
	
	// record block
	bys household ([REMOVED] sid): gen low_gr = [REMOVED][1]
	bys household ([REMOVED] sid): gen low_sch = schoolcode[1]
	
	egen block = group(homeless_hh low_gr low_sch)
	*drop low_gr low_sch
	rename low_gr block_stu_grade
	rename low_sch block_stu_school
	rename homeless_hh block_stu_homeless
	
	isid sid
	count // 20,674
	
	rename (frpl ell sped male race) x=
	// convert from their school code to ours
	preserve
		insheet using "${inputs}\disE\CEPRID_School_Mapping_Extract.csv", comma clear
		rename localschoolid schoolcode
		rename anonymizedschoolid school_code
		tempfile xw
		save `xw'
	restore
	merge m:1 schoolcode using `xw', assert(2 3) keep(3) nogen
	drop schoolcode
	
	rename school_code exp_school
	rename [REMOVED] exp_grade
	rename schoolname exp_school_name
	
	// grab prior info
	preserve
		use "${inputs}\disE\disE_dw_analysis.dta", clear //changed to match new name from pg stitch
		keep if school_year == 2018
		keep sid school_year grade_level abs_rate days_enrolled days_absent
		rename grade_level prior_grade_level
		rename abs_rate prior_abs_rate
		rename days_absent prior_days_absent
		rename days_enrolled prior_days_enrolled
		tempfile afile_info_2018
		save `afile_info_2018'
	restore
	// 31 didn't exist in prior year. Weird.
	merge 1:1 sid using `afile_info_2018', keep(1 3) nogen
	
	// drop kids w/o demogs
	foreach x in frpl ell sped male race {
		drop if mi(x`x')
	}


// Calculate school and school-grade averages of 2018 absence rates

	// Get prior abs-rates adjusted by mean of last-year's observed grade-level/ other expected values
	preserve
		use "${inputs}\disE\disE_dw_analysis.dta", clear 
		keep if school_year==2018 
		drop if days_enrolled <= 20 // 581
		bysort grade_level : egen mean_gl_abs_rate = mean(abs_rate)
		gen adj_abs_rate = abs_rate / mean_gl_abs_rate // abs_rate adjusted within the grade level in the entire district
		bysort school_code : egen sch_prior_adj_abs_rate = mean(adj_abs_rate) // school-wide abs_rate
		bysort school_code grade_level : egen schgr_prior_adj_abs_rate = mean(adj_abs_rate) // school-grade abs_rate
		bys school_code: egen sch_prior_avg_days_absent = mean(days_absent)
		bys school_code grade_level: egen schgr_prior_avg_days_absent = mean(days_absent)
		
		collapse (mean) schgr_prior_adj_abs_rate sch_prior_avg_days_absent sch_prior_adj_abs_rate schgr_prior_avg_days_absent (count)num_stu=sid, by(school_code school_year grade_level)
		
		// rename to merge based on expected school and grade
		bysort school_code : egen temp_sch = max(sch_prior_adj_abs_rate) 
		replace sch_prior_adj_abs_rate = temp_sch 
		drop temp_sch
		rename grade_level exp_grade // This is labeled "exp" just to merge with student level data later
		rename school_code exp_school

		// [REMOVED]
		drop if !inrange(exp_grade, -1, 12)
		drop num_stu
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
		keep exp_school sch_prior_adj_abs_rate sch_prior_avg_days_absent
		tempfile sch_avg_prior_adj_rates
		save `sch_avg_prior_adj_rates'
	restore		

	merge m:1 exp_school exp_grade using `gr_schgr_avg_prior_adj_rates', keep (1 3) nogen
		
	merge m:1 exp_school using `sch_avg_prior_adj_rates', keep(1 3) nogen
	
	// Dummy out missings
		gen mi_schgr_prior = mi(schgr_prior_adj_abs_rate)
		replace schgr_prior_adj_abs_rate = 0 if mi_schgr_prior==1
		gen mi_sch_gr_prior1 = mi(schgr_prior_avg_days_absent)
		replace schgr_prior_avg_days_absent = 0 if mi_sch_gr_prior1==1
		
		gen mi_sch_prior = mi(sch_prior_adj_abs_rate)
		replace sch_prior_adj_abs_rate = 0 if mi_sch_prior==1
		gen mi_sch_prior1 = mi(sch_prior_avg_days_absent)
		replace sch_prior_avg_days_absent = 0 if mi_sch_prior1==1
		
		assert mi(prior_abs_rate) if mi(prior_grade_level) // We do expect this to hold
		bysort prior_grade_level : egen avg_prior_abs_rate = mean(prior_abs_rate) 
		gen prior_adj_abs_rate = prior_abs_rate/avg_prior_abs_rate
		clonevar pr_prior_adj_abs_rate = prior_adj_abs_rate

	// Dummy out missings
		gen mi_prior = mi(prior_adj_abs_rate) 
		replace prior_adj_abs_rate = 0 if mi_prior==1
		
		gen prior_chronic_absent = prior_abs_rate>=0.1 if mi_prior==0

	// Sort treatment into arms: 
		gen arm = treatment + 1
		tab1 arm treatment
		gen student_letter_arm = student_letter_treatment + 1
		tab1 student_letter_arm student_letter_treatment
		
		drop mi_sch_gr_prior1 mi_sch_prior1
		
		compress
		isid sid
		save "${interim}\disE\clean_energybill_student_new", replace
	

// Merge on weekly cumulative absence rates

	// post-treatment
	use "${inputs}\disE\disE_dw_daily_absences.dta", clear
	keep if school_year==2019
	
	merge m:1 sid using "${interim}\disE\clean_energybill_student_new", keep(3) nogen
	pg_get_weekly, startdate_dmy("13nov2018") enddate_dmy("20mar2019") save("${interim}/disE/weekly_cumulative_adj_abs_new.dta")	
	
	// pre-treatment
	use "${inputs}/disE/disE_dw_daily_absences", clear
	keep if school_year==2019
	
	merge m:1 sid using "${interim}/disE/clean_energybill_student_new", keep(3) nogen
	pg_get_weekly, startdate_dmy("20aug2018") enddate_dmy("19oct2018") save("${interim}/disE/pre_treat_adj_abs_new.dta") pre_treat
	
	use "${interim}/disE/clean_energybill_student_new", clear
	merge 1:1 sid using "${interim}/disE/weekly_cumulative_adj_abs_new.dta", keep(1 3) gen(_merge)
	merge 1:1 sid using "${interim}/disE/pre_treat_adj_abs_new.dta", keep(1 3) nogen

	
// Updating Data Files for FE and Count Models

*(1) Gen pr_prior_days_absent/enrolled = prior_days_absent
gen pr_prior_days_absent = prior_days_absent
gen pr_prior_days_enrolled = prior_days_enrolled

/*  Note: Randomization & Intervention occurred in the same year. Therefore, prior_days_absent &
	prior_days_enrolled are equal to the cumulative absent and enrolled days from the prior school year.
*/

*(2) mi_prior and mi_pre_treat
gen mi_pre_treat = missing(pre_treat_days_absent)

replace prior_days_absent = 0 if mi_prior==1
replace pr_prior_days_absent = 0 if mi_prior == 1
replace pr_prior_days_enrolled = 0 if mi_prior==1
replace pre_treat_days_absent = 0 if mi_pre_treat == 1

*(3) & (4) Done Above.

*(5) Generate Strata Vars
rename parent_letter_bin bin
pg_randgroup block_stu_homeless block_stu_school block_stu_grade, bin(bin)

*(6) Adding [REMOVED] Variable
clonevar mi_pr_prior = mi_prior

*(7) Recode Race
replace xrace = 7 if inlist(xrace, 2, 4)

*(8) Rand Unit
clonevar rand_unit = household

*(9) School Year
cap drop school_year
gen school_year=2019



// Attrition

	// Mark student who were assigned to treatment but didn't enroll
		gen attrit_pre_treat = _merge==1 // 567
		drop _merge
	// Post treatment attrition
		gen attrit_post_treat = mi(adj_abs_rate) // 792-567 = 225
		tab1 attrit* // 

// Only get students of interest and save
	rename household hhld
	unique hhld // 17923
	count // 20674
	gen pid = 45669
	
	clonevar pr_prior_abj_abs_rate = prior_adj_abs_rate
	replace pr_prior_adj_abs_rate = 0 if mi_pr_prior==1

	*gen mi_pre_treat = mi(pre_treat_adj_abs_rate)
	save "${outputs}/disE/disE_energybill_oct2018_full_data", replace	


// Save Model Data
	keep if attrit_pre_treat==0 // & attrit_post_treat==0  792 dropped
	*481 attrit_pre_treat attritors dropped
// 	drop attrit_pre_treat attrit_post_treat has_all_weeks
	count // 19882
	*20,180
	
	// keep rows with complete data for all weeks
// 	forval i = 1/15 {
// 		count if mi(adj_abs_rate_wk_`i') & attrit_post_treat
// 	}
// 	drop if mi(pre_treat_adj_abs_rate) // 2
	gen arm_names = treatment_name
// 	gen student_letter_arm_names = student_letter_treatment_name
		
	count // 19799
	unique hhld // 17186
	
	pg_check_data, filetype(model)

	gen rand_level = 2
	replace mi_prior = 1 if prior_days_enrolled < 20
	replace prior_days_enrolled = 0 if prior_days_enrolled < 20
	replace prior_days_absent = 0 if prior_days_enrolled < 20
	replace prior_adj_abs_rate = 0 if prior_days_enrolled < 20
	replace mi_pr_prior = 1 if pr_prior_days_enrolled < 20
	replace pr_prior_days_enrolled = 0 if pr_prior_days_enrolled < 20
	replace pr_prior_days_absent = 0 if pr_prior_days_enrolled < 20
	replace pr_prior_adj_abs_rate = 0 if pr_prior_days_enrolled < 20
	
	replace pre_treat_adj_abs_rate = 0 if missing(pre_treat_adj_abs_rate)
	replace prior_days_enrolled = 0 if missing(prior_days_enrolled)
	replace pre_treat_days_enrolled = 0 if missing(pre_treat_days_enrolled)
	
	pg_check_data, filetype(model)

	save "${outputs}/disE/disE_energybill_oct2018_model_data", replace

	