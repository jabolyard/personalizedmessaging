/*******************************************************************************

	File name: disF_robocalls_sep2019_randomization.do
	Date Created: 8/26/19
	Date Updated:
	
	Major steps in randomization:
		1) Prep student attributes
		2) Prep student school year (including restricting to 7-12 graders)
			* Additional exclusion of kids with gradelevel code "[REMOVED]"
		3) Merge student attributes onto student school year
		4) Make sure you have all appropriately named covariates for the models
		5) Add prior days absent for balance checks
		6) Merge on household identifiers 
		7) Generate household blocking variables
		8) Randomize at household level
		9) Save out treatment, control lists to .csv
		10) Generate balance tables and run checks


*******************************************************************************/


// Set log
cap log close
log using "P:/Proving_Ground/tables_figures_new/pilots_2020/disF_robocalls_sep2019/FINAL_disF_robocalls_sep2019_randomization.log", replace


// Topmatter
{

	// Set Up
	{
		clear all
		macro drop _all
		set more off
		version 15.1
		set type double
	}

	// Globals
	{
		// Inputs
			// misc
			global seed 20190828
			global date = string(date(c(current_date), "DMY"), "%tdCCYYNNDD")
			// raw data
			global raw_dir 				"P:/Proving_Ground/data/raw/Incoming Transfers/20190829 - disF Data"
			global student_attributes	"${raw_dir}/StudentAttributes.csv"
			global student_school_year 	"${raw_dir}/StudentSchoolYear.csv"
			global clean_dir 			"P:/Proving_Ground/data/clean_disF/clean"
			global student_household	"${clean_dir}/disF_student_household.dta"
			global dw_dir				"P:/Proving_Ground/data/clean/CDW/APT/Prod/disF"
			global dw_daily_abs			"${dw_dir}/StudentDailyAttendance.csv"
		// Outputs
			global out_dir 				"P:/Proving_Ground/data/pilots_2020/disF_robocalls_sep2019"
			global hhld_randomized		"${out_dir}/FINAL_disF_robocalls_sep2019_household_${date}.dta"
			global student_randomized	"${out_dir}/FINAL_disF_robocalls_sep2019_students_${date}.dta"
			global treatment_list		"${out_dir}/FINAL_disF_robocalls_treatment_${date}.csv"
			global control_list			"${out_dir}/FINAL_disF_robocalls_control_${date}.csv"
			global fig_dir				"P:/Proving_Ground/tables_figures_new/pilots_2020/disF_robocalls_sep2019"
			global stud_balance_table 	"${fig_dir}/FINAL_disF_robocalls_student_balance"
			global hhld_balance_table	"${fig_dir}/FINAL_disF_robocalls_hhld_balance"
	}

}


// 1. Prep student attributes
{

	// read in raw data
	import delimited "$student_attributes", clear
	pg_trim_strings
	
	// keep only the variables we need
	local to_keep studentid gender raceethnicity latinohispanicflag
	keep `to_keep'
	foreach v in `to_keep' {
		assert !missing(`v')
	}
	
	// xwalk ids
	pg_xwalk, pid([REMOVED]) student_id(studentid) keep
	isid sid
	order sid
	
	// generate school year
	gen school_year = 2020
	
	// create male variable according to CEPR standards
	gen male = .
	replace male = 1 if gender == "M"
	replace male = 0 if gender == "F"
	assert !missing(male)
	fre male
	drop gender
	
	// create race variable according to CEPR standards
	gen race = .
	replace race = 2 if raceethnicity == "[REMOVED]"
	replace race = 1 if raceethnicity == "[REMOVED]"
	replace race = 3 if raceethnicity == "[REMOVED]"
	replace race = 4 if raceethnicity == "[REMOVED]"
	replace race = 7 if raceethnicity == "[REMOVED]"
	replace race = 6 if raceethnicity == "[REMOVED]"
	replace race = 5 if raceethnicity == "[REMOVED]"
	replace race = 3 if latinohispanicflag == "[REMOVED]"
	assert !missing(race)
	fre race
	drop raceethnicity latinohispanicflag
	
	// save prepped data as a tempfile
	tempfile student_attributes
	save `student_attributes'
}


// 2. Prep student school year (including restricting to 7-12 graders)
* Note that student school year contains some of the basic demographic variables,
* like frpl, ell, and iep, that the student attributes file read in above lacked.
{

	// read in raw data
	import delimited "$student_school_year", clear
	pg_trim_strings

	// keep only the variables we need
	local to_keep studentid schoolyear schoolid gradelevel iep_status frpl_status ell_status
	keep `to_keep'
	foreach v in `to_keep' {
		assert !missing(`v')
	}
	
	// xwalk ids
	pg_xwalk, pid(6) student_id(studentid) school_id(schoolid) keep
	drop schoolid // only need to keep local student id, not school id
	order sid
	* I don't check that the file is unique by sid here. I do that at the end
	* of this bracketed code chunk, after dealing with duplicates based on 
	* other recoding.
	
	// rename school year
	rename schoolyear school_year
	replace school_year = school_year + 1
	assert school_year == 2020
	* We have to add 1 for CEPR standards because disF records the school year 
	* as the calendar year of the fall term.
	
	// create grade_level according to CEPR standards
	gen grade_level = .
	replace grade_level = 1 if gradelevel == "01"
	replace grade_level = 2 if gradelevel == "02"
	replace grade_level = 3 if gradelevel == "03"
	replace grade_level = 4 if gradelevel == "04"
	replace grade_level = 5 if gradelevel == "05"
	replace grade_level = 6 if gradelevel == "06"
	replace grade_level = 7 if gradelevel == "07"
	replace grade_level = 8 if gradelevel == "08"
	replace grade_level = 9 if gradelevel == "09"
	replace grade_level = 10 if gradelevel == "10"
	replace grade_level = 11 if gradelevel == "11"
	replace grade_level = 12 if gradelevel == "12"
	replace grade_level = 13 if gradelevel == "[REMOVED]"
	replace grade_level = 0 if gradelevel == "[REMOVED]"
	replace grade_level = -1 if gradelevel == "[REMOVED]"
	replace grade_level = -2 if gradelevel == "[REMOVED]"
	replace grade_level = 13 if gradelevel == "[REMOVED]"
	drop if gradelevel == "[REMOVED]"
	assert !missing(grade_level)
	drop gradelevel
	
	// restrict data to 7-12 graders
	keep if inrange(grade_level, 7, 12)
	
	// create iep according to CEPR standards
	gen sped = .
	replace sped = 1 if iep_status == "Y"
	replace sped = 0 if iep_status == "N"
	assert !missing(sped)
	drop iep_status
	
	// create frpl according to CEPR standards
	gen frpl = .
	replace frpl = 1 if frpl_status == "[REMOVED]"
	replace frpl = 1 if frpl_status == "[REMOVED]"
	replace frpl = 0 if frpl_status == "[REMOVED]"
	assert !missing(frpl)
	drop frpl_status
	
	// create ell according to CEPR standards
	gen ell = .
	replace ell = 1 if ell_status == "[REMOVED]"
	replace ell = 1 if ell_status == "[REMOVED]"
	replace ell = 1 if ell_status == "[REMOVED]"
	replace ell = 0 if ell_status == "[REMOVED]"
	assert !missing(ell)
	drop ell_status
	
	// ensure we've dealt with duplicated students
	drop if sid == [REMOVED] & grade_level == 9 & school_code == [REMOVED]
	isid sid
	
}


// 3. Merge student attributes onto student school year
{
	merge 1:1 sid school_year using `student_attributes', assert(2 3)
	keep if _merge == 3
	drop _merge
	* Every 7-12 grader (with an assigned grade) successfully merged.
}


// 4. Make sure you have all appropriately named covariates for the models
{
	rename grade_level exp_grade
	rename school_code exp_school
	rename male xmale
	rename race xrace
	rename sped xsped
	rename ell xell
	rename frpl xfrpl
}


// 5. Add prior days absent for balance checks
* Note that disF only sends absence and tardy records for their daily attendance, so 
* present kids who had a totally normal day are not included as records. This means 
* summing the absent flag in this dw daily absences file by student and year gives us 
* days absent, but not days enrolled, for the year. So right now, we can only test 
* balance on prior days absent and pr prior days absent (which are the same here), 
* not any absence rates or chronic indicator based on an abs rate.
{
preserve

	// load the data warehouse daily absences file
	import delimited "$dw_daily_abs", clear
	pg_trim_strings
	rename anonymizedstudentid sid
	rename anonymizedschoolid school_code	

	// subset to the 2019 school year
	rename schoolyear school_year
	keep if school_year == 2019
	
	// reformat date
	gen date = dofc(clock(calendardate, "YMD hms"))
	format date %td
	drop calendardate

	// rename the absence indicator variable
	rename absentflag absent

	// deal with duplicate records
	keep sid school_year school_code date absent
	order sid school_year school_code date absent	
	duplicates drop
	bys sid school_year school_code date (absent): keep if _n == _N
	* If the record was a duplicate only on sid, school_year, school_code, 
	* and date, but not on whether the kid was actually absent, then I will
	* consider the kid present, privileging the present record. Maybe these
	* were kids who were marked as absent then came later, but the absence 
	* record was never removed.
	duplicates drop sid school_year date absent, force
	* If a student was marked absent or present at two schools on the same date,
	* just select one record.
	isid sid date
	
	// collapse records to get sum of days absent for the year
	collapse (sum) pr_prior_days_absent = absent prior_days_absent = absent, by(sid school_year)
	isid sid

	tempfile prior_abs_data
	save `prior_abs_data'
	
restore

	// merge prior absence data back onto randomization data
	merge 1:1 sid using `prior_abs_data'
	keep if _merge == 3 | _merge == 1
	drop _merge
	* I do not here add an additional check to make sure that prior grades are
	* generally one below current grades. But I did this check outside this .do
	* file using disF's 2019 and 2020 raw data. They line up in ~99% of cases
	* for returning students.
	
}


// 6. Merge on household identifiers 
{
	merge 1:1 sid school_year using "$student_household", assert(2 3) keepusing(hhld)
	keep if _merge == 3
	drop _merge
	* Every 7-12 grader (with an assigned grade) has a matched household.
}


// 7. Generate household blocking variables
{
	// determine each household lowest-grade child's school and grade
	bys hhld (exp_grade sid): gen block_hhld_school = exp_school[1]
	bys hhld (exp_grade sid): gen block_hhld_grade = exp_grade[1]
}


// 8. Randomize at household level
{

	preserve
		// get to one record per household with household-level vars
		keep hhld school_year block_hhld_school block_hhld_grade
		order hhld school_year block_hhld_school block_hhld_grade
		duplicates drop
		
		// randomize
		pg_randomize treatment, idvar(hhld) seed($seed) blockvars(block_hhld_school block_hhld_grade) arms(2)

		// generate treatment name
		gen treatment_name = ""
		replace treatment_name = "control" if treatment == 0
		replace treatment_name = "robocall" if treatment == 1
		assert !missing(treatment_name)
		
		// save out household-level randomized file
		save "$hhld_randomized", replace
	restore
	
	// merge student-level data back on and save out student-level randomized file
	merge m:1 hhld using "$hhld_randomized", assert(3) nogen
	preserve
		drop studentid // don't want local student id on saved out .dta files
		save "$student_randomized", replace
	restore
}


// 9. Save out treatment, control lists to .csv
{
	
	// for treatment
	preserve
		keep if treatment == 1
		keep studentid treatment_name
		order studentid treatment_name
		export delimited "$treatment_list", replace
	restore
	
	// for control
	preserve
		keep if treatment == 0
		keep studentid treatment_name
		order studentid treatment_name
		export delimited "$control_list", replace
	restore
}


// 10. Generate balance tables and run checks
{

	// student-level covariate tests
	preserve
		keep treatment hhld xmale xrace xsped xell xfrpl exp_grade pr_prior_days_absent
		pg_balance_table, treatvar(treatment) rand_var(hhld) ///
							save("$stud_balance_table")
	restore

	// test household size at household level
	preserve
		keep treatment hhld
		bys hhld: gen N = _N
		duplicates drop
		ttest N, by(treatment)
		putexcel set "$hhld_balance_table", replace
		putexcel A1 = "Household Level Balance Tests"
		putexcel B1 = "Control_mean"
		putexcel C1 = "T1_mean"
		putexcel D1 = "P_val1"
		putexcel A2 = "Household Size"
		putexcel B2 = `r(mu_1)', nformat(number_d3)
		putexcel C2 = `r(mu_2)', nformat(number_d3)
		putexcel D2 = `r(p)', nformat(number_d3)
		putexcel A3 = "N"
		putexcel B3 = `r(N_1)'
		putexcel C3 = `r(N_2)'
	restore

}



cap log close
