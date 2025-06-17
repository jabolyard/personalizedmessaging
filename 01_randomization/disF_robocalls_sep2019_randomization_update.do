/*******************************************************************************

	File name: disF_robocalls_sep2019_randomization_update.do
	Purpose: The DW team accidentally sent the control list of students to disF
			 instead of the treatment list. This short do file reads in the 
			 original randomized files, swaps treatment and control in the 
			 treatment variable, and saves a new randomized file with a new name.
			 It will also re-generate updated treatment and control list files
			 as well as balance tables.
	Date Created: 12/18/19
	Date Updated: 
	

*******************************************************************************/


// Set log
cap log close
log using "R:/Proving_Ground/tables_figures_new/pilots_2020/disF_robocalls_sep2019/UPDATE_disF_robocalls_sep2019_randomization.log", replace


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
			global date = string(date(c(current_date), "DMY"), "%tdCCYYNNDD")
			// original randomized file
			global pilot_dir 					"R:/Proving_Ground/data/pilots_2020/disF_robocalls_sep2019"
			global original_randomized_hhld		"${pilot_dir}/FINAL_disF_robocalls_sep2019_household_20190909.dta"
			global original_randomized_student	"${pilot_dir}/FINAL_disF_robocalls_sep2019_students_20190909.dta"
			// student id xwalk for disF
			global student_xwalk				"R:/Proving_Ground/data/clean/CDW/APT/Prod/disF/CEPRID_Student_Mapping_Extract_valid_ids.csv"
		// Outputs
			global updated_randomized_hhld		"${pilot_dir}/UPDATE_disF_robocalls_sep2019_household_${date}.dta"
			global updated_randomized_student	"${pilot_dir}/UPDATE_disF_robocalls_sep2019_students_${date}.dta"
			global updated_treatment_list		"${pilot_dir}/UPDATE_disF_robocalls_treatment_${date}.csv"
			global updated_control_list			"${pilot_dir}/UPDATE_disF_robocalls_control_${date}.csv"
			global fig_dir						"R:/Proving_Ground/tables_figures_new/pilots_2020/disF_robocalls_sep2019"
			global updated_stud_balance_table 	"${fig_dir}/UPDATE_disF_robocalls_student_balance_${date}"
			global updated_hhld_balance_table	"${fig_dir}/UPDATE_disF_robocalls_hhld_balance_${date}"
	}

}



// Read in original randomized hhld file, swap treatment and control, and
// re-save file with new name
{
	use "$original_randomized_hhld", clear

	fre treatment
	rename treatment original_treatment
	gen treatment = (original_treatment != 1)
	fre treatment

	fre treatment_name
	rename treatment_name original_treatment_name
	gen treatment_name = ""
	replace treatment_name = "control" if original_treatment_name == "robocall"
	replace treatment_name = "robocall" if original_treatment_name == "control"
	fre treatment_name

	assert !missing(treatment) & !missing(treatment_name)

	tab original_treatment original_treatment_name
	tab treatment treatment_name

	drop original_treatment original_treatment_name

	save "$updated_randomized_hhld", replace
}



// Read in original randomized student file, swap treatment and control, and 
// re-save file with new name
{
	use "$original_randomized_student", clear

	fre treatment
	rename treatment original_treatment
	gen treatment = (original_treatment != 1)
	fre treatment

	fre treatment_name
	rename treatment_name original_treatment_name
	gen treatment_name = ""
	replace treatment_name = "control" if original_treatment_name == "robocall"
	replace treatment_name = "robocall" if original_treatment_name == "control"
	fre treatment_name

	assert !missing(treatment) & !missing(treatment_name)

	tab original_treatment original_treatment_name
	tab treatment treatment_name

	drop original_treatment original_treatment_name

	save "$updated_randomized_student", replace
}



// Save out updated treatment and control lists to .csv files
{

	// temporarily merge back on local student ids
	preserve
		import delimited "$student_xwalk", clear
		rename anonymizedstudentid sid
		rename localstudentid studentid
		tempfile local_student_ids
		save `local_student_ids'
	restore
	count
	local original_number = r(N)
	merge 1:1 sid using `local_student_ids', nogen keep(3)
	count
	assert `original_number' == r(N)
	

	// for treatment
	preserve
		keep if treatment == 1
		keep studentid treatment_name
		order studentid treatment_name
		export delimited "$updated_treatment_list", replace
	restore
	
	// for control
	preserve
		keep if treatment == 0
		keep studentid treatment_name
		order studentid treatment_name
		export delimited "$updated_control_list", replace
	restore


	// drop local student ids again
	drop studentid
	
}



// Generate new balance tables
{

	// student-level covariate tests
	preserve
		keep treatment hhld xmale xrace xsped xell xfrpl exp_grade pr_prior_days_absent
		pg_balance_table, treatvar(treatment) rand_var(hhld) ///
							save("$updated_stud_balance_table")
	restore

	// test household size at household level
	preserve
		keep treatment hhld
		bys hhld: gen N = _N
		duplicates drop
		ttest N, by(treatment)
		putexcel set "$updated_hhld_balance_table", replace
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
