/*******************************************************************************
Date: 10/02/2019
File Name: disD_messaging_randomization_oct2019.do
Partner: disD


*** (1) Intervention Details ***
Intervention Name: Automated Messaging
Start Date of Intervention: 10/06/2019
Description of Intervention for Each Group:
	T0 = Does not receive automated message
	T1 = Receives automated message
	
	
*** (2) Randomization Design Details ***
Unit of Randomization: Household
Blocking Variables: block_hhld_school block_hhld_grade
Number of Arms: 2
Ratio T vs. C: 1:1


*** (3) Evaluation Sample Details ***
Evaluation Sample Criteria: All K-12 students in the district
	
Evaluation Sample Exclusions: Students who have no current address in disD raw 
address history (n=112), Pre-K students (we only had 2 pre-K students in 
school year 2020)

Portion of District Treated: 50%


*** (4) File I/O ***
Starting List for Randomization: 
"R:\Proving_Ground\data\clean_disD\disD_dw_files\disD_dw_analysis.dta"
"R:\Proving_Ground\data\clean_disD\clean\disD_student_household.dta"


*** (5) Other Pertinent Details/Notes ***


*******************************************************************************/

// Topmatter
{
	clear all
	set more off, permanently
	set type double
	set seed 100219

	// Globals - Version control
	global type "FINAL"
	global date = string(td($S_DATE),"%tdCYND")
	global seed 100219

	// Globals - Inputs
	global dw_analysis "R:\Proving_Ground\data\clean_disD\disD_dw_files\disD_dw_analysis.dta"
	global households "R:\Proving_Ground\data\clean_disD\clean\disD_student_household.dta"

	
	// Globals - Outputs
	global data "R:\Proving_Ground\data\pilots_2020\disD_messaging_oct2019"
	global tables "R:\Proving_Ground\tables_figures_new\pilots_2020\disD_messaging_oct2019"
	global interim 	"${data}/interim"
	global treatment_list "${data}\\${type}_disD_messaging_treatment_${date}.csv"
	global control_list "${data}\\${type}_disD_messaging_control_${date}.csv"
	global balance_table_file "${tables}\\${type}_disD_messaging_treatment_balance_table_studentlevel_${date}"
	global hhld_size_balance "${tables}\\${type}_disD_messaging_treatment_balance_table_household_${date}"	
}

// Start log
cap log close
log using "${data}\\${type}_disD_messaging_${date}.log", replace


// 1. Load in data, merge on hhld ID, clean up
{
	// Load dw_analysis file and merge
	use "$dw_analysis", clear
	merge m:1 sid school_year using "$households"
	
	// Households file is just 2020 students, so merge numbers seem to make sense
	keep if _merge == 3
	drop _merge
	
	// Only 2 Pre-K students with 2020 records, so drop those
	drop if grade_level==-1
	
	// Two students didn't have a geocode match
	drop if no_stud_geo_match == 1
	
	// rename covariates to follow model data naming conventions
	rename school_code exp_school
	rename grade_level exp_grade
	rename ell xell
	rename sped xsped
	rename male xmale
	rename race xrace
}


// 2. Create household blocking variable
{
	// Identify hhld lowest-grade child's school, grade
	bys hhld (exp_grade sid): gen block_hhld_grade = exp_grade[1]
	bys hhld (exp_grade sid): gen block_hhld_school = exp_school[1]
	
	// Save out student-level file in interim subfolder
	save "${interim}/student_interim_file", replace
}



// 3. Randomize all students at household level
* Blocking on block_hhld_school and block_hhld_grade
* Shoutout to Brian for much of this code, egen BDJ = max(awesomeness)
{
	// Before subsetting, double check that each hhld shares the same hhld-level
	// var values
	count
	local prior_count = r(N)
	local hhld_level_vars pid hhld block_hhld_school block_hhld_grade
	preserve
		keep `hhld_level_vars'
		bys hhld: gen num_records = _N
		duplicates tag `hhld_level_vars', gen(dups)
		gen check = dups + 1
		assert check == num_records
	restore
	keep `hhld_level_vars'
	bys hhld: keep if _n == 1
	isid hhld
	
	// Randomize on block_hhld_school, block_hhld_grade
	pg_randomize treatment, idvar(hhld) seed(100219) ///
							blockvars(block_hhld_school block_hhld_grade) ///
							arms(2)
	
	// Generate treatment_name
	gen treatment_name = "control" if treatment==0
	replace treatment_name = "robocall" if treatment==1
	order treatment_name, after(treatment)
	tab treatment treatment_name
	
	// Save out hhld randomized file
	save "${data}\\${type}_disD_messaging_household_${date}.dta", replace 
	
	// Merge interim student-level data file back onto randomized hhlds
	merge 1:m hhld using "${interim}/student_interim_file", assert(3) nogen
	count
	assert `prior_count' == r(N)
	tab treatment treatment_name
	
	// Save out student-level randomized file
	save "${data}\\${type}_disD_messaging_student_${date}.dta", replace
}


// 4. Save out .csv files of treatment, control lists
{
		// for treatment
		preserve
			keep if treatment == 1
			keep studentid treatment_name
			order studentid treatment_name
			export delimited "$treatment_list", delimiter(",") replace
		restore
		
		// for control
		preserve
			keep if treatment == 0
			keep studentid treatment_name
			order studentid treatment_name
			export delimited "$control_list", delimiter(",") replace
		restore
}


// 5. Check balance
{

	// student-level covariate tests
	preserve
		keep treatment hhld xmale xrace xsped xell prior_days_absent prior_chronic_absent
		pg_balance_table, treatvar(treatment) rand_var(hhld) ///
							save("$balance_table_file")
	restore
	
	// test household size at household level
	preserve
		keep treatment hhld
		bys hhld: gen N = _N
		duplicates drop
		ttest N, by(treatment)
		putexcel set "$hhld_size_balance", replace
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


// 6. Randomization file checks
{
use "${data}\\${type}_disD_messaging_student_${date}.dta", clear
clonevar rand_unit = hhld
gen rand_level = 2
gen arm = 1
replace arm = 2 if treatment==1
pg_randgroup block_hhld_school block_hhld_grade, bin(bin)
pg_check_data, filetype(randomization)
pg_rand_report disD_messaging_oct2019, ///
										log_name("${type}_disD_messaging_${date}.log") ///
										do_name(disD_messaging_randomization_oct2019.do) ///
										treat_data_name("${type}_disD_messaging_treatment_${date}.csv") ///
										head_end(42) balance_name("${type}_disD_messaging_treatment_balance_table_studentlevel_${date}.csv")

save "${data}\\${type}_disD_messaging_student_${date}.dta", replace
}

