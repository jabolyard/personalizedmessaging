
// Topmatter
{
	// Set Up
	{
		clear all
		set seed 12345
		set more off
		set type double
	}

	// Globals - Inputs
	{
		global xw "R:\Proving_Ground\data\clean\CDW\APT\Prod\disC\CEPRID_Student_Mapping_Extract.csv"
		global clean "R:\Proving_Ground\data\clean_disC\clean"
		global students_raw "R:\Proving_Ground\data\raw\Incoming Transfers\20180924 - disC Data\PG_Student_Attributes.csv"
		global geocode "R:\Proving_Ground\data\clean_disC\interim\geocoded_addresses.csv" 
		global shelters_geocoded "R:\Proving_Ground\data\[REMOVED]\interim\temp\Export_Output_shelters.txt"
	}

	// Globals - Outputs 
	{
		global date			"20181022"
		global interim		"R:\Proving_Ground\data\pilots\disC_energybill_oct2018\interim"
		global data_out		"R:\Proving_Ground\data\pilots\disC_energybill_oct2018"
		global figures 		"R:\Proving_Ground\tables_figures_new\pilots\disC_energybill_oct2018"
	}
	
	// Programs
	do "R:\Proving_Ground\programs_new\aux_dos_analysis\pg_balance_table.do"
	do "R:\Proving_Ground\programs_new\aux_dos_analysis\pg_balance_graph.do"

}

// Prepare data
{
	//Log
	cap log close
	log using "R:\Proving_Ground\data\pilots\disC_energybill_oct2018\randomization_log_$date.log", replace
	
	//Bring in latest student attributes file (with address/email info)
	
	import delimited "$students_raw", clear
	
	//Merge on ID crosswalk
	preserve
		import delimited "$xw", stringcols (1/2) clear
		destring local, replace
		*bysort localstudentid : drop if _N > 1 // 6 IDs are duplicated in the crosswalk due to leading zeros
		bysort localstudentid (anonymizedstudentid): keep if _n ==1
		destring anonymizedstudentid , replace
		rename localstudentid studentid
		tempfile ids
		save `ids'
	restore

	merge m:1 studentid using `ids'
	keep if _m == 3
	drop _m
	
	rename anonymized sid
	
	//Bring in school enrollment information
	merge 1:m sid using "${clean}/clean_disC_spells.dta"
	keep if  _m == 3	//3 students in _m==1 that don't show up in enrollments at all (not even raw)
	drop _m
	
	//Keep 2019 records for students that are still enrolled
	keep if school_year == 2019
	keep if withdraw_code == 4
	
	bys studentid: drop if _N == 2 & withdraw_date != td("[REMOVED]")
	
	isid studentid
	
	//Get rid of extraneous contact information
	drop [REMOVED]
	
	pg_trim_strings
	pg_replace_dashes
	
	//Prepare to use email information
	format parent1_email %30s
	rename parent1_email parent_email
	
	gen has_email = !mi(parent_email)
	tab has_email, mi	//missing for 11.5%
	
	bys parent_email: gen size = _N
	
	bys parent_email: egen check_differences_address = nvals([REMOVED])
	bys parent_email: egen check_differences_phone = nvals([REMOVED] )
	tab check_differences_address
	tab check_differences_phone
	drop check_differences*
	
	//this line generates address from disparate address columns
	gen address = [REMOVED] + " " + [REMOVED] + " " + [REMOVED] + " " + [REMOVED]
	
	preserve
	
	/*
	//Save out address data for geocoding
	preserve
		keep studentid address
		 export delimited using "R:\Proving_Ground\data\clean_disC\interim\student_addresses.csv", replace
	restore
	*/
	
		 import delimited $geocode, clear 
		 keep if status == "[REMOVED]"
		 keep studentid match_addr arc_single x y
		 tempfile geocoded
		 save `geocoded'
	 
	 restore
	 
	 merge 1:1 studentid using `geocoded'
	
	bys parent_email: egen check_address_diff = nvals(match_addr)
	tab check_address_diff	//84% (3461) of emails have one address only. 92 emails have 2 addresses. 1 email has 3.
	
	//Identify homeless shelters
	drop _merge
	preserve

		import delimited "$shelters_geocoded", clear 
		//Manually geocode one unmatched address
		replace x = [REMOVED] if arc_single == "[REMOVED]"
		replace y = [REMOVED] if arc_single == "[REMOVED]"
		replace x = [REMOVED] if arc_single == "[REMOVED]"
		replace y = [REMOVED]  if arc_single == "[REMOVED]"
		keep x y arc_single
		gen shelter = 1
		tempfile shelters
		save `shelters'
		
	restore
	merge m:1 x y using `shelters'
	drop if _m == 2
	gen homeless = _m == 3
	drop _m
	
	//Identify households
	//1st households with email, then households without email
	egen household_email = group(parent_email)
	bys household_email: gen fam_size = _N
			tab fam_size
			drop fam_size
			
	gen leftover = mi(household_email)
	tab leftover
	//Then households without
	count if !mi(parent1_cell_phone ) & has_email == 0	//139
	count if !mi(parent1_home_phone  ) & has_email == 0 //50
	count if !mi(parent1_last ) & has_email == 0	//161

	egen household_cellphone = group(parent1_cell_phone) if leftover==1
	bys household_cellphone: gen fam_size = _N
	tab fam_size
	drop fam_size

	drop leftover
	gen leftover = mi(household_email) & mi(household_cellphone)
	tab leftover		
	  
	egen household_address = group(lastname studentstreetaddress) if leftover==1
	bys household_address: gen fam_size = _N
		tab fam_size
		drop fam_size
	
	drop leftover
			gen leftover = mi(household_email) & mi(household_cellphone) & mi(household_address)
			tab leftover		//one student with no info except his name

			
	replace household_email = 0 if mi(household_email)	
	replace household_cellphone = 0 if mi(household_cellphone)
	replace household_address = 0 if mi(household_address)
			
	egen household = group(household_email household_cellphone household_address)
		gen flag_0 = household_email != 0
		bys household: gen fam_size = _N
		tab fam_size flag_0 // compare household sizes between round 1 matching methods and round 2
		drop fam_size flag_0
	
	gen missing_email =  household_email == 0
	
	//Keep household information  and bring in [REMOVED] demographic info, and attendance info for 2018
	keep studentid sid school_year school_code homeless household missing_email
	
	merge 1:1 sid using  "${clean}\disC_student_attributes.dta"
	drop if _m==2
	drop _m partner pid date_of_birth
	
	merge 1:1 sid school_year using "${clean}\disC_student_school_year.dta"
	drop if _m==2
	drop pid frpl ever_frpl ever_sped ever_ell _merge
	
	preserve
		use "${clean}\clean_disC_annual_absences.dta", clear
		keep if school_year == 2018
		keep sid days_absent days_enrolled
		rename (days_absent days_enrolled) (days_absent_2018 days_enrolled_2018)
		tempfile abs_2018
		save `abs_2018'
	restore
	
	merge 1:1 sid using `abs_2018'
	drop if _m==2
	drop _m
	
	gen [REMOVED]_eligible = !mi(days_enrolled_2018) & days_enrolled_2018 >= 45  & ///
		!mi(days_absent_2018) & days_absent_2018 <= 50 & ///
		days_absent_2018>=0.05*days_enrolled_2018
	
	//Bring down to household level
	
		//Identify households with homeless kids
		bys household: egen homeless_hh = max(homeless)
		tab homeless_hh, mi
		
		//Identify households without email
		bys household: egen household_noemail = max(missing_email)
	
		//Identify [REMOVED]-eligible households
		bys household: egen [REMOVED]_flag = max([REMOVED]_eligible)
	
		
		save "${interim}\[REMOVED]_eligible_students.dta", replace
		
}		
		
		//Begin randomization
{		
		
		//Identify earliest grade level in a household
		bys household (grade_level sid): keep if _n == 1
		
		/*
		1) Randomize treatment and control for all households
			- block on school and grade
		*/
		
		keep  household school_code grade_level  homeless_hh household_noemail [REMOVED]_flag
		
		gen household_block = 0
		replace household_block = 1 if household_noemail == 1 
		replace household_block = 2 if household_block == 0 & homeless == 1
		//Step 1
		pg_randomize treatment, idvar(household) seed(12345) blockvars(household_block school_code grade_level [REMOVED]_flag) arms(2)
		tab treatment, m
		rename treatment treatment_1_num
		
		gen treatment_1 = ""
		replace treatment_1 = "Control" if treatment_1_num == 0
		replace treatment_1 = "parent_email" if treatment_1_num == 1 
		tab treatment_1
		
		*keep household schoolcode [REMOVED] treatment_1_num treatment_1 has_hs_student homeless_hh
		save "${data_out}\FINAL_disC_energybill_hhld_$date.dta", replace
		
		//Prepare student level files
		{
			//All households
			use "${data_out}\FINAL_disC_energybill_hhld_$date.dta", clear
			rename grade_level block_grade
			keep household* treatment* block_grade 
		
			merge 1:m household using "${interim}\[REMOVED]_eligible_students.dta", assert(3) nogen
			rename treatment_1 treatment_name
			
			save "${data_out}\FINAL_disC_energybill_student_$date.dta", replace
				
				//Save different files for treatment and control
				//Control students
				preserve
					keep if treatment_1_num == 0
					keep treatment_name studentid
					sort treatment_name studentid
					export delimited "$data_out\FINAL_disC_control_parent_energybill_$date" //, replace 
				restore
				// Treatment students
				preserve
					keep if treatment_1_num == 1
					keep treatment_name studentid
					sort treatment_name studentid
					export delimited "$data_out\FINAL_disC_treatment_parent_energybill_$date" //, replace 
				restore	
				
				// Balance check
				if 1{	
				//Need to obtain following variables: male, race, ever_ell, ever_sped, ever_frpl, chronic_absent, abs_rate_2018, days_absent_2018, current_enrolled current_present
					//current_absent current_excused current_unexcused
	
					preserve
						keep treatment_1_num school_code block_grade grade_level homeless male race sped days_absent_2018 days_enrolled_2018 dcps_flag household_block
						rename 	treatment_1_num treatment
						gen chronic_absent = days_absent_2018>=0.1*days_enrolled_2018 if !mi(days_enrolled) & !mi(days_absent) & days_enrolled >= 20
						
						pg_balance_table, blockvars(household_block school_code block_grade [REMOVED]_flag) save("${figures}\disC_energybill_balance_table_$date")
						*export excel using "${figures}\disC_energybill_balance_table_$date.xlsx", first(var) replace
						
						drop if regexm(Characteristic, "Days")
						pg_balance_graph, arm1("Control") arm2("Treatment")
						graph export "${figures}\disC_energybill_balance_graph_$date.emf", replace
					restore
				}
				}
		