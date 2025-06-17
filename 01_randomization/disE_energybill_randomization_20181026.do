/*******************************************************************************
Author: Marina
Date: October 26, 2018
File Name: disE_energybill_randomization_20181026.do
Partner: disE


*** (1) Intervention Details ***
Intervention Name: Energy Bills
Start Date of Intervention: November 13, 2018
Description of Intervention for Each Group:
	T0 = Parents of Students Receive Energy Bill Letters. These letters contain information about 
	the number of absences their child has accrued over the course of the year and a comparison to 
	the average absence rate at his/her school.
	T1 = Control
	
	
*** (2) Randomization Design Details ***
Unit of Randomization: Household
Blocking Variables: homeless_hh schoolcode [REMOVED]_grade_2019
Number of Arms: 2
Ratio T vs. C: 
50:50


*** (3) Evaluation Sample Details ***
Evaluation Sample Criteria:
	- Students with >5% absences last year
	- >45 days enrolled in previous year
	- Students in K-12
	
Evaluation Sample Exclusions:
	- Not enrolled in [REMOVED] alternative schools

Portion of District Treated: Approximately 18% of the district


*** (4) File I/O ***
Starting List for Randomization:
"R:\Proving_Ground\data\raw\Incoming Transfers\20181017 - disE Attendance Data\3-1_Student_Cumulative_Attendance_Summary_20181014.xlsx"


*** (5) Other Pertinent Details/Notes ***

*******************************************************************************/




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
		global pull_date "10/21/2018"
		global xw "R:\Proving_Ground\data\clean\CDW\APT\Prod\disE\CEPRID_Student_Mapping_Extract.csv"
		global students "R:\Proving_Ground\data\raw\Incoming Transfers\20180928 - disE Data\students.csv"
		global geocode "R:\Proving_Ground\data\clean_disE\interim\temp\Export_Output_10012018.txt" 
		global homeless_students "R:\Proving_Ground\data\raw\Incoming Transfers\20180925 - disE Homeless Data\disE_DRT2500_20180923.xlsx"
		global shelters_geocoded "R:\Proving_Ground\data\clean_disE\interim\temp\Export_Output_shelters.txt"
		global attendance_2018 "R:\Proving_Ground\data\raw\Incoming Transfers\20180926 - disE Cumulative Attendance Data\SY1718_EOY_3-1_Student_Cumulative_Attendance_Summary.xlsx"
		global attendance_2019 "R:\Proving_Ground\data\raw\Incoming Transfers\20181017 - disE Attendance Data\3-1_Student_Cumulative_Attendance_Summary_20181014.xlsx"
		global attendance_mailmerge "R:\Proving_Ground\data\raw\Incoming Transfers\20181023 - disE Attendance Data\3-1_Student_Cumulative_Attendance_Summary_20181021.xlsx"
	}

	// Globals - Outputs 
	{
		global date			"20181023"
		global interim		"R:\Proving_Ground\data\pilots\disE_energybill_oct2018\interim"
		global data_out		"R:\Proving_Ground\data\pilots\disE_energybill_oct2018"
		global figures 		"R:\Proving_Ground\tables_figures_new\pilots\disE_energybill_oct2018"
	}
		global seed 12345
	
	// Programs
	do "R:\Proving_Ground\programs_new\aux_dos_analysis\pg_balance_table.do"
	do "R:\Proving_Ground\programs_new\aux_dos_analysis\pg_balance_graph.do"

}


// DW Student IDs
{
	insheet using "$xw", comma clear
	rename anonymizedstudentid sid
	rename localstudentid studentid
	tempfile xw
	save `xw'
}

// Bring in geocoded addresses and combine them with 2019 student information
{		
	import delimited "$geocode", clear

	drop if status == "[REMOVED]" | status == "[REMOVED]"	//drop unmatched or tied addresses from geocode file
	drop status 

	keep match_addr  x y  student_id

	tempfile addresses
	save `addresses'

	import delimited "$students", clear
	merge 1:1 student_id using `addresses'

	rename student_id studentid
	merge m:1 studentid using `xw', keep(1 3) nogen

	//Identify homeless students
	drop _merge
	preserve

		import excel "$homeless_students", sheet("Student Data") firstrow clear
		keep StudentID 
		duplicates drop
		rename StudentID studentid
		gen homeless = 1
		tempfile homeless
		save `homeless'
		
	restore

	merge 1:1 studentid using `homeless'
	drop _merge
	replace homeless = 0 if mi(homeless)
	
	//Identify homeless shelters
	preserve

		import delimited "$shelters_geocoded", clear 
		//Manually geocode one unmatched address
		replace x = [REMOVED] if arc_single == "[REMOVED]"
		replace y = [REMOVED]  if arc_single == "[REMOVED]"
		replace x = [REMOVED] if arc_single == "[REMOVED]"
		replace y = [REMOVED]  if arc_single == "[REMOVED]"
		keep x y arc_single
		gen shelter = 1
		tempfile shelters
		save `shelters'
		
	restore
	merge m:1 x y using `shelters'
	drop if _m == 2
	drop _merge

	replace shelter = 0 if mi(shelter)

	tab homeless shelter
	replace homeless = 1 if homeless == 0 & shelter == 1


	pg_trim_strings

	//Examine contact methods 	
	replace contact_email = "" if contact_email == "[REMOVED]"
	replace contact_email = "" if contact_email == "[REMOVED]"
	gen has_email = !mi(contact_email)
	tab has_email, mi
	gen has_address = !mi(match_addr)
	tab has_address, mi
	tab has_address has_email, mi
	gen has_phone = !mi(contact_phone)
	tab has_phone has_email
	
	
	/**************************************************************************
	Matching households
	Round 1) (80% of records)
		1- match on (arcgis) standardized address and email
		2- match on standardized address and phone
		3- match on standardized address and apartment number [if present]
	Round 2) (for remaining 20% of records)
		1- Email alone
		2- Phone alone
		3- Standardized address and contact name
		4- Standardized address and last name 
		5- Unmatched address and last name
	**************************************************************************/
	
	egen household_address_email = group(match_addr contact_email) if !mi(match_addr) & !mi(contact_email)
	egen household_address_phone = group(match_addr contact_phone) if !mi(match_addr) & !mi(contact_phone) & mi(contact_email)
	egen household_address = group(match_addr apartment) if !mi(match_addr) & !mi(apartment) & mi(contact_email) & mi(contact_phone)
	
	egen check = rowmiss(household_address_email household_address_phone household_address)
	tab check
	
	replace household_address_email = 0 if mi(household_address_email)
	replace household_address_phone = 0 if mi(household_address_phone)
	replace household_address = 0 if mi(household_address)
	
	replace household_address_email = . if check == 3
	replace household_address_phone = . if check == 3
	replace household_address = . if check == 3
	
	egen household = group(household_address_email household_address_phone household_address)
	bys household: gen fam_size = _N
	tab fam_size		// Looks good
	drop fam_size
	
	gen leftover = mi(household)
	tab leftover	//so far, we've captured 80% of the records
	
	rename household household_0
	
	//Dealing with the remaining 20% of records
	
	tab leftover has_email
	tab leftover has_phone
	tab leftover has_address
	
		//Step 1:
			//Use contact email alone
			egen household_1 = group(contact_email ) if !mi(contact_email) & leftover == 1
			bys household_1: gen fam_size = _N
			tab fam_size
			drop fam_size
		
			drop leftover
			gen leftover = mi(household_0) & mi(household_1)
			tab leftover
			
		//Step 2:
			//Use contact phone alone
			egen household_2 = group (contact_phone) if !mi(contact_phone) & leftover == 1
			bys household_2: gen fam_size = _N
			tab fam_size
			drop fam_size
			
			drop leftover
			gen leftover = mi(household_0) & mi(household_1) & mi(household_2)
			tab leftover
			
		//Step 3:
			//Use address and contact name
			egen household_3 = group(match_addr contact_name) if !mi(match_addr) & !mi(contact_name) & leftover == 1
			bys household_3: gen fam_size = _N
			tab fam_size
			drop fam_size
			
			drop leftover
			gen leftover = mi(household_0) & mi(household_1) & mi(household_2) & mi(household_3)
			tab leftover
		
		//Step 4:
			//Use address and surname
			egen household_4 = group(match_addr last_name) if !mi(match_addr) & !mi(last_name) & leftover == 1
			bys household_4: gen fam_size = _N
			tab fam_size
			drop fam_size
			
			drop leftover
			gen leftover = mi(household_0) & mi(household_1) & mi(household_2) & mi(household_3) & mi(household_4)
			tab leftover
			
		//Step 5:
			//Use unmatched address and surname
			gen unmatched_address = string(street_number) + " " + street_name
			egen household_5 = group(unmatched_address last_name) if !mi(unmatched_address) & !mi(last_name) & leftover == 1
			bys household_5: gen fam_size = _N
			tab fam_size
			drop fam_size
			
			drop leftover
			gen leftover = mi(household_0) & mi(household_1) & mi(household_2) & mi(household_3) & mi(household_4) & mi(household_5)
			tab leftover
		
		drop check
		egen check = rowmiss(household_0 household_1 household_2 household_3 household_4 household_5)
		tab check	//all students have been identified within a household
		
		foreach x in 0 1 2 3 4 5 {
	
			replace household_`x' = 0 if mi(household_`x')
	
		}
			
		egen household = group(household_0 household_1 household_2 household_3 household_4 household_5)
		gen flag_0 = household_0 != 0
		bys household: gen fam_size = _N
		tab fam_size flag_0 // compare household sizes between round 1 matching methods and round 2
		drop fam_size
		
		//Keep household information  and bring in disE attendance info for 2018
		keep studentid last_name first_name  household homeless
		isid studentid
		tempfile households
		save `households'
		
		}
		
// disE Attendance Information
	{	
		//DW Student IDs again 
		insheet using "$xw", comma clear
		rename anonymizedstudentid sid
		rename localstudentid studentid
		tempfile xw
		save `xw'

		import excel "$attendance_2018", sheet("Students") firstrow clear case(lower)
		merge m:1 studentid using `xw', keep(1 3) nogen
		
		isid sid schoolcode registration_date exit_date
		
				
		gen ed = date(registration_date,"MDY")
		gen wd = date(exit_date,"MDY")
		format ed wd %td
		drop registration_date exit_date
		// Missing is EOY
		replace wd = td(13jul2018) if mi(wd)

		bys sid schoolcode ed (wd): drop if wd<wd[2] & !mi(wd[2])
		isid sid schoolcode ed

		bys sid schoolcode (ed): gen flag = ed<wd[_n-1] if _n>=2
		tab flag

		// whatever - collapse
		replace grade = "-1" if inlist(grade,"P3","P4","Pre")
		replace grade = "0" if inlist(grade,"K")
		destring grade, gen([REMOVED]_grade) force
		collapse (sum) membershipdays excusedabsences unexcusedabsences inseatabsences (max) [REMOVED]_grade, by(sid studentid)
		rename [REMOVED]_grade [REMOVED]_grade_2018
		rename membershipdays [REMOVED]_days_enrolled_2018
		rename excusedabsences [REMOVED]_days_excused_2018
		rename unexcusedabsences [REMOVED]_days_unexcused_2018
		// looks like this is just total absences.
		rename inseatabsences [REMOVED]_inseatabs_2018

		gen [REMOVED]_days_absent_2018 = [REMOVED]days_excused_2018 + [REMOVED]_days_unexcused_2018
		gen [REMOVED]_chronic_absent_2018 = [REMOVED]_days_absent_2018>=0.1*[REMOVED]_days_enrolled_2018
		
		merge 1:1 studentid using `households'
		
		//We can only care about students who were present in both 2018 and 2019
			//If they're only in 2019, we don't have prior attendance info so they're not part of the study
			//If they're only in 2018, they're no longer enrolled so they're not part of the study
		keep if _m == 3
		drop _merge
		
		//Bring in 2019 disE attendance info
		preserve
		
			//DW Student IDs again #deal with it :P
			insheet using "$xw", comma clear
			rename anonymizedstudentid sid
			rename localstudentid studentid
			tempfile xw
			save `xw'

			import excel "$attendance_2019", sheet("Students") firstrow clear case(lower)
			merge m:1 studentid using `xw', keep(3) nogen
			duplicates drop
			
			isid sid schoolcode registration_date exit_date
			
					
			gen ed = date(registration_date,"MDY")
			gen wd = date(exit_date,"MDY")
			format ed wd %td
			drop registration_date exit_date
			// Missing is EOY
			replace wd = td(13jul2019) if mi(wd)

			bys sid schoolcode ed (wd): drop if wd<wd[2] & !mi(wd[2])
				bys sid schoolcode ed (wd): drop if wd<wd[2] & !mi(wd[2])
			isid sid schoolcode ed

			bys sid schoolcode (ed): gen flag = ed<wd[_n-1] if _n>=2
			tab flag

			// whatever - collapse
			replace grade = "-1" if inlist(grade,"P3","P4","Pre")
			replace grade = "0" if inlist(grade,"K")
			destring grade, gen([REMOVED]_grade) force
			//find latest school attended in 2019
			bys sid (wd): replace schoolcode = schoolcode[_N]
			bys sid (wd): replace schoolname = schoolname[_N]
			
			
			collapse (sum) membershipdays excusedabsences unexcusedabsences inseatabsences (max) [REMOVED]_grade, by(sid studentid schoolcode schoolname)
			rename [REMOVED]_grade [REMOVED]_grade_2019
			rename membershipdays [REMOVED]_days_enrolled_2019
			rename excusedabsences [REMOVED]_days_excused_2019
			rename unexcusedabsences [REMOVED]_days_unexcused_2019
			// looks like this is just total absences.
			rename inseatabsences [REMOVED]_inseatabs_2019

			gen [REMOVED]_days_absent_2019 = [REMOVED]_days_excused_2019 + [REMOVED]_days_unexcused_2019
			gen [REMOVED]_chronic_absent_2019 = [REMOVED]_days_absent_2019>=0.1*[REMOVED]_days_enrolled_2019
			
			tempfile disE_2019
			save `disE_2019'
			
		restore
		
		merge 1:1 sid using `disE_2019'
		
		keep if _m == 3
		drop _m
		save "$interim\disE_attendance_data", replace
}

		//Randomization prep
		
{		
		/*Flag students with 5% or more absences
		and Exclude students who were 'medically out' and those
		with less than 45 days in membership
	
		*/
		gen exclude = [REMOVED]_days_enrolled_2018 < 45
		replace exclude = 1 if schoolname == "[REMOVED]" | schoolname == "[REMOVED]" ///
								| schoolname == "[REMOVED]"
		tab exclude
		drop if exclude == 1
		
		gen meet_target = [REMOVED]_days_absent_2018>=0.05*[REMOVED]_days_enrolled_2018
		keep if meet_target
		
		
		//Identify households with at least one HS student
		gen in_hs = inlist([REMOVED]_grade_2019, 9, 10, 11, 12)
		bys household: egen has_hs_student = max(in_hs)
		drop in_hs
		
		//Identify households with homeless kids
		bys household: egen homeless_hh = max(homeless)
		tab homeless_hh, mi
		
		//Identify earliest grade level in a household
			//If household only has an ungraded student, drop it
		drop if mi([REMOVED]_grade_2019)
		
		save "$interim\disE_eligible_students", replace
		bys household ([REMOVED]_grade_2019 sid): keep if _n == 1
}		
		//Begin randomization
{		
		/*
		2-step process:
		1) Randomize treatment and control for all households
			- block on school and grade
		2) Keep only households with HS students
			- block on school, grade, and 1st round treatment status
		*/
		
		keep  household schoolcode [REMOVED]_grade_2019 has_hs_student homeless_hh
		
		//Step 1
		pg_randomize treatment, idvar(household) seed(12345) blockvars(homeless_hh schoolcode [REMOVED]_grade_2019) arms(2)
		tab treatment, m
		rename treatment treatment_1_num
		
		gen treatment_1 = ""
		replace treatment_1 = "Control" if treatment_1_num == 0
		replace treatment_1 = "parent_energybill" if treatment_1_num == 1 
		tab treatment_1
		
		keep household schoolcode [REMOVED]_grade_2019 treatment_1_num treatment_1 has_hs_student homeless_hh
		save "$interim\disE_pilot_treatment1_hhs.dta", replace
		
		//Step 2
		keep if has_hs_student == 1
		
		pg_randomize treatment, idvar(household) seed(12345) blockvars(homeless_hh schoolcode [REMOVED]_grade_2019 treatment_1_num) arms(2)
		tab treatment, m
		rename treatment treatment_2_num
		
		gen treatment_2 = ""
		replace treatment_2 = "Control" if treatment_2_num == 0
		replace treatment_2 = "student_energybill" if treatment_2_num == 1 
		tab treatment_2
		
		keep household schoolcode [REMOVED]_grade_2019 treatment_2_num treatment_2 has_hs_student homeless_hh treatment_1_num
		save "$interim\disE_pilot_treatment2_hhs.dta", replace
		
		//Prepare student level files
		{
			//All households
			use "$interim\disE_pilot_treatment1_hhs.dta", clear
			rename [REMOVED]_grade block_grade
			keep household block_grade treatment*
		
			merge 1:m household using "$interim\disE_eligible_students"
			keep if _m == 3
			drop _m
			rename treatment_1 treatment_name
				
				//Save different files for treatment and control
				//Control students
				preserve
					keep if treatment_1_num == 0
					keep treatment_name studentid
					sort treatment_name studentid
					export delimited "$data_out\DRAFT_disE_control_parent_energybill_$date", replace 
				restore
				// Treatment students
				preserve
					keep if treatment_1_num == 1
					keep treatment_name studentid
					sort treatment_name studentid
					export delimited "$data_out\DRAFT_disE_treatment_parent_energybill_$date", replace 
				restore	
				
				// Balance check
				if 1{	
				//Need to obtain following variables: male, race, ever_ell, ever_sped, ever_frpl, chronic_absent, abs_rate_2018, days_absent_2018, current_enrolled current_present
					//current_absent current_excused current_unexcused
					tempfile household_randomization
					save `household_randomization'
					
					use "R:\Proving_Ground\data\clean_disE\clean\disE_student_school_year.dta"
					bys sid (school_year): keep if _n == _N
					keep sid ell sped frpl ever_ell ever_sped ever_frpl
					
					merge 1:1 sid using `household_randomization' , keep(2 3)
					drop _m
					
					tempfile ssy
					save `ssy'
					
					use "R:\Proving_Ground\data\clean_disE\clean\disE_student_attributes.dta", clear
					keep sid male race
					
					merge 1:1 sid using `ssy', keep(3)
					drop _m
					
					rename treatment_1_num treatment
					gen current_abs_rate = [REMOVED]_days_absent_2019 / [REMOVED]_days_enrolled_2019
					tab current_abs_rate
					gen current_chronic_absent = current_abs_rate > .1 & [REMOVED]_days_enrolled_2019 >= 20
					
					gen abs_rate_2018 = [REMOVED]_days_absent_2018 / [REMOVED]_days_enrolled_2018
					tab abs_rate_2018
					gen chronic_absent_2018 = abs_rate_2018 > .1 & [REMOVED]_days_enrolled_2018 >= 20
					
					gen current_days_present = [REMOVED]_days_enrolled_2019 - [REMOVED]_days_absent_2019
					
					rename [REMOVED]_grade_2019 [REMOVED]_grade
					
					bys household (sid): egen household_grade = min([REMOVED]_grade)
					
					save "$data_out\DRAFT_disE_energybill_parent_$date.dta", replace
					
					preserve
						rename ([REMOVED]_days_enrolled_2019 [REMOVED]_days_excused_2019 [REMOVED]_days_unexcused_2019 [REMOVED]_days_absent_2019) ///
								(current_days_enrolled current_days_excused current_days_unexcused current_days_absent)
						keep treatment schoolcode block_grade [REMOVED]_grade homeless male race ever_ell ever_sped ever_frpl ///
							chronic_absent abs_rate_2018 [REMOVED]_days_absent_2018 current_days_enrolled  current_days_present ///
							current_days_absent current_days_excused current_days_unexcused
						
						pg_balance_table, blockvars(homeless schoolcode block_grade) save("${figures}\disE_energybill_balance_table_$date")
						*export excel using "${figures}\disE_energybill_balance_table_$date.xlsx", first(var) replace
						drop if regexm(Characteristic, "Days")
						pg_balance_graph, arm1("Control") arm2("Treatment")
						graph export "${figures}\disE_energybill_balance_graph_$date.emf", replace
					restore
				}
				
			//High school households
			use "$interim\disE_pilot_treatment2_hhs.dta", clear
			rename [REMOVED]_grade block_grade
			keep household block_grade treatment*
			merge 1:m household using "$interim\disE_eligible_students"
			keep if _m == 3
			drop _m
			rename treatment_2 treatment_name
			
				//BUT!! Only keep HS students
				keep if inlist([REMOVED]_grade_2019, 9, 10, 11, 12)
		
			//Save different files for treatment and control
				//Control students
				preserve
					keep if treatment_2_num == 0
					keep treatment_name studentid
					sort treatment_name studentid
					export delimited "$data_out\DRAFT_disE_control_student_energybill_HS_$date", replace 
				restore
				// Treatment students
				preserve
					keep if treatment_2_num == 1
					keep treatment_name studentid
					sort treatment_name studentid
					export delimited "$data_out\DRAFT_disE_treatment_student_energybill_HS_$date", replace 
				restore	
				
				// Balance check
				if 1{	
				//Need to obtain following variables: male, race, ever_ell, ever_sped, ever_frpl, chronic_absent, abs_rate_2018, days_absent_2018, current_enrolled current_present
					//current_absent current_excused current_unexcused
					tempfile household_randomization
					save `household_randomization'
					
					use "R:\Proving_Ground\data\clean_disE\clean\disE_student_school_year.dta"
					bys sid (school_year): keep if _n == _N
					keep sid ell sped frpl ever_ell ever_sped ever_frpl
					
					merge 1:1 sid using `household_randomization' , keep(2 3)
					drop _m
					
					tempfile ssy
					save `ssy'
					
					use "R:\Proving_Ground\data\clean_disE\clean\disE_student_attributes.dta", clear
					keep sid male race
					
					merge 1:1 sid using `ssy', keep(3)
					drop _m
					
					rename treatment_2_num treatment
					gen current_abs_rate = [REMOVED]_days_absent_2019 / [REMOVED]_days_enrolled_2019
					tab current_abs_rate
					gen current_chronic_absent = current_abs_rate > .1 & [REMOVED]_days_enrolled_2019 >= 20
					
					gen abs_rate_2018 = [REMOVED]_days_absent_2018 / [REMOVED]_days_enrolled_2018
					tab abs_rate_2018
					gen chronic_absent_2018 = abs_rate_2018 > .1 & [REMOVED]_days_enrolled_2018 >= 20
					
					gen current_days_present = [REMOVED]_days_enrolled_2019 - [REMOVED]_days_absent_2019
					
					rename [REMOVED]_grade_2019 [REMOVED]_grade
					
					save "$data_out\DRAFT_disE_energybill_student_$date.dta", replace
					
					preserve
						rename ([REMOVED]_days_enrolled_2019 [REMOVED]_days_excused_2019 [REMOVED]_days_unexcused_2019 [REMOVED]_days_absent_2019) ///
								(current_days_enrolled current_days_excused current_days_unexcused current_days_absent)
								rename treatment_1_num block_treatment
						keep treatment block_treatment schoolcode block_grade [REMOVED]_grade homeless male race ever_ell ever_sped ever_frpl ///
							chronic_absent abs_rate_2018 [REMOVED]_days_absent_2018 current_days_enrolled  current_days_present ///
							current_days_absent current_days_excused current_days_unexcused
						
						pg_balance_table, blockvars(homeless schoolcode block_grade block_treatment) save("${figures}\disE_energybill_HS_balance_table_$date")
						*export excel using "${figures}\disE_energybill_HS_balance_table_$date.xlsx", first(var) replace
						drop if regexm(Characteristic, "Days")
						pg_balance_graph, arm1("Control") arm2("Treatment")
						graph export "${figures}\disE_energybill_balance_graph_$date.emf", replace
					restore
				}
		
		}
		
		}

//Generate mail merge items

{
	//All Households Mail Merge
{
	use "$interim\disE_pilot_treatment1_hhs.dta", clear
	keep household treatment*

	merge 1:m household using "$interim\disE_eligible_students"
	keep if _m == 3
	drop _m
	rename treatment_1 treatment_name

	keep if treatment_1_num == 1

	//Student last name:			last_name
	//Student first name:				first_name
	//Days of absence this year:		[REMOVED]_days_absent_2019

	//More school/less school/the same amount of school
	drop [REMOVED]_days_absent_2019
	preserve
		import excel "$attendance_mailmerge", sheet("Students") firstrow clear case(lower)
		duplicates drop
		gen ed = date(registration_date,"MDY")
		gen wd = date(exit_date,"MDY")
		format ed wd %td
		drop registration_date exit_date
		// Missing is EOY
		replace wd = td(13jul2019) if mi(wd)

		bys studentid schoolcode ed (wd): drop if wd<wd[2] & !mi(wd[2])
		bys studentid schoolcode ed (wd): drop if wd<wd[2] & !mi(wd[2])
		isid studentid schoolcode ed

		bys studentid schoolcode (ed): gen flag = ed<wd[_n-1] if _n>=2
		tab flag

		// whatever - collapse
		replace grade = "-1" if inlist(grade,"P3","P4","Pre")
		replace grade = "0" if inlist(grade,"K")
		destring grade, gen([REMOVED]_grade) force
		//find latest school attended in 2019
		bys studentid (wd): replace schoolcode = schoolcode[_N]
		bys studentid (wd): replace schoolname = schoolname[_N]

		collapse (sum) membershipdays excusedabsences unexcusedabsences inseatabsences (max) [REMOVED]_grade, by(studentid schoolcode schoolname)
		rename [REMOVED]_grade [REMOVED]_grade_2019
		rename membershipdays [REMOVED]_days_enrolled_2019
		rename excusedabsences [REMOVED]_days_excused_2019
		rename unexcusedabsences [REMOVED]_days_unexcused_2019
		// looks like this is just total absences.
		rename inseatabsences [REMOVED]_inseatabs_2019
		gen [REMOVED]_days_absent_2019 = [REMOVED]_days_excused_2019 + [REMOVED]_days_unexcused_2019
		drop if mi([REMOVED]_grade_2019)

		tempfile days_absent
		save `days_absent'
		
		collapse (mean) [REMOVED]_days_absent_2019, by(schoolcode )
		replace [REMOVED]_days_absent_2019 = round([REMOVED]_days_absent_2019, 1)
		rename [REMOVED]_days_absent_2019 peer_average

		tempfile avg_absences
		save `avg_absences'
	restore
	merge m:1 schoolcode  using `avg_absences', assert (2 3) keep(3) nogen
	merge 1:1 studentid using `days_absent', keepusing([REMOVED]_days_absent_2019) assert(2 3) keep(3) nogen

	gen comparison_words = ""
	replace comparison_words = "more school than" if [REMOVED]_days_absent_2019 > peer_average
	replace comparison_words = "less school than" if [REMOVED]_days_absent_2019 < peer_average
	replace comparison_words = "the same amount of school as" if [REMOVED]_days_absent_2019 == peer_average

	//His/her classmates

	preserve
	import excel "$attendance_mailmerge", sheet("Students") firstrow clear case(lower)
	keep studentid gender
	duplicates drop
	isid studentid
	gen gender_pronoun = "his" if gender == "M"
	replace gender_pronoun = "her" if gender == "F"
	tempfile pronoun
	save `pronoun'
	restore
	merge 1:1 studentid using `pronoun', assert (2 3) keep(3) nogen

	//School Name
	//Attendance Point of Contact

	preserve
		import excel "R:\Proving_Ground\data\raw\Incoming Transfers\20181017 - disE Attendance Data\Attendance POC list_101718 - PG.xlsx", sheet("Sheet1") firstrow case(lower) clear
		keep schoolcode attendancepoc schoolphonenumber 
			set obs 116
			replace schoolcode = 947 if mi(schoolcode)
			replace attendancepoc = "[REMOVED]" if schoolcode == [REMOVED]
			replace schoolphonenumber  = "[REMOVED]0" if schoolcode == [REMOVED]
		tempfile school_contact
		save `school_contact'
	restore

	merge m:1 schoolcode using `school_contact', assert (2 3) keep(3) nogen
	

	keep treatment_name studentid schoolcode first_name last_name [REMOVED]_days_absent_2019 peer_average comparison_words gender_pronoun schoolname attendancepoc schoolphonenumber  
	gen data_pull_date = "$pull_date"

	rename (schoolcode first_name last_name [REMOVED]_days_absent_2019  schoolname attendancepoc schoolphonenumber) ///
	(schoolid student_first_name student_last_name current_ytd_absences school_name attendance_poc school_phone)
	
	// Populate graph title.
	gen title_line1 = ""
	// If student is below peer average, just say that
	replace title_line1 = student_first_name + " has missed fewer school days than" if comparison_words == "less school than"
	// If same as average, report that
	replace title_line1 = student_first_name + " has missed as many school days as" if comparison_words == "the same amount of school as"
	// If above, need to calculate the comparison we want.
	gen ratio = current_ytd_absences / peer_average
	// If ratio is over 1.75, round to nearest integer (double, triple, etc)
	replace title_line1 = student_first_name + " has missed " + string(round(ratio,1)) + " times as many school days as" if comparison_words == "more school than" & ratio >=1.75
	// Otherwise, round to nearest 10%
	// Make sure you don't say 0% more if it would round to 0 though - keep that at 
	replace title_line1 = student_first_name + " has missed about " + string(max(10,100*(round(ratio,.1) - 1))) + "% more school days than" if comparison_words == "more school than" & ratio<1.75
	// All should now have a title
	assert !mi(title_line1)
	drop ratio

	// Second line of the title
	gen title_line2 = gender_pronoun + " classmates so far this year.**"

	order treatment_name studentid schoolid data_pull_date student_first_name student_last_name ///
		current_ytd_absences peer_average comparison_words gender_pronoun school_name ///
		attendance_poc school_phone  title_line1 title_line2
	
	foreach var of varlist treatment_name-title_line2 {
			di "`var'"
			assert !mi(`var')
	}
	
	
	export delimited using "${data_out}\disE_parent_letter_data_$date.csv", replace
	

}

	//High School students mail merge
	
{
	use "$interim\disE_pilot_treatment2_hhs.dta", clear
	keep household treatment*
	merge 1:m household using "$interim\disE_eligible_students"
	keep if _m == 3
	drop _m
	rename treatment_2 treatment_name

	//BUT!! Only keep HS students
	keep if inlist([REMOVED]_grade_2019, 9, 10, 11, 12)
	keep if treatment_2_num == 1

	//Student Name 
	//schoolname
	//days absent in 2018: [REMOVED]_days_absent_2018
	
	/*Goal:
	1) If sid exists on disE mail merge, use their goal
	2) If not, the minimum of 18 vs (disE 2018 absences / 2, rounded up)
	*/

	gen set_goal = round([REMOVED]_days_absent_2018 / 2)
	replace set_goal = 18 if set_goal > 18

	preserve
		import excel "R:\Proving_Ground\data\raw\Incoming Transfers\20181012 - disE MailingMerge\MailMergeFile - disE September mailing.xlsx", sheet("Letter-[REMOVED]") firstrow case(lower) clear
		gen disE_goal = substr(goalstatement, 47, 3)
		destring disE_goal, replace
		keep studentid disE_goal
		replace disE_goal = 18 if disE_goal > 18
		tempfile goal
		save `goal'
	restore

	merge 1:1 studentid using `goal', keep (1 3) nogen


	gen absence_goal = disE_goal if !mi(disE_goal)
	replace absence_goal = set_goal if mi(disE_goal) & mi(absence_goal)
	assert !mi(absence_goal)
	/*On track to meet goal:
	# of abs so far	/ # of days enrolled so far) <	(goal / total days)
	- total days: 	181 if standard
					191 if extended
	*/
	preserve
		import excel "$attendance_mailmerge", sheet("Students") firstrow clear case(lower)
		keep schoolcode calendar
		duplicates drop
		drop if regexm(calendar, "PK")
		isid schoolcode
		tempfile calendar
		save `calendar'
	restore
	merge m:1 schoolcode using `calendar', assert (2 3) keep(3) nogen
	

	gen totaldays = 181 if calendar == "[REMOVED]"
	replace totaldays = 191 if calendar == "[REMOVED]"
	
	drop [REMOVED]_days_absent_2019
	preserve
		import excel "$attendance_mailmerge", sheet("Students") firstrow clear case(lower)
		duplicates drop
		gen ed = date(registration_date,"MDY")
		gen wd = date(exit_date,"MDY")
		format ed wd %td
		drop registration_date exit_date
		// Missing is EOY
		replace wd = td(13jul2019) if mi(wd)

		bys studentid schoolcode ed (wd): drop if wd<wd[2] & !mi(wd[2])
		bys studentid schoolcode ed (wd): drop if wd<wd[2] & !mi(wd[2])
		isid studentid schoolcode ed

		bys studentid schoolcode (ed): gen flag = ed<wd[_n-1] if _n>=2
		tab flag

		// whatever - collapse
		replace grade = "-1" if inlist(grade,"P3","P4","Pre")
		replace grade = "0" if inlist(grade,"K")
		destring grade, gen([REMOVED]_grade) force
		//find latest school attended in 2019
		bys studentid (wd): replace schoolcode = schoolcode[_N]
		bys studentid (wd): replace schoolname = schoolname[_N]

		collapse (sum) membershipdays excusedabsences unexcusedabsences inseatabsences (max) [REMOVED]_grade, by(studentid schoolcode schoolname)
		rename [REMOVED]_grade [REMOVED]_grade_2019
		rename membershipdays [REMOVED]_days_enrolled_2019
		rename excusedabsences [REMOVED]_days_excused_2019
		rename unexcusedabsences [REMOVED]_days_unexcused_2019
		// looks like this is just total absences.
		rename inseatabsences [REMOVED]_inseatabs_2019
		gen [REMOVED]_days_absent_2019 = [REMOVED]_days_excused_2019 + [REMOVED]_days_unexcused_2019

		tempfile days_absent
		save `days_absent'
		
	restore
	merge 1:1 studentid  using `days_absent', keepusing([REMOVED]_days_absent_2019 [REMOVED]_days_enrolled_2019) assert (2 3) keep(3) nogen
	
	gen on_track = ""
	replace on_track = "are" if ([REMOVED]_days_absent_2019 / [REMOVED]_days_enrolled_2019) < (absence_goal / totaldays)
	replace on_track = "are not" if ([REMOVED]_days_absent_2019 / [REMOVED]_days_enrolled_2019) >= (absence_goal / totaldays)

	//School Name
	//Attendance Point of Contact

	preserve
		import excel "R:\Proving_Ground\data\raw\Incoming Transfers\20180917 - disE Attendance POC Data\Attendance POC list - PG.xlsx", sheet("Sheet1") firstrow case(lower) clear
		keep schoolcode attendancepoc schoolphonenumber 
		tempfile school_contact
		save `school_contact'
	restore

	merge m:1 schoolcode using `school_contact', assert (2 3) keep(3) nogen
	
	gen first_sentence = ""
	replace first_sentence = "cut those absences in half" if round([REMOVED]_days_absent_2018 / 2) >= 18 //Attendance goal is half of prior year attendance
	replace first_sentance = "reduce your absences" if ???	//Attendance goal was capped at 18
	
	gen final_sentence = ""
	replace final_sentence = "Together, we can ensure you attend every day the rest of the year." if on_track == "are not"
	replace final_sentence = "Keep up the good work!" if on_track == "are"

	gen student_name = first_name + " " + last_name

	keep treatment_name studentid schoolcode student_name [REMOVED]_days_absent_2019 [REMOVED]_days_absent_2018 absence_goal on_track schoolname attendancepoc final_sentence 
	gen data_pull_date = "$pull_date"

	rename (schoolcode [REMOVED]_days_absent_2019 [REMOVED]_days_absent_2018 on_track schoolname attendancepoc) ///
	(schoolid current_ytd_absences prior_yr_absences on_track_words school_name attendance_poc)

	order treatment_name studentid schoolid data_pull_date student_name current_ytd_absences ///
		prior_yr_absences absence_goal on_track_words school_name attendance_poc final_sentence
		
		foreach var of varlist treatment_name-final_sentence {
			di "`var'"
			assert !mi(`var')
	}
	
	
	export delimited using "${data_out}\disE_student_letter_data_$date.csv", replace

}

}

