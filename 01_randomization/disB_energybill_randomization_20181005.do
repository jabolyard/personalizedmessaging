/*******************************************************************************

	File: 				disB_energybill_randomization.do
	Date: 				October 11th, 2018
	Purpose: 			Simulation to estimate power for energybill pilot
						Randomized at the house-hold level
	
	Steps:				
						1) Clean the sample:
							a) Start from SSY + Attributes + Enrollment
							b) Merge the contact information (address)
							b) Merge school info: include only elementary, middle, high,and alternative schools
							c) Keep grade level 2-12 
							d) Exclude chronically ill kids ("Homebound")
						
						2) The samples are divided into three household groups:
							a) Observations with known guardian address
							b) Obs with unknown(missing) guardian address
							c) Obs with the "group home" address
								Notes: We define a household solely on the guardian address.
								Households with more than 10 students are grouped as "group home". 
								
						3) Stratification
							a) Household group (described above)
							b) School
							c) Grade Level
							
*******************************************************************************/
// Topmatter
{
	// Set Up
		clear all
		cap log close
		set more off, permanently
		set seed 12345

	// Globals - pilot info
		global partner 		"disB"
		global pilot		"energybill"
		global date			"20181011"
		
		// Globals - Inputs
		global raw			"R:\Proving_Ground\data\raw\Incoming Transfers\20180209 - disB Data"
		global raw_14		"R:\Proving_Ground\data\raw\Incoming Transfers\20180213 - disB 2014 Data"
		global raw_18		"R:\Proving_Ground\data\raw\Incoming Transfers\20180702 - disB 2018 Data"
		global raw_19 		"R:\Proving_Ground\data\raw\Incoming Transfers\20181003 - disB Data"
		global raw_contact 	"R:\Proving_Ground\data\raw\Incoming Transfers\20181005 - disB Energy Bill"

		global clean		"R:\Proving_Ground\data\clean_disB\clean"
		global abs_period 	"R:\Proving_Ground\data\clean_disB\int\absence_period"

	// DW Crosswalk
		global xw_stu		"R:\Proving_Ground\data\clean\CDW\APT\Prod\disB\CEPRID_Student_Mapping_Extract_valid_ids"		
		global xw_sch		"R:\Proving_Ground\data\clean\CDW\APT\Prod\disB\CEPRID_School_Mapping_Extract_valid_ids"		

	// Globals - Outpus
		global interim		"R:\Proving_Ground\data\pilots\disB_energybill_oct2018\interim"
		global data_out		"R:\Proving_Ground\data\pilots\disB_energybill_oct2018"
		global figures 		"R:\Proving_Ground\tables_figures_new\pilots\disB_energybill_oct2018"

	// Programs
}

cap log close
log using "R:\Proving_Ground\data\pilots\disB_energybill_oct2018\randomization_log_$date.log", replace

// Base file: identify students who are in SSY + Attributes + Enrollment + Contact Info 
{	
	// 1)SSY
	import delimited "$raw_19\StudentSchoolYear", clear
	keep studentid gradelevel frpl_status iep_status ell_status homeless* immigrant*
	duplicates drop // 142 dropped
	isid studentid
	gen flag_ssy = 1
	
	// 2)Student Attributes
	preserve
		import delimited "$raw_19\StudentAttributes", clear
		keep studentid gender raceethnicity  
		isid studentid	
		gen flag_attr = 1 
		tempfile stu_name
		save `stu_name'
	restore
	
	merge 1:1 studentid using `stu_name', gen(m_attr) 
		* 1690 not merged- ssyonly: 980, attronly: 710 
	
	// 3)School Enrollment Spells
	preserve
		import delimited "$raw_19\StudentSchoolEnrollment", clear
		tab withdrawaldate
		
		// format dates
			gen enroll_date = date(enrollmentdate, "YMD")
			format enroll_date %td
			gen withdraw_date = date(withdrawaldate, "YMD")
			format withdraw_date %td
			
		// keep the latest records of each student
			bysort studentid (withdraw_date): gen flag_spells = 1 if _n == _N
			keep if flag_spells == 1 // 4848 obs deleted

		keep studentid schoolid enroll_date withdraw_date withdrawalcode flag* entrygradelevel
		tempfile stu_spells
		save `stu_spells'
	restore

	merge 1:m studentid using `stu_spells', gen(m_spells)
		* spells_only: 4192 (not merged)
		
	// We have two grade level information: check if they match (SSY and Spells files)
	assert (gradelevel == entrygradelevel)

	// 4) Contact Information: Addresses
	preserve
		import excel "$raw_contact\Energy Bill Addresses - Final", first clear
		rename student_id studentid
		isid studentid
		gen flag_contact = 1 
		tempfile address
		save `address'
	restore
		
	merge 1:1 studentid using `address', gen(m_contact)
		* original_only: 5390, contact_only: 0, merged: 21465
		
	// Save
	save "$interim\energybill_basefile", replace
}	

// Investigate the basefile merge issues
{
	
	use "$interim\energybill_basefile", clear 
	
	// Check enrollment spells: some students exit the system and we haven't dropped them yet
	gen withdraw_code = .
	
	// Transferred out
	replace withdraw_code = 1 if inlist(withdrawalcode, "[REMOVED]") // 590
	replace withdraw_code = 1 if inlist(withdrawalcode, "[REMOVED]") //213
	replace withdraw_code = 1 if inlist(withdrawalcode,"[REMOVED]") // 4

	// Dropped out
	replace withdraw_code = 2 if inlist(withdrawalcode, "[REMOVED]") //2
	replace withdraw_code = 2 if inlist(withdrawalcode,"[REMOVED]") // 131
	
	// Graduated 
	replace withdraw_code = 3 if inlist(withdrawalcode, "[REMOVED]") // 84

	// Still Enrolled
	replace withdraw_code = 4 if inlist(withdrawalcode, "[REMOVED]") // 1
	
	assert !mi(withdraw_code) if !mi(withdrawalcode) // there is no remaining withdrawalcode that weren't mapped to CEPR standard spell codes.
	replace withdraw_code = 4 if mi(withdrawalcode) // missing withdrawalcode in raw spells file -> assuming they are still enrolled
	assert !mi(withdraw_code)
	
	// Remove students who transferred out/ dropped out or graduated.
	drop if inlist(withdraw_code, 1, 2, 3) // 1024 dropped
	assert withdraw_code == 4 
	
	// Replace missing to 0 in flag* vars
	foreach var of varlist flag_ssy flag_attr flag_spells flag_contact{
		replace `var' = 0 if mi(`var')
		tostring(`var'), replace
	}
	
	// Checking in which files students appear
	gen group = flag_ssy + flag_attr + flag_spells + flag_contact
	tab group, m
	
	gen group_desc = ""
	replace group_desc = "[REMOVED]" if group == "[REMOVED]"
	replace group_desc = "[REMOVED]" if group == "[REMOVED]"
	replace group_desc = "[REMOVED]" if group == "[REMOVED]"
	replace group_desc = "[REMOVED]" if group == "[REMOVED]"

	replace group_desc = group if mi(group_desc)
	
	tab group_desc, m
		* 20,767 in all four; 3,956 only in [REMOVED]; 210 in [REMOVED]; 668 all but attr.
		* Browing on the 4k students who are only in [REMOVED], thier schools seem to be ones we don't care about.
	
	// Merging school file
	preserve
		import delimited "$raw_19\School", clear
		keep if elementary == 1 | middle == 1 | high == 1 | alternative == 1  
		keep schoolid schoolname 
		tempfile schools
		save `schools'
	restore
	
	merge m:1 schoolid using `schools', gen(m_sch)
	
	keep if m_sch == 3 // 4924 deleted
	
	tab group_desc, m
	
	// Grade Level: 2-12 only
	keep if inrange(gradelevel, 2, 12) // 4200 dropped
	count // 16596
	
	// Drop chronically ill students; 
	preserve
		import delimited "$raw_18\StudentDailyAttendance.csv", clear 
		tempfile daily_18
		save `daily_18'
	restore
	
	preserve
		import delimited "$raw_19\StudentDailyAttendance.csv", clear
		tempfile daily_19
		save `daily_19'
	restore
	
	preserve
		use `daily_18', clear
		append using `daily_19'
		save "$interim\daily_attendance_18_19", replace
	restore
	
	preserve
		use "$interim\daily_attendance_18_19.dta", clear 
		keep if inlist(schoolyear, 2018, 2019)
		keep studentid periodabsencecode
		
		// Flag students with [REMOVED] in the period level data 
		gen homebound = 0 
		replace homebound = 1 if regexm(periodabsencecode, " [REMOVED]")
		gen homebound_h = 0 
		replace homebound_h = 1 if regexm(periodabsencecode, "[REMOVED]")
		tab homebound homebound_h // confirming that I am not miscounting
		
		// sum up how many time [REMOVED] appears in period level absence records
		collapse (sum) homebound, by(studentid)
		save "$interim\homebound", replace
	restore
	
	merge 1:1 studentid using "$interim\homebound", keep(1 3) // just 11 students only in original base file
	tab homebound 
	
	// Drop all students with at least one Homebound in their period level absences
	keep if mi(homebound) | homebound == 0  // 307 dropped

	drop homebound _merge 
	
	count // 16,388
	
// save
	save "$interim\disB_energybill_clean",replace
}

// Merge anonymized studentid + schoolid
if 1{
		preserve
			import delimited "$xw_stu", clear
			rename localstudentid 		studentid
			rename anonymizedstudentid  sid
			drop if mi(studentid) // 0 dropped
			format sid studentid %20.0g
			tempfile xw_stu
			save `xw_stu'
		restore
		merge m:1 studentid using `xw_stu', keep(1 3) gen(m_xw_stu)
		assert m_xw_stu == 3
		
		preserve
			import delimited "$xw_sch", clear
			rename localschoolid 		schoolid
			rename anonymizedschoolid  school_code
			drop if mi(school_code) // 0 dropped
			format school_code schoolid %20.0g
			tempfile xw_sch
			save `xw_sch'
		restore

		merge m:1 schoolid using `xw_sch', nogen keep(1 3) 
			* _m1: 0, _m2: 194, _m3: 15,554,505	
}

// Format to CEPR standard and merge absence info
{	
	// Gender
		gen male = (gender == "M")
		tab male, m
		
	// Race
		gen race = .
		replace race = 1 if regexm(raceethnicity, "[REMOVED]")
		replace race = 2 if regexm(raceethnicity, "[REMOVED]")
		replace race = 3 if regexm(raceethnicity, "[REMOVED]")
		replace race = 4 if regexm(raceethnicity, "[REMOVED]")
		replace race = 5 if regexm(raceethnicity, "[REMOVED]")
		replace race = 6 if regexm(raceethnicity, "[REMOVED]")
		replace race = 7 if regexm(raceethnicity, "[REMOVED]")
		tab race, m

	// Rename
		rename gradelevel grade_level
		rename iep_status sped
		rename frpl_status frpl
		rename ell_status ell
	
	// Frpl: since disB is CEP district, we set frpl and ever_frpl = 0 
		replace frpl = 0
		gen ever_frpl = 0
		
	// Cep
		gen cep = 1
		// Label
		label define cep 0 "Not CEP" 1 "CEP"
		label value cep cep
		
	// Ell
		replace ell = "1" if ell == "Y"
		replace ell = "0" if ell == "N"
		destring ell, replace
		// Label
		label define ell 0 "Not ELL" 1 "ELL" 
		label value ell ell
		// Ever_ell
		bysort sid: egen ever_ell = max(ell)

	// Sped
		replace sped = "1" if sped == "Y"
		replace sped = "0" if sped == "N"
		destring sped, replace
		// Label
		label define sped 0 "No Disability" 1 "Has Disability" 
		label value sped sped
		// Ever_sped
		bysort sid: egen ever_sped = max(sped)
	
	// Homeless
		replace homelessflag = "1" if homelessflag == "Y"
		replace  homelessflag= "0" if homelessflag == "N"
		destring homelessflag, replace
		rename homelessflag homeless
		// Label
		label define homeless 0 "Non-homeless status" 1 "Homless status" 
		label value homeless homeless
		// Ever_homeless
		bysort sid: egen ever_homeless = max(homeless)
	
	// Immigrant: clean the variable in SSY but merge on Attribute file.
		replace immigrantflag = "1" if immigrantflag == "Yes"
		replace  immigrantflag= "0" if immigrantflag == "No"
		destring immigrantflag, replace
		bysort sid: egen immigrant_max = max(immigrantflag)
		drop immigrantflag
		rename immigrant_max immigrant	
		// Label
		label define immigrant 0 "Non-immigrant" 1 "Immigrant" 
		label value immigrant immigrant
	

	// Merge absence records from 2018 using analysis file
		preserve	
			use "$clean\disB_analysis_file", clear
			keep if school_year == 2018
			keep days_enrolled days_absent abs_rate chronic_absent sid
			rename * *_2018
			rename sid_2018 sid
			tempfile 2018_abs_info
			save `2018_abs_info'
		restore
		
		merge 1:1 sid using `2018_abs_info', keep(1 3) nogen
	
	// Merge 2019 absence records
		preserve
			import delimited "$raw_19\StudentAttendanceSummary", clear
			assert schoolyear == 2019
			drop daysof* schoolyear
			rename days* current_*_2019
			collapse (sum) current*, by(studentid)
			tempfile 2019_abs_info
			save `2019_abs_info'
		restore
	
		merge 1:1 studentid using `2019_abs_info'
		drop if _merge == 2 
		
	save "$interim\disB_energybill_clean", replace
}

// Define household only by guardian addresses on student level data
{
	use  "$interim\disB_energybill_clean", clear
	pg_trim_strings
	count // 16265
	isid sid
	
	bysort address unit: egen num_stu = nvals(sid)
	tab num_stu // vary by 1-19, 11, 17, 18, 30, 41

	order address unit contact_id contact_first_name contact_last_name student_last_name
	
	tab address if num_stu > 10
			* One address with 11 people

	// students with missing guardian addresses:  replace the streetname with "missing_number" 
	count if mi(address) // 14
	
	sort address unit contact_id sid
	gen temp_num = _n
	tostring(temp_num), replace
	replace address = "missing_" + temp_num if mi(address)
	
	// combine address and unit and group them to number the household: household_prera
	gen address_unit = address + " " + unit
	pg_trim_strings	
	egen household_prera = group(address_unit) // 9289/16265

	order address unit address_unit household_prera
	sort household_prera
	
	drop address_unit temp* 
	// save the student level data so that we can merge back after randomization
	save "$interim\disB_energybill_clean_studentlevel", replace
}

// Collapse down to household level: keeping only the lowest grade level student
{
	// For each household, keep the record of the student with lowest grade level
	bysort address unit(grade_level): keep if _n == 1 // 6976 dropped
	
	count // 9,289
	unique address unit // 9289

	// Stratification
		// Flag for missing address: 14
		gen mi_address = 0
		replace mi_address = 1 if regexm(address, "missing") 
		
		// Flag for group homes
		gen group_home = 0 
		replace group_home = 1 if num_stu > 10 & mi_address != 1  // 4
	
		tab1 group_home mi_address
	
		// Group using the flags
		gen household_block = .
		replace household_block = 1 if group_home != 1 & mi_address != 1  // 9271: regular households
		replace household_block = 2 if group_home != 1 & mi_address == 1 // 14: missing addresses
		replace household_block = 3 if group_home == 1 & mi_address != 1 // 4: group homes
	
		assert !mi(household_block)
	
	save "$interim\disB_energybill_clean_household", replace
}	

// Randomize
if 0{
	use  "$interim\disB_energybill_clean_household", clear
	count // 9289
	
	pg_randomize treatment, idvar(household_prera) seed(12345) blockvars(household_block school_code grade_level) num_to_gen(1) arms(3)
	tab treatment, m
	
	// Generate treatment variable (string)
	gen treatment_name = ""
	replace treatment_name = "Control" if treatment == 0 // 3096
	replace treatment_name = "lostlearning" if treatment == 1  // 3096
	replace treatment_name = "energybill" if treatment == 2 // 3097
	
	sort treatment_name studentid
	save "$data_out\FINAL_disB_energybill_hhld_$date"
	
	rename school_code hh_school_code
	rename grade_level hh_grade_level
	// Merge with student file
	keep household_prera treatment_name treatment household_block hh_school_code hh_grade_level bin
	
	merge 1:m household_prera using "$interim\disB_energybill_clean_studentlevel", assert(3) nogen

	// check
		tab treatment treatment_name
		tab treatment school_code
		tab treatment grade_level
	sort treatment_name studentid
	save "$data_out\FINAL_disB_energybill_student_${date}_updated", replace
}

cap log close

// Save different files for treatment and control schools
{
	// Control students
	preserve
		keep if treatment == 0
		keep treatment_name studentid
		sort treatment_name studentid
		export delimited "$data_out\FINAL_disB_control_energybill_${date}"
	restore
	
	// Treatment students
	preserve
		keep if treatment != 0
		keep treatment_name studentid
		sort treatment_name studentid
		export delimited "$data_out\FINAL_disB_treatment_energybill_${date}"
	restore	
}

// Balance check
if 1{	
	preserve
		keep treatment school_code grade_level household_block male race ever_ell ever_sped ever_frpl chronic_absent abs_rate_2018 days_absent_2018 current* 
		rename (male race ever_ell ever_sped ever_frpl) (male_prera race_prera ever_ell_prera ever_sped_prera ever_frpl_prera)
		pg_balance_table, blockvars(household_block school_code grade_level) stats(tstat) save(${figures}\disB_energybill_balance_table)
		export excel using "${figures}\disB_energybill_balance_table.xlsx", first(var) replace
	restore
}
