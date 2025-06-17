/*******************************************************************************
 
	Date Created: Oct 2018
	Date Modified: 9/16/19
	Purpose: create pilot full and model data files

	!!! need to add in tables
	!!! use analyst analysis file until reconcile DW

*******************************************************************************/


// Top Matter
{
	// Global - Abbreviations and Parameters
	global site disC
	global pilot disC_energybill_oct2018
	global startpilot 29oct2018 // date first treatment email was sent out
	global endpilot 14jun2019 // this is the final day of the 2019 school year for [REMOVED]
	global startpretreat 01jul2018 
	global endpretreat 22oct2018
	global useyear 2019
	global rand_var household

	// Global - Directories
	global pilot_data	"R:\Proving_Ground\data\pilots\\${pilot}\\"
	global clean_dw		"R:\Proving_Ground\data\clean_${site}\\${site}_dw_files\\"
	global interim		"R:\Proving_Ground\data\pilots\\${pilot}\\interim"
	global deck_images	"R:/Proving_Ground/tables_figures_new/pilots/${pilot}/deck_images/"
	global deck_tables	"R:/Proving_Ground/tables_figures_new/pilots/${pilot}/deck_tables/"
	global utils		"R:/Proving_Ground/programs_new/pilots/utilities/"

	// Globals - Inputs
	{
		global afile "${clean_dw}\\${site}_dw_analysis.dta"
		global rfile "${pilot_data}\\FINAL_${site}_energybill_student_20181022.dta"
		global rfile_hhld "${pilot_data}\\FINAL_${site}_energybill_hhld_20181022.dta"
		*global dailyabs "${clean_dw}\\${site}_dw_daily_absences.dta"
		global dailyabs "R:\Proving_Ground\data\\clean_${site}\\clean\\clean_${site}_daily_absences.dta"
	}

	// Globals - Outputs
	{
		global ffile "${pilot_data}\${pilot}_full_data.dta"
		global mfile "${pilot_data}\${pilot}_model_data.dta"
	}

// 	// Programs
// 	global balance_table "R:\Proving_Ground\programs_new\aux_dos_analysis\pg_balance_table.do"
// 	global balance_graph "R:\Proving_Ground\programs_new\aux_dos_analysis\pg_balance_graph.do"
	
}
			

			
// *****************************************************************************
// // Step 0 - Run utilites
// *****************************************************************************
//
// run $balance_table		
// run $balance_graph		
			
			
			
*****************************************************************************
// Step 1 - Create interim file of pilot students with covariates and exclusion flag 
*****************************************************************************

// Identify ids of students in the pilot

// Merge to daily absences

// Call Utility to generate 

use $rfile, clear

des , f

// Generate prior_abs_rate, mi_prior indicator
gen prior_abs_rate = days_absent_2018 / days_enrolled_2018
gen mi_prior = mi(prior_abs_rate)
replace prior_abs_rate = 0 if mi_prior

// If enrolled <20 days in prior year, make prior info missing
replace mi_prior = 1 if days_enrolled_2018 < 20
replace prior_abs_rate = 0 if days_enrolled_2018 < 20
replace days_absent_2018 = 0 if days_enrolled_2018 < 20


gen double pid = [REMOVED]

cap confirm variable frpl
if !_rc == 0 {
	cap confirm variable cep
	if _rc == 0 {
		gen frpl = cep
	}
}

// Rename covariates
rename treatment_1_num treatment
rename household hhld
rename grade_level exp_grade
rename school_code exp_school

// Sort treatment into arms
gen arm = treatment + 1

// Deal with missingness among demographic variables
mdesc male sped ell race frpl
gen mi_race = missing(race)
replace race = 7 if missing(race) // missing race grouped with multiple/other
rename race xrace
foreach v of varlist male sped ell frpl {
	gen mi_`v' = missing(`v')
	replace `v' = 0 if missing(`v')
	rename `v' x`v'
}

// Consolidate racial categories to: 1 Black, 3 Hispanic, 5 White, 7 Other
replace xrace = 7 if inlist(xrace, 2, 4, 6)


bys exp_grade:  egen avg_prior_abs_rate = mean(prior_abs_rate)
gen prior_adj_abs_rate = prior_abs_rate/avg_prior_abs_rate
replace prior_adj_abs_rate = 0 if mi_prior

sum prior_adj_abs_rate 

keep pid hhld mi_prior prior_abs_rate prior_adj_abs_rate school_year sid treatment ///
	treatment_name arm exp_grade exp_school xmale xsped xell xrace xfrpl days_absent_2018 ///
	days_enrolled_2018 block_grade household_block [REMOVED]_flag

// confirm unique by student
isid sid

sum pid

save "$interim\temp_pilot_${pilot}_data.dta", replace



*****************************************************************
// Step 2 - Generate Weekly Absences for Students in Pilot
*****************************************************************

use using "$dailyabs" if school_year == $useyear, clear

merge m:1 sid using "$interim\temp_pilot_${pilot}_data.dta" , gen(_mrgpilotdaily) keepusing(sid treatment exp_grade exp_school)

keep if inlist(_mrgpilotdaily, 2, 3)

unique sid if _mrgpilotdaily == 2
unique sid if _mrgpilotdaily == 3

keep if _mrgpilotdaily == 3

tempfile pilotdaily
save `pilotdaily'

// check start is valid date //
assert inrange(dow(td($startpretreat)),0,6)
assert inrange(dow(td($endpretreat)),0,6)
assert td($endpretreat) < td($startpilot)

pg_get_weekly , startdate_dmy("$startpilot") enddate_dmy("$endpilot") save("${pilot_data}/weekly_cumulative_adj_abs.dta")



*******************************************************************************
// Step 3 - Generate Pre-RA Weekly Absences for Pilots that Start During Year
*******************************************************************************

use `pilotdaily', clear

// check pretreat start and end are valid dates and that pretreat ends before pilot starts //
assert inrange(dow(td($startpretreat)),0,6)
assert inrange(dow(td($endpretreat)),0,6)
assert td($endpretreat) < td($startpilot)

pg_get_weekly , startdate_dmy("$startpretreat") ///
						save("${pilot_data}/pre_treat_adj_abs.dta") enddate_dmy("$endpretreat")  pre_treat

						
/*						
gen junkgrade = exp_grade + 100
regress adj_abs_rate treatment xmale i.xrace xsped xell i.junkgrade prior_abs_rate
*/



*******************************************************************************
// Step 4 - Merge weekly info onto pilot data and identify attriters
*******************************************************************************

use "$interim\temp_pilot_${pilot}_data.dta", clear
sum pid

merge 1:1 sid using "${pilot_data}//weekly_cumulative_adj_abs.dta", gen(_mrgwkly)
// Mark student who were assigned to treatment but didn't enroll
gen attrit_pre_treat = _mrgwkly==1
tab treatment attrit_pre_treat, row

merge 1:1 sid using "${pilot_data}//pre_treat_adj_abs.dta", gen(_mrgprewkly)
keep if inlist(_mrgprewkly, 1,3)



*******************************************************************************
// Step 5 - Create School and School Grade Priors
*******************************************************************************

// check missingness among students who matriculated
foreach var of varlist * {
	cap assert !mi(`var') if attrit_pre_treat==0
	if _rc != 0 {
		di "`var' is sometimes missing: "
		cap tempvar `var'_is_missing
		if _rc ==0 {
			gen ``var'_is_missing' = mi(`var') if attrit_pre_treat==0
			label var ``var'_is_missing' "`var' Missing"
			tab ``var'_is_missing'
			drop ``var'_is_missing'
		}
		else {
			tempvar `var'_mi // some names get too long
			gen ``var'_mi' = mi(`var') if attrit_pre_treat==0
			label var ``var'_mi' "`var' Missing"
			tab ``var'_mi'
			drop ``var'_mi'
		}
		
	}
}


// calculate school and school-grade averages of 2018 absence rates using
// the analysis file
preserve

	use "${afile}", clear

	keep if school_year == ($useyear - 1)
	unique school_year
	assert r(unique) == 1

	drop if days_enrolled < 20 // exclude students enrolled <20 days from sch and sch-gr averages
	
	bysort grade_level : egen mean_gl_abs_rate = mean(abs_rate)
	gen adj_abs_rate = abs_rate / mean_gl_abs_rate
	
	// school prior avg days absent and adjusted abs rate
	bysort school_code : egen sch_prior_adj_abs_rate = mean(adj_abs_rate)
	bysort school_code: egen sch_prior_avg_days_absent = mean(days_absent)
	
	// school-grade prior avg days absent and adjusted abs rate 
	bysort school_code grade_level : egen schgr_prior_adj_abs_rate = mean(adj_abs_rate)
	bysort school_code grade_level: egen schgr_prior_avg_days_absent = mean(days_absent)
	
	rename school_code exp_school
	rename grade_level exp_grade
	
	// tempfile containing school and school-grade priors
	keep exp_school exp_grade sch_prior_adj_abs_rate sch_prior_avg_days_absent ///
		 schgr_prior_adj_abs_rate schgr_prior_avg_days_absent
	bys exp_school exp_grade: keep if _n == 1
	tempfile schgr_prior
	save `schgr_prior'

	// tempfile containing school priors only
	keep exp_school sch_prior_adj_abs_rate sch_prior_avg_days_absent
	bys exp_school: keep if _n == 1	
	tempfile sch_prior
	save `sch_prior'

restore

merge m:1 exp_school exp_grade using `schgr_prior', gen(_mrgpriorschgr) keepusing(exp_school exp_grade schgr_prior_adj_abs_rate schgr_prior_avg_days_absent)
drop if _mrgpriorschgr == 2

merge m:1 exp_school using `sch_prior', gen(_mrgpriorsch)  keepusing(exp_school sch_prior_adj_abs_rate sch_prior_avg_days_absent)
drop if _mrgpriorsch == 2


// dummy out missings for school and school-grade absence vars
gen mi_sch_prior = missing(sch_prior_adj_abs_rate)
replace sch_prior_adj_abs_rate = 0 if mi_sch_prior == 1
replace sch_prior_avg_days_absent = 0 if mi_sch_prior == 1
gen mi_schgr_prior = missing(schgr_prior_adj_abs_rate)
replace schgr_prior_adj_abs_rate = 0 if mi_schgr_prior == 1
replace schgr_prior_avg_days_absent = 0 if mi_schgr_prior == 1


/*
// student prior absences (adjusted)  
*!! by grade
egen avg_prior_abs_rate = mean(prior_abs_rate)
gen prior_adj_abs_rate = prior_abs_rate/avg_prior_abs_rate

// dummy out missings
//gen mi_prior = mi(prior_adj_abs_rate) 
replace prior_adj_abs_rate = 0 if mi_prior==1
*/


foreach X of varlist * {
	qui count if mi(`X')
	if r(N) != 0 {
	di "Variable `X' has missing values"
	}
}
 

// mark attrition
gen attrit_post_treat = mi(adj_abs_rate) // this should be switched to look at withdrawl codes when the analysis file is ready

sum pid

preserve
	// These don't appear on attrition table, but they seem worth checking
	//gen switched_school = school_code != exp_school if attrit_pre_treat==0
	//gen switched_grade = grade_level != exp_grade if attrit_pre_treat==0
	
	gen enrolled_post = attrit_pre_treat==0
	gen enrl_post_and_didnt_attrit = enrolled_post==1 & attrit_post_treat==0
	collapse (count) Assigned=sid (sum) enrolled_post enrl_post_and_didnt_attrit (firstnm) treatment_name, by(arm)
	gen show_rate = (enrolled_post)/Assigned
	replace show_rate = round(show_rate, .01)*100
	tostring show_rate, replace
	replace show_rate = show_rate + "%"
	gen pct_enrled_entire_post = round((enrl_post_and_didnt_attrit/Assigned), .01)*100
	tostring pct_enrled_entire_post, replace
	replace pct_enrled_entire_post = pct_enrled_entire_post + "%"
	replace treatment_name = subinstr(treatment_name, " ", "_",.)
	sort arm
	order treatment_name arm Assigned enrolled_post show_rate enrl_post_and_didnt_attrit pct_enrled_entire_post
	drop arm
	sxpose, clear force destring firstnames
	gen x = ""
	local rows `""Assigned" "Enrolled at all in Post-Treatment Period" "%" "Enrolled for Entire Analysis Period" "%""'
	forval i=1/5 {
		local s : word `i' of `rows'
		replace x = "`s'" if _n==`i'
	}
	order x *
	export delimited using "${deck_tables}/attrition_table.csv", replace
restore


// making sure prior and pre-randomization abs count vars are present
// and are the same as prior_days_absent and prior_days_enrolled (becauase
// pilot randomization and implementation happened in same school year)
rename days_absent_2018 prior_days_absent
clonevar pr_prior_days_absent = prior_days_absent
rename days_enrolled_2018 prior_days_enrolled
clonevar pr_prior_days_enrolled = prior_days_enrolled
clonevar mi_pr_prior = mi_prior
clonevar pr_prior_adj_abs_rate = prior_adj_abs_rate


// making sure student-level abs count vars and missing indicators are 
// present for count models and in alignment
foreach v in prior_days_absent pr_prior_days_absent prior_days_enrolled pr_prior_days_enrolled {
	replace `v' = 0 if mi_prior == 1
}
assert prior_days_absent == 0 & pr_prior_days_absent == 0 & prior_days_enrolled == 0 & pr_prior_days_enrolled == 0 if mi_prior == 1
assert !missing(prior_days_absent) & !missing(pr_prior_days_absent) & !missing(prior_days_enrolled) & !missing(pr_prior_days_enrolled) if mi_prior == 0
gen mi_pre_treat = missing(pre_treat_days_absent)
replace pre_treat_days_absent = 0 if mi_pre_treat == 1
replace pre_treat_adj_abs_rate = 0 if mi_pre_treat == 1
assert pre_treat_days_absent == 0 & pre_treat_adj_abs_rate == 0 if mi_pre_treat == 1
assert !missing(pre_treat_days_absent) if mi_pre_treat == 0


// adding blocking material
preserve
	use $rfile_hhld, clear // need to re-add bin from hhld rfile
	rename household hhld
	keep hhld bin
	tempfile rfile_hhld
	save `rfile_hhld'
restore
merge m:1 hhld using `rfile_hhld', assert(3) nogen


// make sure blocking variables follow new convention
rename household_block block_hhld_no_email_homeless
gen block_stu_school = exp_school
gen block_stu_grade = exp_grade
rename [REMOVED]_flag block_hhld_[REMOVED]

// create rand_bin, rand_bin_vars, and block_group
pg_randgroup block_hhld_no_email_homeless block_stu_school block_stu_grade block_hhld_[REMOVED], bin(bin)

// generate rand_unit, which will just be a exact copy of hhld
clonevar rand_unit = hhld

// generate randomization level var
gen rand_level = 2

save "${ffile}", replace


// drop attriters for model data file
keep if attrit_pre_treat==0 
drop attrit_pre_treat attrit_post_treat has_all_weeks


// check model data file specs before saving
pg_check_data, filetype(model)

save "${mfile}", replace



// Run Balance Table 

use "${mfile}"

preserve

	rename hhld household 
	keep household arm xmale xsped xell xrace xfrpl exp_grade prior_abs_rate 
	
	pg_balance_table, treatvar(arm) deck(YES) rand_var(household) save("$deck_tables")
	pg_balance_chart, treatvar(arm) deck(YES) rand_var(household) save("$deck_tables")

restore
	
cap gen integer grade_level = exp_grade
cap gen double school_code = exp_school
do "${utils}/run_deck_tables_figures.do"


use $mfile, clear
tab xrace, mi

stop here



*use "${pilot_data}//${pilot}_model_data.dta", clear

gen tmp_grade  = exp_grade + 100
regress adj_abs_rate treatment pre_treat_abs_rate prior_adj_abs_rate sch_prior_adj_abs_rate schgr_prior_adj_abs_rate xmale xsped xell i.xrace i.tmp_grade

egen tmp_schgr = group(exp_school exp_grade)
mixed adj_abs_rate treatment pre_treat_abs_rate prior_adj_abs_rate sch_prior_adj_abs_rate schgr_prior_adj_abs_rate xmale xsped xell i.xrace i.tmp_grade || exp_school:  || tmp_schgr:   || hhld : 


stop here

preserve

keep treatment xmale xsped xfrpl xrace


restore 

*do "${utils}/run_deck_tables_figures.do"



stop here





