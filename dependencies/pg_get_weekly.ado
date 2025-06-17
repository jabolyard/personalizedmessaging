// utility that calculates weekly cumulative absence rates based on daily data
cap program drop pg_get_weekly
program define pg_get_weekly
	version 15
	syntax , STARTdate_dmy(string) save(string) [ENDdate_dmy(string) pre_treat abs_cap(integer 10000)]
	// When this is run, the daily data should be in memory, only including records for
	// those who were part of the pilot
	
	// limit data to post-treatment period
	local pilot_start_date `=date("`startdate_dmy'","DMY")'
	keep if date >= `pilot_start_date'
	// if an end-date is specified, drop records after end-date
	if "`enddate_dmy'" != "" {
		local pilot_end_date `=date("`enddate_dmy'","DMY")'
		keep if date <= `pilot_end_date'
	}
	
	// check that treatment is not missing
	assert !mi(treatment)
	
	// save current data
	tempfile current
	save `current'
	
	di "`pre_treat'"
	
	if "`pre_treat'" == "" {
		
		// calculate weeks
		bysort date : keep if _n==1
		keep date
		sort date
		gen week_of_pilot = floor((_n-1)/5) + 1
		qui sum week_of_pilot
		local max_week `r(max)'
		count if week_of_pilot == `max_week'
		local days_in_last_week = r(N)
		if `days_in_last_week' < 5 {
			di as err "Note that the final week contains only `days_in_last_week' days"
		}
		tempfile weeks
		save `weeks'
		
		use `current', clear
		merge m:1 date using `weeks', assert(3) nogen
		
		// generate adjusted abs-rate (within partner-grade) by week (cumulative)
		qui sum week_of_pilot
		local max_week `r(max)'
		forval i=1/`max_week' {
			bysort sid : gegen cum_abs_rate_wk_`i' = mean(absent) if week_of_pilot <= `i'
			bysort sid : gegen cum_days_absent_wk_`i' = total(absent) if week_of_pilot <= `i'
			// option for capping absences 
			if `abs_cap' < 10000 {
				bysort sid : gegen days_enrolled_wk_`i' = count(absent) if week_of_pilot <= `i'
				bysort sid : replace cum_abs_rate_wk_`i' = `abs_cap'/days_enrolled_wk_`i' if week_of_pilot <= `i' & cum_days_absent_wk_`i' >= 40
			}
			replace cum_abs_rate_wk_`i' = . if week_of_pilot != `i' // only want to calculate if they were enrolled in that time
			bysort sid : gegen cum_days_enrolled_wk_`i' = count(absent) if week_of_pilot <= `i'
			// Only keep 1 value per student so that all are weighted equally
			bysort sid week_of_pilot : replace cum_abs_rate_wk_`i' = . if _n != 1
			bysort sid week_of_pilot : replace cum_days_absent_wk_`i' = . if _n != 1
		}
		// Split this into two loops so that sorts only need to be done once
		forval i=1/`max_week' {
			tempvar mean_rate_`i'
			bysort exp_grade : gegen `mean_rate_`i'' = mean(cum_abs_rate_wk_`i') if week_of_pilot <= `i' & !mi(treatment)
			sum `mean_rate_`i''
			if `r(mean)'==0 {
				di as err "Warning: the mean abs-rate is 0 in week `i'!"
			}
			bysort exp_grade : gegen mean_abs_rate_wk_`i' = max(`mean_rate_`i'')
			gen adj_abs_rate_wk_`i' = cum_abs_rate_wk_`i'/mean_abs_rate_wk_`i'
			
			// Store the most recent 5-day period as the adj_abs_rate
			if `i'==`max_week' {
				gegen mean_abs_rate = mean(cum_abs_rate_wk_`i')
				gen adj_abs_rate = adj_abs_rate_wk_`i'
				gen cum_days_absent = cum_days_absent_wk_`i'
				gen cum_abs_rate = cum_abs_rate_wk_`i'
				bysort sid : gen cum_days_enrolled = _N
				gen max_week = `max_week'
			}
		}

		gcollapse (max) adj_abs_rate* mean_abs_rate cum_abs_rate* cum_days_absent* cum_days_enrolled* max_week, by(sid)
		gen has_all_weeks = 1
		forval i=1/`max_week' {
			replace has_all_weeks = 0 if mi(adj_abs_rate_wk_`i')
		}

		gen days_in_last_week = `days_in_last_week'
	} // end non-pre-treat section
	else {
		cap assert "`enddate_dmy'" != ""
		if _rc != 0 {
			di as err "If you specify the pre-treat option, you must provide an end-date"
			exit 2
		}
		cap assert `abs_cap' == 10000
		if _rc != 0 {
			di as err "Abs_cap cannot be used with the pre-treat option"
			exit 2
		}
		bysort sid : gegen pre_treat_abs_rate = mean(absent)
		bysort sid : gegen pre_treat_days_absent = total(absent)
		bysort sid : gen pre_treat_days_enrolled = _N
		// Only keep 1 value per student so that all are weighted equally
		bysort sid : replace pre_treat_abs_rate = . if _n != 1
		bysort sid : replace pre_treat_days_absent = . if _n != 1
		
		tempvar mean_rate
		bysort exp_grade : gegen `mean_rate' = mean(pre_treat_abs_rate) if !mi(treatment)
		sum `mean_rate'
		if `r(mean)'==0 {
			di as err "Warning: the mean abs-rate is 0!"
		}
		bysort exp_grade : gegen mean_abs_rate = max(`mean_rate')
		gen pre_treat_adj_abs_rate = pre_treat_abs_rate/mean_abs_rate
		
		gcollapse (max) pre_treat_abs_rate pre_treat_days_absent pre_treat_adj_abs_rate pre_treat_days_enrolled , by(sid)
	}
	save "`save'", replace
end
