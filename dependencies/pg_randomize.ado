// define randomization procedure
cap program drop pg_randomize
program define pg_randomize
	version 14
	syntax newvarname(numeric), IDvar(varlist) [seed(integer 12345) blockvars(varlist) num_to_gen(integer 1) RAND_varnames(namelist min=1 max=1) arms(integer 2) save(string) split(string) BIN_varname(namelist min=1 max=1)]
	// Option notes:
		/*
			Split takes the fraction of observations that you want assigned to treatment (or arm 2).
				- So, if you want 2/3 of students to be in the treatment group, specify split("2/3")
		*/
	
	tempfile data_origin
	// parameter checks
	assert `seed' > 0
	assert inrange(`arms', 2, 100)
	assert `num_to_gen' >= 1
	// set rand number default if not specified
	if "`rand_varnames'"=="" {
		forval i=1/`=`: word count of `blockvars''+2' {
			confirm new var _rand_`i'
		}
		local rand_varnames _rand 
	}
	else { // check namespace for specified rand-names
		forval i=1/`=`: word count of `blockvars''+2' {
			confirm new var `rand_varnames'_`i'
		}
	}
	
	isid `idvar'
	
	// if no bin varname is specified, set default
	if "`bin_varname'" == "" {
		confirm new var bin
		local bin_varname bin
	}
	else { // check namespace for specified bin-names
		confirm new var `bin_varname'
	}
	
	// If no blocking variables were specified, create a constant variable to block on
	if "`blockvars'"=="" {
		tempvar constant_var
		gen `constant_var' = 1
		local blockvars `constant_var'
	}
	
	// make sure that the blocking variables are never missing
	foreach var of varlist `blockvars' {
		assert !mi(`var')
	}
	
	// check that split has not been called with more than two arms
	if `arms' != 2 & "`split'" != "" {
		di as err "Split option can only be used with two arms."
		exit 1
	}
	// parse split
	if "`split'" != "" {
		local parse = regexm("`split'", "([0-9]*)/([0-9]*)")
		local split_num = regexs(1)
		local split_den = regexs(2)
		di `split_num'
		di `split_den'
	}
	// check that split is not greater than one
	if "`split'" != "" {
		cap assert `split_num' < `split_den'
		if _rc != 0 {
			di as err "Split must be less than 1/1."
			BREAK
		}
	}
	
	set seed `seed'
	// sort the data so that the randomization can't be affected by changes in how the input data was sorted
	sort `idvar', stable
	// create vars that will hold the random numbers
	tempvar rand odd_rand
	qui gen `bin_varname' = .
	qui gen `rand' = .  
	// loop over the number of treatment permutations you want
		// for an experiment, we'll just do this once, but if you want 
		// assignments for a simulation, you can get an arbitrary number of different
		// treatment groups
	
	// For two arms, we can do ratios other than 1:1; the split option specifies the proportion to be assigned to treatment 0
	if "`split'" != "" & `arms'==2 {
		forval i=1/`num_to_gen' {
					// index the treatment varname if running more than once
		local treat_varname = cond(`num_to_gen'==1,"`varlist'", "`varlist'_`i'")
		tempvar oddball oddball_temp
		qui gen `oddball_temp' = .
		gen `oddball' = 1
		qui gen `treat_varname' = .
		local num_rounds = `: word count `blockvars'' + 2
		forval j=0/`num_rounds' {
			tempvar rand_`j'
			qui gen double `rand_`j'' = runiform()
		}
		
		forval j=0/`: word count `blockvars'' {
			local templist
			forval k=1/`=`: word count `blockvars''-`j'' {
				local templist `templist' `: word `k' of `blockvars''
			}
			di "Blocking on: `templist'"
			qui unique `rand_`j'' if `oddball'==1
			assert `r(unique)'==`r(N)'
			sort `templist' `oddball' `rand_`j'', stable
			qui by `templist' `oddball' : replace `treat_varname' = inrange(mod(_n,`split_den'),1,`split_num') if `oddball'==1 & _n <= _N - mod(_N, `split_den')
			cap assert mi(`treat_varname') if `oddball'==1
			if _rc==0 {
				di as err "Blocking on `templist' results in no treatments being assigned."
			}
			sort `templist' `oddball' `rand_`j'', stable
			qui by `templist' `oddball' : replace `oddball_temp' = _n > _N - mod(_N,`split_den') if `oddball' != 0
			qui if `i'==1 {
				replace `bin_varname' = `j' + 1 if `oddball_temp'==0 & `oddball'==1
				gen `rand_varnames'_`=`j'+1' = `rand_`j''
			}
			tab `treat_varname' if `oddball'==1, mi
			sort `templist' `oddball' `rand_`j'', stable
			qui by `templist' `oddball' : replace `oddball' = _n > _N - mod(_N,`split_den') | _N < `split_den' if `oddball' != 0
		}
		
		qui replace `treat_varname' = `rand_`num_rounds'' < `split_num'/`split_den' if `oddball'==1
		qui if `i'==1 gen `rand_varnames'_`num_rounds' = `rand_`num_rounds''
		qui if `i'==1 replace `bin_varname' = `=`: word count `blockvars''+2' if `oddball'==1
		di "Oddest-ball block:"
		tab `treat_varname' if `oddball'==1
		
		// display randomization summary for logging
		if `i'==1 {
			tab `bin_varname' `treat_varname' , mi row
			count
			local N_randomized `r(N)'
			cap unique `blockvars'
			local N_blocks `r(unique)'
			tempvar num_in_block
			qui bysort `blockvars' : gen `num_in_block' = _N if _n==1
			noi {
				di "Date of run: `c(current_date)' at `c(current_time)'"
				di "Unit of randomization: `idvar'"
				di "Number of units randomized: `N_randomized'"
				if "`blockvars'" != "" {
					di "Blocking Variables: `blockvars'"
					di "Number of Blocks: `N_blocks'"
					di "Min, Max, and Median of N within Blocks: "
					tabstat `num_in_block', s(min max median)
				}
				else di "No Blocking Variables Specified"
				di "Name of treatment variable: `treat_varname'"
				di "Number of arms: `arms'"
				tab `treat_varname'
				di "Seed value: `seed'"
				di "Number of iterations generated: `num_to_gen'"
			}
		}
		} // end loop to generate treatment assignments
	} // end split option
	else {
	forval i=1/`num_to_gen' {
		// index the treatment varname if running more than once
		local treat_varname = cond(`num_to_gen'==1,"`varlist'", "`varlist'_`i'")
		tempvar oddball oddball_temp
		qui gen `oddball_temp' = .
		gen `oddball' = 1
		qui gen `treat_varname' = .
		forval j=0/`: word count `blockvars'' {
			local templist
			forval k=1/`=`: word count `blockvars''-`j'' {
				local templist `templist' `: word `k' of `blockvars''
			}
			if `i'==1 di "Blocking on: `templist'"
			qui replace `rand' = runiform() if `oddball'==1
			qui replace `rand' = . if `oddball'==0
			qui unique `rand' if `oddball'==1
			assert `r(unique)'==`r(N)'
			qui bysort `templist' `oddball' (`rand'): replace `treat_varname' = mod(_n,`arms') if `oddball'==1 & _n <= _N - mod(_N, `arms') & _N >= `arms'
			// check that at least some treatments were assigned in this round
			if `i'==1 {
				cap assert mi(`treat_varname') if `oddball'==1
				if _rc==0 {
					di as err "Blocking on `templist' results in no treatments being assigned."
				}
			}
			qui bysort `templist' `oddball' (`rand'): replace `oddball_temp' = _n > _N - mod(_N,`arms') | _N < `arms' if `oddball' != 0
			qui if `i'==1 {
				replace `bin_varname' = `j' + 1 if `oddball_temp'==0 & `oddball'==1
				gen `rand_varnames'_`=`j'+1' = `rand'
				noi tab `treat_varname' if `oddball'==1, mi
			}
			qui bysort `templist' `oddball' (`rand'): replace `oddball' = `oddball_temp' if `oddball' != 0
		}
		
		qui save `data_origin', replace
		
		clear
		qui {
			set obs `arms'
			tempvar arm arm_rand
			gen `arm' = _n - 1
			gen `arm_rand' = runiform()
			sort `arm_rand'
			gen n = _n
			tempfile odd_treats
			save `odd_treats'
		}

		use `data_origin', clear
		qui replace `rand' = runiform() if `oddball'==1
		qui bysort `oddball' (`rand') : gen n = _n if `oddball'==1
		quietly merge m:1 n using `odd_treats', keep(1 3) nogen
		qui bysort `oddball' (`rand') : replace `treat_varname' = `arm' if `oddball'==1
		qui if `i'==1 replace `bin_varname' = `=`: word count `blockvars''+2' if `oddball'==1
		qui if `i'==1 gen `rand_varnames'_`=`:word count `blockvars''+2' = `rand'
		if `i'==1 {
			di "Oddest-ball block:"
			tab `treat_varname' if `oddball'==1
		}
		drop n `arm' `arm_rand'
		
		// display randomization summary for logging
		if `i'==1 {
			tab `bin_varname' `treat_varname' , mi row
			count
			local N_randomized `r(N)'
			cap unique `blockvars'
			local N_blocks `r(unique)'
			tempvar num_in_block
			qui bysort `blockvars' : gen `num_in_block' = _N if _n==1
			noi {
				di "Date of run: `c(current_date)' at `c(current_time)'"
				di "Unit of randomization: `idvar'"
				di "Number of units randomized: `N_randomized'"
				if "`blockvars'" != "" {
					di "Blocking Variables: `blockvars'"
					di "Number of Blocks: `N_blocks'"
					di "Min, Max, and Median of N within Blocks: "
					tabstat `num_in_block', s(min max median)
				}
				else di "No Blocking Variables Specified"
				di "Name of treatment variable: `treat_varname'"
				di "Number of arms: `arms'"
				tab `treat_varname'
				di "Seed value: `seed'"
				di "Number of iterations generated: `num_to_gen'"
				pg_check_commit S:\ado15.1\personal\p
				di "Randomized using version: `r(commit)'"
			}
		}
	} // end loop to generate treatment assignments
	} // end else loop
	
	if "`save'" != "" {
		di "Saving to file: `save'"
		save "`save'", replace
	}
	
end
