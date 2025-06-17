/*******************************************************************************

	Purpose: To generate the groups in which students were actually randomized
			 for a pilot, based on bin, as well as the groups in which they were
			 intended to be randomized, based solely on the blocking variables
	Author: BDJ
	Date Created: 6/6/2019
	Date Update: 6/3/2019 *made rand_group_vars more explicit so that it contains
						   which blocking vars defined the randomization group 
						   and their respecive values
				 6/6/2019 *changed name of rand_group to rand_bin

*******************************************************************************/

program pg_randgroup 

	version 15.1

	// Set syntax
	syntax varlist(min=1), [bin(varlist min=1 max=1 numeric)]
	if "`bin'" == "" {
		local bin bin
	}
	confirm numeric var `bin'
	
	// Assertions about what the user entered
	foreach v in `varlist' {
		assert !missing(`v')
	}
	assert !missing(`bin')
	cap confirm var rand_bin, exact
	if _rc == 0 {
		display in red "rand_bin already exists"
		exit
	}
	cap confirm var block_group, exact 
	if _rc == 0 {
		display in red "block_group already exists"
		exit
	}
	cap confirm var rand_bin_vars, exact
	if _rc == 0 {
		display in red "rand_bin_vars already exists"
		exit
	}
	
	// Generate randomization bin (group they were actually randomized in), 
	// block group (groups based only and naively on blocking variables), 
	// and rand_bin_vars (vars and values that defined an observation's rand bin)
	{
		// block group
		egen block_group = group(`varlist')
		label var block_group "intended randomization group based on blocking vars"
		// randomization bin
		local block_list ""
		forvalues i = 1/`: word count `varlist'' {
			tempvar group`i'
			tempvar string`i'
		}
		forvalues i = 1/`: word count `varlist'' {
			local block_list `block_list' `: word `i' of `varlist''
			local reverse_counter = `:word count `varlist'' - `i' + 1 // count down because it's easier to accummulate list of vars
			egen `group`reverse_counter'' = group(`block_list') if `bin' == `reverse_counter'	
			tempvar str_of_block_list 
			gen `str_of_block_list' = "(`block_list')"
			egen `string`reverse_counter'' = concat(`block_list' `str_of_block_list') if `bin' == `reverse_counter', punct("  ") format(%11.0f)
		}
		if `: word count `varlist'' > 1 {
			forvalues i = 2/`: word count `varlist'' {
				replace `group1' = `group`i'' if `bin' == `i'
				replace `string1' = `string`i'' if `bin' == `i'
			}	
		}
		replace `group1' = 1 if !inrange(`bin', 1, `:word count `varlist'')
		replace `string1' = "passed_completely_back_up" if !inrange(`bin', 1, `:word count `varlist'')
		assert !missing(`group1')
		assert !missing(`string1')
		egen rand_bin = group(`group1' `bin')
		label var rand_bin "actual randomization bin based on bin number and bin vars"
		// rand_bin_vars
		gen rand_bin_vars = `string1'
		label var rand_bin_vars "vars on which randomization bin was based"
		local rand_bin_vars_strlen = strlen(rand_bin_vars)
		format rand_bin_vars %-`rand_bin_vars_strlen's
	}
	
end
