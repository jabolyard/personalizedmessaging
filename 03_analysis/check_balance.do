/*******************************************************************************

	File: check_balance.do
	Purpose: Check balance between treatment and control for completed model data.

*******************************************************************************/


	//NOTE:  BALANCE WAS A BIG ENDEAVOR FOR THIS PAPER...THERE ARE MULTIPLE (SLIGHTLY DIFFERENT) ITERATIONS OF BALANCE.  THE ONE WE DECIDED TO USE AS THE MAIN ONE, THOUGH, STARTS AROUND LINE 59 (SEARCH "NOW FOR EACH ARM INDIVIDUALLY")
	
	//NOTE 2 - ALL OF THE BALANCE TABLES PRODUCED HERE WERE OUTPUTTED DIRECTLY TO TABLES_FIGURES_NEW AND THEN COPIED INTO A FOLDER THERE CALLED "BALANCE_TABLES_BY_ARM".  Their names were also edited slightly (e.g., from "balance_table_Mail_with_partner_" to balance_table_arm_2)
	
	
	
	use "${outputs}/stacked_model_data_file", clear
	bys treatment: sum(Hispanic) if strpos(pilot_id, "[REMOVED]")>0 | strpos(pilot_id, "[REMOVED]")>0
	preserve
	keep if ran_Mail==1 
	bys treatment: sum(Hispanic) if strpos(pilot_id, "[REMOVED]")>0 | strpos(pilot_id, "[REMOVED]")>0
	bys treatment: sum(Hispanic) if strpos(pilot_id, "[REMOVED]")>0 
	restore
	
	
	*TESTING THE VARIABLES FOR SIGNIFICANCE
	local JOINT_VAR xmale xell xsped xfrpl Black Hispanic White log_schgr_prior log_sch_prior pre_treat_abs_rate pr_prior_abs_rate mi_schgr_prior mi_sch_prior mi_pr_prior mi_pre_treat
 	logit treatment `JOINT_VAR' i.pooled_rand_bin i.pid, cluster(pooled_rand_unit)
	test `JOINT_VAR'
	
	
	*Use another excel file for SEs with all arms combined
	use "${outputs}/stacked_model_data_file", clear
	replace arm = 2 if arm>2 //consolidate to either treatment or control (i.e., arms 2-6 combined)
	replace treatment_name = "Treatment" if treatment_name!="Control"
	local row = 2
	putexcel set "${tables_figures}/SEs_for_continuous_variables/SEs_for_Balance_Table_all_arms.xlsx", replace
	putexcel A1  = "Variables", border(bottom)
	putexcel B1 = "Control SEs", border(bottom)
	putexcel C1 = "Treatment SEs", border(bottom)
	
	foreach var in xmale xell xsped xfrpl Black Hispanic White log_schgr_prior log_sch_prior pre_treat_abs_rate pr_prior_abs_rate mi_schgr_prior mi_sch_prior mi_pr_prior mi_pre_treat {
		sum `var' if treatment_name== "Control"
		local sd_c = round(`r(sd)', 0.01)
		sum `var' if treatment_name== "Treatment"
		local sd_t = round(`r(sd)', 0.01)
		putexcel A`row' = "`var'", border(bottom)
		putexcel B`row' = `sd_c', border(bottom)
		putexcel C`row' = `sd_t', border(bottom)
		local row = `row' +1 //increment counter for rows

	}
	
	
	
	*NOW FOR EACH ARM INDIVIDUALLY (the only difference from the one directly above is that the control is different for each treatment arm...it is only the students in the control group from those given pilots with the treatment arm)
	
	*using revised pg_balance utility that includes both pooled_rand_bin, as opposed to rand_bin, and i.pid)
	use "${outputs}/stacked_model_data_file", clear
	

	foreach treatment_arm in Mail Backpack Text Email Robocall  {
		preserve
			replace arm = 2 if arm>2 //makes the excel sheet easier to read
			keep if ran_`treatment_arm'==1 //use binary variable in stacked model data file called "Ran_Text", "Ran_Backpack", etc...
			pg_balance_with_partner_FEs, data_type(randomization) rand_var(pooled_rand_unit) extra_vars(xmale xell xsped xfrpl Black Hispanic White Other log_schgr_prior log_sch_prior pre_treat_abs_rate pr_prior_abs_rate mi_schgr_prior mi_sch_prior mi_pr_prior mi_pre_treat) table_save("${tables_figures}/balance_table_`treatment_arm'_with_partner_FEs.xlsx") pooled 
			
			*output SEs for each arm (I copied these into the balance table manually later)
			local row = 2
			putexcel set "${tables_figures}/SEs_for_continuous_variables/SEs_for_`treatment_arm'.xlsx", replace 
			putexcel A1  = "Variables", border(bottom)
			putexcel B1 = "Control SEs", border(bottom)
			putexcel C1 = "Treatment SEs", border(bottom)
			
			foreach var in xmale xell xsped xfrpl Black Hispanic White Other log_schgr_prior log_sch_prior pre_treat_abs_rate pr_prior_abs_rate mi_schgr_prior mi_sch_prior mi_pr_prior mi_pre_treat {
				sum `var' if treatment_name== "Control"
				local sd_c = `r(sd)' //control mean
				sum `var' if arm>=2
				local sd_t = `r(sd)'
				putexcel A`row' = "`var'", border(bottom)
				putexcel B`row' = `sd_c', border(bottom)
				putexcel C`row' = `sd_t', border(bottom)
				local row = `row' +1 //increment counter for rows

	}
	
		restore
	}