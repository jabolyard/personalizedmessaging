/*
Purpose: Create Pooled model data file for use in analysis of personalized messaging pilots
*/



//Topmatter
clear all 
set more off


*Add disA
use "${outputs}/disA/disA_targeted_aug2018_model_data.dta", clear
gen pilot_id = "disA_targeted_aug2018"

*Add disB
append using "${outputs}/disB/disB_energybill_oct2018_model_data" 
count if missing(pilot_id)
*assert `r(N)' == 15732
replace pilot_id = "disB_energybill_oct2018" if missing(pilot_id)

*Add disC
append using "${outputs}/disC/disC_energybill_oct2018_model_data" 
count if missing(pilot_id)
*assert `r(N)' == 
replace pilot_id = "disC_energybill_oct2018" if missing(pilot_id)

*Add disD
append using "${outputs}/disD/disD_messaging_oct2019_model_data" 
count if missing(pilot_id)
*assert `r(N)' == 
replace pilot_id = "disD_messaging_oct2019" if missing(pilot_id)

*Add disE
append using "${outputs}/disE/disE_energybill_oct2018_model_data" 
count if missing(pilot_id)
*assert `r(N)' == 
replace pilot_id = "disE_energybill_oct2018" if missing(pilot_id)

drop school_level //disF data not merging because of this variable, so just drop it (not needed)


*Add disF
append using "${outputs}/disF/disF_robocalls_sep2019_model_data" 
count if missing(pilot_id)
*assert `r(N)' == 
replace pilot_id = "disF_robocalls_sep2019" if missing(pilot_id)

order days_enrolled attrit_post_treat


* Generate pooled versions of rand_bin, rand_unit
egen pooled_rand_bin = group(pilot_id rand_bin)
egen pooled_rand_unit = group(pilot_id rand_unit)
drop _merge
	save "${outputs}/data_for_consort_diagrams", replace

	count
	* Mark and remove any extraneous pre-treat attriters (leave in post-treat attriters)
	assert attrit_pre_treat != 1 // these students were dropped prior to saving out individual pilot model data files
	drop attrit_pre_treat attrit_post_treat
	
	count if missing(adj_abs_rate)
	gen attrit_post_treat = missing(adj_abs_rate)  //create attrit_post_treat variable so that these students can be dropped in R when using a lienar model (i.e., after the Poisson model including these students is run)
	logit attrit_post_treat treatment // quick logit just to gut check differential post-treat attrition
	
	
	
	*drop if attrit_post_treat == 1
	*drop attrit_pre_treat attrit_post_treat
	

tab pilot_id

tab max_week

//create outcome variables
//cum_abs_rate already created--main outcome for linear model
mdesc cum_abs_rate

//cum days enrolled and cum days absent for poisson model
mdesc cum_days_enrolled cum_days_absent

//create rates per 5 weeks  (NOTE: THESE ARE CALLED 0-5, 5-10, 10-15, EVEN THOUGH THEY ARE REALLY 05, 6-10, 11-15, etc...). It's very difficult to go back and change them, so I just labeled them correctly when I outputted them in R
clonevar cum_days_absent_0_to_5 = cum_days_absent_wk_5

clonevar cum_days_enrolled_0_to_5 = cum_days_enrolled_wk_5
gen abs_rate_0_to_5 = cum_days_absent_0_to_5 / cum_days_enrolled_0_to_5
gen enrolled_at_all_wks_0_to_5 = 0 //variable for if they were enrolled at all in this period
replace enrolled_at_all_wks_0_to_5 = 1 if cum_days_enrolled_0_to_5>0  & cum_days_enrolled_0_to_5!=.
replace cum_days_enrolled_0_to_5 = 1 if cum_days_enrolled_0_to_5==.


gen cum_days_absent_5_to_10 = cum_days_absent_wk_10 - cum_days_absent_wk_5
gen cum_days_enrolled_5_to_10 = cum_days_enrolled_wk_10 - cum_days_enrolled_wk_5
replace cum_days_enrolled_5_to_10 = . if cum_days_enrolled_5_to_10==0 
gen abs_rate_5_to_10 = cum_days_absent_5_to_10 / cum_days_enrolled_5_to_10

gen enrolled_at_all_wks_5_to_10 = 0 //variable for if they were enrolled at all in this period
replace enrolled_at_all_wks_5_to_10 = 1 if cum_days_enrolled_5_to_10>0 & cum_days_enrolled_5_to_10!=.
*replace cum_days_absent_5_to_10 = 0 if cum_days_absent_5_to_10==. //there is no contrast for R if cum_days_absent is blank, so change to 0
replace cum_days_enrolled_5_to_10 = 1 if cum_days_enrolled_5_to_10==.



//disF max is 11 weeks
gen cum_days_absent_10_to_15 = cum_days_absent_wk_15 - cum_days_absent_wk_10 if pilot_id != "disF_robocalls_sep2019"
gen cum_days_enrolled_10_to_15 = cum_days_enrolled_wk_15 - cum_days_enrolled_wk_10 if pilot_id != "disF_robocalls_sep2019"
replace cum_days_absent_10_to_15 = cum_days_absent_wk_11 - cum_days_absent_wk_10 if pilot_id == "disF_robocalls_sep2019"
replace cum_days_enrolled_10_to_15 = cum_days_enrolled_wk_11 - cum_days_enrolled_wk_10 if pilot_id == "disF_robocalls_sep2019"
replace cum_days_enrolled_10_to_15 = . if cum_days_enrolled_10_to_15==0 //see above
gen abs_rate_10_to_15 = cum_days_absent_10_to_15 / cum_days_enrolled_10_to_15

gen enrolled_at_all_wks_10_to_15 = 0 //variable for if they were enrolled at all in this period
replace enrolled_at_all_wks_10_to_15 = 1 if cum_days_enrolled_10_to_15>0 & cum_days_enrolled_10_to_15!=.
replace cum_days_enrolled_10_to_15 = 1 if cum_days_enrolled_10_to_15==.




//disE sdrops at 15 weeks (no special construction) and disA drops at 19 weeks 
gen cum_days_absent_15_to_20 = cum_days_absent_wk_20 - cum_days_absent_wk_15 if pilot_id != "disF_robocalls_sep2019" & pilot_id != "disA_targeted_aug2018"
gen cum_days_enrolled_15_to_20 = cum_days_enrolled_wk_20 - cum_days_enrolled_wk_15 if pilot_id != "disF_robocalls_sep2019" & pilot_id != "disA_targeted_aug2018"

replace cum_days_absent_15_to_20 = cum_days_absent_wk_19 - cum_days_absent_wk_15 if pilot_id == "disA_targeted_aug2018"
replace cum_days_enrolled_15_to_20 = cum_days_enrolled_wk_19 - cum_days_enrolled_wk_15 if pilot_id == "disA_targeted_aug2018"
replace cum_days_enrolled_15_to_20 = . if cum_days_enrolled_15_to_20==0 //see above
gen abs_rate_15_to_20 = cum_days_absent_15_to_20 / cum_days_enrolled_15_to_20 if pilot_id != "disF_robocalls_sep2019"

gen enrolled_at_all_wks_15_to_20 = 0 //variable for if they were enrolled at all in this period
replace enrolled_at_all_wks_15_to_20 = 1 if cum_days_enrolled_15_to_20>0 & cum_days_enrolled_15_to_20!=.
replace cum_days_enrolled_15_to_20 = 1 if cum_days_enrolled_15_to_20==.




//disD messaging drops at 23 weeks
gen cum_days_absent_20_to_25 = cum_days_absent_wk_25 - cum_days_absent_wk_20 if pilot_id != "disF_robocalls_sep2019" & pilot_id != "disE_energybill_oct2018" & pilot_id != "disD_messaging_oct2019" & pilot_id != "disA_targeted_aug2018"
gen cum_days_enrolled_20_to_25 = cum_days_enrolled_wk_25 - cum_days_enrolled_wk_20 if pilot_id != "disF_robocalls_sep2019" & pilot_id != "disE_energybill_oct2018" & pilot_id != "disD_messaging_oct2019" & pilot_id != "disA_targeted_aug2018"

replace cum_days_absent_20_to_25 = cum_days_absent_wk_23 - cum_days_absent_wk_20 if pilot_id == "disD_messaging_oct2019"
replace cum_days_enrolled_20_to_25 = cum_days_enrolled_wk_23 - cum_days_enrolled_wk_20 if pilot_id == "disD_messaging_oct2019"
replace cum_days_enrolled_20_to_25 = . if cum_days_enrolled_20_to_25==0 //see above
gen abs_rate_20_to_25 = cum_days_absent_20_to_25 / cum_days_enrolled_20_to_25 if pilot_id != "disF_robocalls_sep2019" & pilot_id != "disE_energybill_oct2018" & pilot_id != "disA_targeted_aug2018"

gen enrolled_at_all_wks_20_to_25 = 0 //variable for if they were enrolled at all in this period
replace enrolled_at_all_wks_20_to_25 = 1 if cum_days_enrolled_20_to_25>0 & cum_days_enrolled_20_to_25!=.
replace cum_days_absent_20_to_25 = 0 if cum_days_absent_20_to_25==.
replace cum_days_enrolled_20_to_25 = 1 if cum_days_enrolled_20_to_25==.


//disB drops at 27 weeks, disC drops at 28 weeks 
gen cum_days_absent_25_to_30 = cum_days_absent_wk_27 - cum_days_absent_wk_25 if pilot_id == "disB_energybill_oct2018"
gen cum_days_enrolled_25_to_30 = cum_days_enrolled_wk_27 - cum_days_enrolled_wk_25 if pilot_id == "disB_energybill_oct2018"

replace cum_days_absent_25_to_30 = cum_days_absent_wk_28 - cum_days_absent_wk_25 if pilot_id == "disC_energybill_oct2018"
replace cum_days_enrolled_25_to_30 = cum_days_enrolled_wk_28 - cum_days_enrolled_wk_25 if pilot_id == "disC_energybill_oct2018"
replace cum_days_enrolled_25_to_30 = . if cum_days_enrolled_25_to_30==0 //see above
gen abs_rate_25_to_30 = cum_days_absent_25_to_30 / cum_days_enrolled_25_to_30 if pilot_id != "disF_robocalls_sep2019" & pilot_id != "disE_energybill_oct2018" & pilot_id != "disD_messaging_oct2019" & pilot_id != "disA_targeted_aug2018"

gen enrolled_at_all_wks_25_to_30 = 0 //variable for if they were enrolled at all in this period
replace enrolled_at_all_wks_25_to_30 = 1 if cum_days_enrolled_25_to_30>0 & cum_days_enrolled_25_to_30!=.
replace cum_days_absent_25_to_30 = 0 if cum_days_absent_25_to_30==. 
replace cum_days_enrolled_25_to_30 = 1 if cum_days_enrolled_25_to_30==.

fre pilot_id if mi(abs_rate_25_to_30)
fre pilot_id if mi(abs_rate_20_to_25)
fre pilot_id if mi(abs_rate_15_to_20)
fre pilot_id if mi(abs_rate_10_to_15)
fre pilot_id if mi(abs_rate_5_to_10)


*adding in cluster variable (7/29)
egen disA_cluster = group(sid) if pilot_id=="disA_targeted_aug2018"
egen disB_cluster = group(hhld) if pilot_id=="disB_energybill_oct2018"
egen disC_cluster = group(hhld) if pilot_id=="disC_energybill_oct2018"
egen disD_cluster = group(hhld) if pilot_id=="disD_messaging_oct2019"
egen disE_cluster = group(hhld) if pilot_id=="disE_energybill_oct2018"
egen disF_cluster = group(hhld) if pilot_id=="disF_robocalls_oct2018"
egen temp_cluster = rowtotal(disA_cluster disB_cluster disC_cluster disD_cluster disE_cluster disF_cluster)
assert !mi(temp_cluster)
egen cluster = group(pid temp_cluster)


drop email robocall text mail_letter //delete existing binary variables for deliery from disA pilot rows
tab treatment_name pilot_id


*Create variable for arm name
gen Mail = 0
replace Mail = 1 if treatment_name =="parent_energybill" //disE (all mail)
replace Mail = 1 if treatment_name =="energybill" //disB (all mail, but half energybill and half lostlearning)
replace Mail = 1 if treatment_name =="lostlearning" //disB
replace Mail = 1 if treatment_name =="mail_letter" //disA (1/5 mail letter)

gen Backpack = 0
replace Backpack = 1 if treatment_name =="backpack_letter" //disA (1/5 backpack mail)

gen Email = 0
replace Email = 1 if treatment_name =="parent_email" //disC (all email)
replace Email = 1 if treatment_name =="email" //disA (1/5 email)

gen Robocall = 0
replace Robocall = 1 if treatment_name =="robocall" //disD (all call) and disA (1/5 call) and disF (all call)

gen Text = 0
replace Text = 1 if treatment_name =="text" //disA (1/5 text)


*Using the binary variables above, create an additional set of variables for if an arm was run in a specific district (for balance tables).  In other words, we want the control and treatment group for a given arm, not only the treatment group.


foreach treatment_arm in Mail Backpack Text Email Robocall {
	cap drop ran_`treatment_arm'
	gen ran_`treatment_arm' = 0
	replace ran_`treatment_arm' = 1 if `treatment_arm' ==1
	preserve 
		keep if `treatment_arm'==1
		levelsof(pilot_id), local(pilotids_`treatment_arm')
	restore
	
	foreach pilot_id in `pilotids_`treatment_arm'' {
		replace ran_`treatment_arm' = 1 if pilot_id=="`pilot_id'" & arm==1 //only for control
	}
}

foreach treatment_arm in Mail Backpack Text Email Robocall {
	tab ran_`treatment_arm', mi
}



fre treatment_name
tab treatment_name arm
replace treatment_name="Control" if treatment_name=="control" //for some reason, one control group has a capital c
replace treatment_name= "Mail" if Mail == 1 & treatment_name!="control"
replace treatment_name= "Backpack" if Backpack == 1 & treatment_name!="control"
replace treatment_name = "Email" if Email == 1  & treatment_name!="control" 
replace treatment_name = "Robocall" if Robocall == 1 & treatment_name!="control"
replace treatment_name = "Text" if Text == 1 & treatment_name!="control"
fre treatment_name


*Arm is wrong, so recode
drop arm
gen arm = 1 //control
replace arm = 2 if treatment_name=="Mail"
replace arm=3 if treatment_name== "Backpack"
replace arm=4 if treatment_name== "Text"
replace arm=5 if treatment_name== "Email"
replace arm=6 if treatment_name== "Robocall"

label define arm_label_new 1 "Control" 2 "Mail" 3 "Backpack" 4 "Text" 5 "Email" 6 "Robocall"
label values arm arm_label_new






//create rates per 10 weeks 

clonevar cum_days_absent_0_to_10 = cum_days_absent_wk_10
clonevar cum_days_enrolled_0_to_10 = cum_days_enrolled_wk_10
gen abs_rate_0_to_10 = cum_days_absent_0_to_10 / cum_days_enrolled_0_to_10


gen enrolled_at_all_wks_0_to_10 = 0 //variable for if they were enrolled at all in this period
replace enrolled_at_all_wks_0_to_10 = 1 if cum_days_enrolled_0_to_10>0 & cum_days_enrolled_0_to_10!=.
replace cum_days_absent_0_to_10 = 0 if cum_days_absent_0_to_10==. 
replace cum_days_enrolled_0_to_10 = 1 if cum_days_enrolled_0_to_10==.
assert cum_days_enrolled_0_to_10!=0


//disE drops at 15 weeks and disA drops at 19 weeks 
gen cum_days_absent_10_to_20 = cum_days_absent_wk_20 - cum_days_absent_wk_10 if pilot_id != "disF_robocalls_sep2019" & pilot_id != "disA_targeted_aug2018" & pilot_id != "disE_energybill_oct2018"
gen cum_days_enrolled_10_to_20 = cum_days_enrolled_wk_20 - cum_days_enrolled_wk_10 if pilot_id != "disF_robocalls_sep2019" & pilot_id != "disA_targeted_aug2018" & pilot_id != "disE_energybill_oct2018"

replace cum_days_absent_10_to_20 = cum_days_absent_wk_11 - cum_days_absent_wk_10 if pilot_id == "disF_robocalls_sep2019"
replace cum_days_enrolled_10_to_20 = cum_days_enrolled_wk_11 - cum_days_enrolled_wk_10 if pilot_id == "disF_robocalls_sep2019"

replace cum_days_absent_10_to_20 = cum_days_absent_wk_19 - cum_days_absent_wk_10 if pilot_id == "disA_targeted_aug2018"
replace cum_days_enrolled_10_to_20 = cum_days_enrolled_wk_19 - cum_days_enrolled_wk_10 if pilot_id == "disA_targeted_aug2018"

replace cum_days_absent_10_to_20 = cum_days_absent_wk_15 - cum_days_absent_wk_10 if pilot_id == "disE_energybill_oct2018"
replace cum_days_enrolled_10_to_20 = cum_days_enrolled_wk_15 - cum_days_enrolled_wk_10 if pilot_id == "disE_energybill_oct2018"

gen abs_rate_10_to_20 = cum_days_absent_10_to_20 / cum_days_enrolled_10_to_20 if pilot_id != "disF_robocalls_sep2019"


gen enrolled_at_all_wks_10_to_20 = 0 //variable for if they were enrolled at all in this period
replace enrolled_at_all_wks_10_to_20 = 1 if cum_days_enrolled_10_to_20>0 & cum_days_enrolled_10_to_20!=.
replace cum_days_absent_10_to_20 = 0 if cum_days_absent_10_to_20==. //there is no contrast for R if cum_days_absent is blank, so change to 0
replace cum_days_enrolled_10_to_20 = 1 if (cum_days_enrolled_10_to_20==. | cum_days_enrolled_10_to_20==0) & enrolled_at_all_wks_10_to_20!=1
assert cum_days_enrolled_10_to_20!=0




//disB drops at 27 weeks, disC drops at 28 weeks 
gen cum_days_absent_20_to_30 = cum_days_absent_wk_27 - cum_days_absent_wk_20 if pilot_id == "disB_energybill_oct2018"
gen cum_days_enrolled_20_to_30 = cum_days_enrolled_wk_27 - cum_days_enrolled_wk_20 if pilot_id == "disB_energybill_oct2018"

replace cum_days_absent_20_to_30 = cum_days_absent_wk_28 - cum_days_absent_wk_20 if pilot_id == "disC_energybill_oct2018"
replace cum_days_enrolled_20_to_30 = cum_days_enrolled_wk_28 - cum_days_enrolled_wk_20 if pilot_id == "disC_energybill_oct2018"

gen abs_rate_20_to_30 = cum_days_absent_20_to_30 / cum_days_enrolled_20_to_30 if pilot_id != "disF_robocalls_sep2019" & pilot_id != "disE_energybill_oct2018" & pilot_id != "disD_messaging_oct2019" & pilot_id != "disA_targeted_aug2018"


gen enrolled_at_all_wks_20_to_30 = 0 //variable for if they were enrolled at all in this period
replace enrolled_at_all_wks_20_to_30 = 1 if cum_days_enrolled_20_to_30>0 & cum_days_enrolled_20_to_30!=.
replace cum_days_absent_20_to_30 = 0 if cum_days_absent_20_to_30==.
replace cum_days_enrolled_20_to_30 = 1 if (cum_days_enrolled_20_to_30==. | cum_days_enrolled_20_to_30==0) & enrolled_at_all_wks_20_to_30!=1
assert cum_days_enrolled_20_to_30!=0


//create prior abs rate variable
cap drop prior_abs_rate
gen prior_abs_rate = prior_days_absent / prior_days_enrolled
assert mi_prior == 1 if prior_abs_rate == .

//combine arms and rand bin vars
assert treatment == 0 if arm == 1 //checking all controls are treatment 
gen treatment_name_2 = "control"
replace treatment_name_2 = "personalized_message" if treatment_name != "Control"

*Variable to analyze by prior absence
gen prior_abs_high = cond(prior_abs_rate >= 0.1, 1, 0) //using 10% (typical chronic absence rate)


*for the roughly 3,000 students who are post-treat attriters, calculate their cum_abs_rate manually (they have blank values currently)
replace cum_abs_rate = cum_days_absent/cum_days_enrolled if cum_abs_rate==.

tab pr_prior_abs_rate, mi //most are missing
cap drop pr_prior_abs_rate
tab pr_prior_adj_abs_rate, mi //doesnt seem to be missing any
assert mi_pr_prior == mi_prior
assert pr_prior_days_absent == 0 & pr_prior_adj_abs_rate ==0 if mi_pr_prior==1
clonevar pr_prior_abs_rate = prior_abs_rate
assert  pr_prior_abs_rate == prior_abs_rate

*Binary variable for if disD is excluded (only focus on when disD_excluded==1)
gen disD_excluded = 0
replace disD_excluded = 1 if strpos(pilot_id, "disD")==0

*Stuff for balance tables (see "check_balance" file)
gen Black = 0 
replace Black = 1 if xrace==1
label variable Black Black

gen Hispanic = 0 
replace Hispanic = 1 if xrace==3
label variable Hispanic Hispanic

gen White = 0
replace White = 1 if xrace==5
label variable White White

label variable xmale Male
label variable xell English Language Learner
label variable xfrpl Free/Reduced Price Lunch (FRPL)
label variable xsped Special Education
label variable pre_treat_abs_rate "Pre-Treatment Absence Rate"
label variable pr_prior_abs_rate "Prior-Year Absence Rate"
label variable mi_pr_prior "Missing Prior-Year Absence Rate"
label variable mi_schgr_prior "Missing Prior-Year School Grade Average Days Absent"
label variable mi_sch_prior "Missing Prior-Year School Average Days Absent"
label variable mi_pre_treat "Missing Pre-Treatment Absence Rate"

gen log_sch_prior = log(sch_prior_avg_days_absent)
label variable log_sch_prior "Log of Prior-Year School Average Days Absent"
gen log_schgr_prior= log(schgr_prior_avg_days_absent)
label variable log_schgr_prior "Log of Prior-Year School-Grade Average Days Absent"

cap drop _merge
//save data in outputs 
save "${outputs}/stacked_model_data_file", replace 

