/*******************************************************************************

	File: personalized_messaging_may2022_paper_master.do
	Purpose: Run full paper pipeline 
*******************************************************************************/


	/**************************************************
	(0) Set up
	**************************************************/

	cap log close
	clear all
	set more off
	set seed 05132022
	
	* Date
	local currdate = string(date(c(current_date), "DMY"), "%tdCCYYNNDD")
	global today = `currdate'	
	
	* Globals 
		* General top-level directories
		global data_dir				"R:/Proving_Ground/data/papers/personalized_messaging_spring2022"
		global programs				"R:/Proving_Ground/programs_new/papers/personalized_messaging_spring2022"
		global tables_figures		"R:/Proving_Ground/tables_figures_new/papers/personalized_messaging_spring2022"
		* Data subdirectories
		global raw					"${data_dir}/raw"		
		global interim				"${data_dir}/interim"				
		global inputs				"${data_dir}/input"
		global outputs				"${data_dir}/output"
		global logs					"${data_dir}/logs"
		* Code subdirectories
		global model_data_code 		"${programs}/model_data_code"
		* Use archived utilities 
		sysdir set PERSONAL		 	"${programs}/utilities/"	
	
	* Set log
	log using "${logs}/master_do_file_run_${today}.log", replace
	
	
	/********************************************
	(1) Stitch daily absences fror all partners
	********************************************/

	pg_stitch_abs disE, dwindir("${raw}/disE") ///
						 interimdir("${interim}/disE") ///
						 logdir("${logs}/disE")
	pg_stitch_abs disF, dwindir("${raw}/disF") ///
						interimdir("${interim}/disF") ///
						logdir("${logs}/disF")	
	pg_stitch_abs disC, dwindir("${raw}/disC") ///
						  interimdir("${interim}/disC") ///
						  logdir("${logs}/disC")
	pg_stitch_abs disB, dwindir("${raw}/disB") ///
						  interimdir("${interim}/disB") ///
					      logdir("${logs}/disB")	
	pg_stitch_abs disA, dwindir("${raw}/disA") ///
						  interimdir("${interim}/disA") ///
						  logdir("${logs}/disA")	
	pg_stitch_abs disD, dwindir("${raw}/disD") ///
						  interimdir("${interim}/disD") ///
						  logdir("${logs}/disD")	
	


	/*******************************************
	(2) Stitch analysis files for all partners
	*******************************************/

	
	help pg_stitch
	
	pg_stitch disE, test_off geocode_off ///
					dwpath("${raw}/disE") ///
					dwclean("${inputs}/disE") ///
					interim("${interim}/disE")
	pg_stitch disF, test_off geocode_off ///
					dwpath("${raw}/disF") ///
					dwclean("${inputs}/disF") ///
					interim("${interim}/disF")
	pg_stitch disC, test_off geocode_off ///
					dwpath("${raw}/disC") ///
					dwclean("${inputs}/disC") ///
					interim("${interim}/disC")
	pg_stitch disB, test_off geocode_off ///
					dwpath("${raw}/disB") ///
					dwclean("${inputs}/disB") ///
					interim("${interim}/disB")
	pg_stitch_2018_2019_only disA, test_off geocode_off ///
					 dwpath("${raw}/disA") ///
					 dwclean("${inputs}/disA") ///
					 interim("${interim}/disA")
	pg_stitch disD, test_off geocode_off ///
					  dwpath("${raw}/disD") ///
					  dwclean("${inputs}/disD") ///
					  interim("${interim}/disD")
	



					  
	/*********************************************
	(3) Run individual pilot model data files
	*********************************************/


	* disE
	do "${model_data_code}/disE_energybill_oct2018/afile_disE_energybill_oct2018_paper.do"
	
	
	* disF
	do "${model_data_code}/disF_robocalls_sep2019/afile_disF_robocalls_sep2019_paper.do"
	
	
	* disC
	do "${model_data_code}/disC_energybill_oct2018/afile_disC_energybill_oct2018_paper.do"
	
	* disB
	do "${model_data_code}/disB_energybill_oct2018/afile_disB_energybill_oct2018_paper.do"
	
	* disA
	do "${model_data_code}/disA_targeted_aug2018/afile_disA_targeted_aug2018_paper.do"
	
	* disD
	do "${model_data_code}/disD_messaging_oct2019/afile_disD_messaging_oct2019_paper.do"
	

	
	/********************************************
	(4) Set up pooled pilot model data file
	********************************************/
	
	do "${model_data_code}/stack_pooled_model_data.do"
	
	

	/**************************
	(5) Check balance
	**************************/

	do "${model_data_code}/check_balance.do"
	
	
	
	

	
	
	
	cap log close




