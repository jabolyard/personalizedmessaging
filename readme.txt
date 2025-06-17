District names are anonymized throughout code to A-F. 
Certain identifying variable names and codes are replaced with "Removed". Certain district-specific codes were also replaced.
Run order: 
01: Randomization code was run prior to the experiment's implementation. 
02: Each district's afile program was run after the experiment to combine randomization lists with student data. 
"stack_pooled_model_data" was run to combine data from each experiment into one usable dataset. 
03: "check_balance" and "personalized_messaging_spring2022_analysis" were used to analyze the combined dataset. 

These programs reference several user-written dependencies:
pg_randomize This randomizes the dataset according to specified ratio and blocking variables, and is included. 
pg_randgroup This recodes randomization blocking variables for future analysis, and is included. 
pg_check_data. This produces data checks used internally, but is not crucial to the paper, and is omitted.  
pg_balance_table This produces individual balance tables for each experiment but was not used to produce tables for the paper, and is omitted. 
pg_balance_chart This produces individual balance charts for each experiment but was not used to produce tables for the paper, and is omitted. 
pg_trim_strings Data cleaning program, included. 
pg_replace_dashes Data cleaning program, included. 
pg_xwalk This crosswalks local id's and anonymized id's that we use. The program contains sensitive information, and is omitted. 
pg_get_weekly This generates additional absence variables from daily data, and is included. 
pg_rand_report This outputs randomization information in a report, but is not used for the paper, and is omitted. 