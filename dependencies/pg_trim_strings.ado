program define pg_trim_strings
	qui{
		ds , has(type string)				// List names of vars that are strings
		local string_vars = r(varlist)		// Create local 
		foreach var of local string_vars{	// Trimming each string in the local 
			replace `var' = trim(`var')
		}
	}
end
