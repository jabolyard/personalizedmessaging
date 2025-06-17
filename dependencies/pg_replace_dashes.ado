program define pg_replace_dashes
	qui {
		ds , has(type string)
		local string_vars = r(varlist)
		foreach var of local string_vars{
			replace `var' = "" if regexm(`var',"^-+$")
		}
	}
end
