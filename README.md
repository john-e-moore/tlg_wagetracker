	• Objective
		○ Slice and dice data based on wage quartile, age, and education
		○ Atlanta Fed's wage tracker uses moving averages and medians (?), we want to be able to view averages 
	• Data source
		○ CADRE - Current Population Survey (kansascityfed.org)
		○ Harmonized Variable and Longitudinally Matched [Atlanta Federal Reserve] (1976-Present)
	• Important information
		○ Includes only data for 16+ year olds
		○ Each variable's start year is indicated at the end of the variable name
			§ For example, mlr76 is coded with consistent values (1 = employed, 2 = unemployed, 3 = not in labor force) from 1976 until today
		○ For many variables, lags are provided
			§ For example, mlr76_tm12 is the individual's labor force status from 12 months ago

* Download the Harmonized Variable and Longitudinally Matched [Atlanta Federal Reserve] (1976-Present) dataset (.dta file).
* Use 'ingest/ingest_cps.py' to ingest the data into a SQLite table.
* Run 'data/sqlite/scripts/create_wgt_groups' to create the dimensions used to slice the data.
* Run 'data/sqlite/scripts/create_wgt_unweighted' to make the final longitudinally matched CPS dataset, observations for like individuals 12 months apart.
* Load the resulting table into a BI tool. From there, you can calculate moving averages and make plots.

