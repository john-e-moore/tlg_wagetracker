/* 
* Author: John Moore
* Date: 2024-02-02
* 
* Joins groups created in create_wgt_groups back to some columns from
* the original data. This is the dataset that will be used for unweighted
* aggregations.
*/
create table wgt_unweighted as
with 
	wages as (
		select 
			personid
			, date
			, strftime('%Y', date) as year
		    , strftime('%m', date) as month
		    , strftime('%Y-%m', date) as date_monthly
		    , recession76
		    /* Atlanta Fed uses average of wage and 12-month lag to create quartiles. */
			, (wageperhr82 + wageperhr82_tm12) / 2 as wage_hr_avg
			, wagegrowthtracker83 
		from cps_harmonized_longitudinally_matched
		where 
			age76 >= 16
			/* Wage observations present for current observation and 12-month lag of same person. */
			and wagegrowthtracker83 is not null
	),
	groups as (
		select * 
		from wgt_groups
	)
select
	g.*
	, w.year
	, w.month
	, w.date_monthly
	, w.recession76
	, w.wage_hr_avg
	, w.wagegrowthtracker83
from groups g
join wages w
	on g.personid = w.personid
	and g.date = w.date;
			