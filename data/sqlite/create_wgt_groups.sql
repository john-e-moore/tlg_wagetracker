create table wgt_groups as
with 
	cps as (
		select 
			personid
			, date
			, age76
			, female76
			, race76
			, censusdiv76
			, occupation76
			, occupation76_tm12
			, industry76
			, industry76_tm12
			, recession76
			, metstat78
			/* wageperhr82 == wageperhrclean82 for all values; not sure why 'clean' exists */
			, wageperhr82
			, wageperhr82_tm12
			, paidhrly82
			, paidhrly82_tm12
			, wagegrowthtracker83
			, employer89
			, educ92
			, lfdetail94
			, lfdetail94_tm12
			, sameemployer94
			, sameemployer94_tm1
			, sameemployer94_tm2
			, sameactivities94
			, sameactivities94_tm1
			, sameactivities94_tm2
			, strftime('%Y', date) as year
		    , strftime('%m', date) as month
		    , strftime('%Y-%m', date) as date_monthly
		from cps_harmonized_longitudinally_matched
		where age76 >= 16
		order by date desc
	),
	/* Wage quartiles */
	ranked_wages as (
	    select
	        date_monthly
	        , wageperhr82
	        , ntile(4) over (
	            partition by date_monthly
	            order by wageperhr82
	        ) as quartile
	    from cps
	    where wageperhr82 is not null
	),
	wage_quartiles as (
		select
		    date_monthly
		    , quartile
		    , min(wageperhr82) as quartile_start
		    , max(wageperhr82) as quartile_end
		from ranked_wages
		group by date_monthly, quartile
	),
	wage_groups as (
		select 
			c.personid
			, c.date_monthly
			, c.wageperhr82
			, case
				when c.wageperhr82 between w.quartile_start and w.quartile_end
				then w.quartile
				else null
			end as wagegroup
		from cps c
		join wage_quartiles w on c.date_monthly = w.date_monthly
	),
    wage_groups_trimmed as (
    	select *
    	from wage_groups
    	where wagegroup is not null
    ),
	/* Create all groups */
	groups as (
		select 
			personid
			, date
			, date_monthly
			, wageperhr82
			, wageperhr82_tm12
			, case
				when paidhrly82 == 1 then 'Hourly'
				when paidhrly82 == 2 then 'Non-Hourly'
			end as hrlygroup
			, case
				when
					occupation76 != occupation76_tm12
					or industry76 != industry76_tm12
					or sameemployer94 == 2
					or sameemployer94_tm1 == 2
					or sameemployer94_tm2 == 2
					or sameactivities94 == 2
					or sameactivities94_tm1 == 2
					or sameactivities94_tm2 == 2
				then 'Job Switcher'
				else 'Job Stayer'
			end as jstayergroup
			, case 
				when age76 between 16 and 24 then '16-24'
				when age76 between 25 and 54 then '25-54'
				when age76 >= 55 then '55+'
			end as agegroup
			, case
				when female76 == 1 then 'Female'
				else 'Male'
			end as gendergroup
			, case
				when industry76 in (1,2,3,13) then 'Goods'
				when industry76 in (4,5,6,7,8,9,10,11,12) then 'Services'
			end as secgroup
			, case
				when educ92 between 1 and 3 then 'Nodegree'
				when educ92 between 6 and 7 then 'Bachelor+'
				when educ92 between 4 and 5 then 'Associates'
			end as edgroup3
			, case
				when educ92 between 1 and 3 then 'Nodegree'
				when educ92 between 4 and 7 then 'Degree'
			end as edgroup2
			, case
				when occupation76 in (11,12,13) then 'Professional'
				when occupation76 in (21,22,23,31,32,33,34) then 'Nonprofessional'
			end as occgroup
			, case
				when lfdetail94 == 6 or lfdetail94 between 8 and 20 then 'Full-time'
				when lfdetail94 == 7 or lfdetail94 between 21 and 32 then 'Part-time'
			end as ftptgroup
			, case
				when occupation76 in (32,33,34) then 'Low'
				when occupation76 in (21,22,23,31) then 'Middle'
				when occupation76 in (11,12,13) then 'High'
			end as skillgroup
			, case
				when industry76 in (1,2) then 'Construction & Mining'
				when industry76 == 9 then 'Education & Health'
				when industry76 in (6,7,8) then 'Finance and Business Services'
				when industry76 in (10,11) then 'Leisure & Hospitality'
				when industry76 == 3 then 'Manufacturing'
				when industry76 == 12 then 'Public Administration'
				when industry76 in (4,5) then 'Trade & Transportation'
			end as indgroup
			, case
				when race76 == 1 then 'White'
				when race76 in (2,3) then 'Nonwhite'
			end as racegroup
			, case
				when metstat78 == 1 then 'MSA'
				when metstat78 == 2 then 'NonMSA'
			end as msagroup
			, case
				when censusdiv76 == 1 then 'pac'
				when censusdiv76 == 2 then 'esc'
				when censusdiv76 == 3 then 'wsc'
				when censusdiv76 == 4 then 'mnt'
				when censusdiv76 == 5 then 'nen'
				when censusdiv76 == 6 then 'sat'
				when censusdiv76 == 7 then 'wnc'
				when censusdiv76 == 8 then 'enc'
				when censusdiv76 == 9 then 'mat'
			end as cdivgroup
		from cps
	)
select
	g.personid
	, g.date
	, g.hrlygroup
	, g.jstayergroup
	, g.agegroup
	, g.gendergroup
	, g.secgroup
	, g.edgroup3
	, g.edgroup2
	, g.occgroup
	, g.ftptgroup
	, g.skillgroup
	, g.indgroup
	, g.racegroup
	, g.msagroup
	, w.wagegroup
from groups g
join wage_groups_trimmed w 
	on g.personid = w.personid
	and g.date_monthly = w.date_monthly;

