select a.*
from result_last a
, (
select start_dt as 'start_dt'
, end_dt as 'end_dt'
, target_cd as 'target_cd'
, term_type as 'term_type'
, max(model_profit) as 'model_profit'
from result_last
where start_dt in ('2001-01-01', '2012-01-01')
and end_dt = '2018-08-31'
group by start_dt, end_dt, target_cd, term_type
) b
where a.start_dt = b.start_dt
and a.end_dt = b.end_dt
and a.target_cd = b.target_cd
and a.model_profit = b.model_profit
and a.term_type = b.term_type
