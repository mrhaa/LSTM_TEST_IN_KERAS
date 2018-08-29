

select a.*
from result_last a
,(
	select start_dt as start_dt
		  , end_dt as end_dt
		  , curr_dt as curr_dt
		  , target_cd as target_cd
		  , window_size as window_size
		  , max(model_profit) as model_profit
	from result_last
	group by start_dt, end_dt, curr_dt, target_cd, window_size
 ) b
where a.start_dt = b.start_dt
and a.end_dt = b.end_dt
and a.curr_dt = b.curr_dt
and a.target_cd = b.target_cd
and a.window_size = b.window_size
and a.model_profit = b.model_profit