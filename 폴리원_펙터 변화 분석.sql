select a.start_dt, a.end_dt, c.nm, a.multi_factors_nm, a.factors_num
, a.model_profit, a.bm_profit, a.model_profit-a.bm_profit, a.signal_cd
, d.nm, e.nm, f.nm, g.nm, h.nm, i.nm, j.nm, k.nm, l.nm, m.nm, a.update_tm
from result_last a
LEFT JOIN item AS d
    ON a.factor_cd0 = d.cd
LEFT JOIN item AS e
    ON a.factor_cd1 = e.cd
LEFT JOIN item AS f
    ON a.factor_cd2 = f.cd
LEFT JOIN item AS g
    ON a.factor_cd3 = g.cd
LEFT JOIN item AS h
    ON a.factor_cd4 = h.cd
LEFT JOIN item AS i
    ON a.factor_cd5 = i.cd
LEFT JOIN item AS j
    ON a.factor_cd6 = j.cd
LEFT JOIN item AS k
    ON a.factor_cd7 = k.cd
LEFT JOIN item AS l
    ON a.factor_cd8 = l.cd
LEFT JOIN item AS m
    ON a.factor_cd9 = m.cd
,(
	select start_dt as start_dt
		  , end_dt as end_dt
		  , curr_dt as curr_dt
		  , target_cd as target_cd
/*		  , window_size as window_size*/
		  , max(model_profit) as model_profit
	from result_last
	group by start_dt, end_dt, curr_dt, target_cd/*, window_size*/
 ) b
, item c
where a.start_dt = b.start_dt
and a.end_dt = b.end_dt
and a.curr_dt = b.curr_dt
and a.target_cd = b.target_cd
/*and a.window_size = b.window_size*/
and a.model_profit = b.model_profit
/*and a.target_cd = 1*/
and a.target_cd = c.cd
order by a.target_cd, a.end_dt
