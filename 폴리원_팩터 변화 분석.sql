select a.start_dt
     , a.end_dt
     , c.cd
	  , c.nm as 'target idx'
	  , a.multi_factors_nm
	  , a.factors_num as '#f'
     , a.model_profit
	  , a.bm_profit
	  , a.model_profit - a.bm_profit as 'gap'
	  , a.signal_cd as 'signal'
     , d.nm as 'f0'
	  , e.nm as 'f1'
	  , f.nm as 'f2'
	  , g.nm as 'f3'
	  , h.nm as 'f4'
	  , i.nm as 'f5'
	  , j.nm as 'f6'
	  , k.nm as 'f7'
	  , l.nm as 'f8'
	  , m.nm as 'f9'
	  , a.update_tm
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
		  , target_cd as target_cd
        , term_type as term_type
		  , max(model_profit) as model_profit
	from result_last
	Where term_type = 3 
	group by start_dt, end_dt, target_cd, term_type
 ) b
, item c
where a.start_dt = b.start_dt
and a.end_dt = b.end_dt
and a.target_cd = b.target_cd
and a.term_type = b.term_type
and a.model_profit = b.model_profit
/*and a.target_cd = 1*/
and a.target_cd = c.cd
order by a.target_cd, a.end_dt
