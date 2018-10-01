select a.start_dt
     , a.end_dt
     , c.nm
     , a.multi_factors_nm
     , a.window_size
     , a.signal_cd
     , a.model_profit
     , a.bm_profit
     , a.term_type
     , a.factors_num
     , n.nm
     , d.factor_profit - d.index_profit
     , o.nm
     , e.factor_profit - e.index_profit
     , p.nm
     , f.factor_profit - f.index_profit
     , q.nm
     , g.factor_profit - g.index_profit
     , r.nm
     , h.factor_profit - h.index_profit
     , s.nm
     , i.factor_profit - i.index_profit
     , t.nm
     , j.factor_profit - j.index_profit
     , u.nm
     , k.factor_profit - k.index_profit
     , v.nm
     , l.factor_profit - l.index_profit
     , w.nm
     , m.factor_profit - m.index_profit
from result_last a
LEFT JOIN result_factor AS d
	ON a.factor_cd0 = d.factor_cd
  AND a.start_dt = d.start_dt
  AND a.end_dt = d.end_dt
  AND a.target_cd = d.target_cd
  AND a.window_size = d.window_size
LEFT JOIN result_factor AS e
  ON a.factor_cd1 = e.factor_cd
 AND a.start_dt = e.start_dt
 AND a.end_dt = e.end_dt
 AND a.target_cd = e.target_cd
 AND a.window_size = e.window_size
LEFT JOIN result_factor AS f
  ON a.factor_cd2 = f.factor_cd
 AND a.start_dt = f.start_dt
 AND a.end_dt = f.end_dt
 AND a.target_cd = f.target_cd
 AND a.window_size = f.window_size  
LEFT JOIN result_factor AS g
  ON a.factor_cd3 = g.factor_cd
 AND a.start_dt = g.start_dt
 AND a.end_dt = g.end_dt
 AND a.target_cd = g.target_cd
 AND a.window_size = g.window_size
LEFT JOIN result_factor AS h
  ON a.factor_cd4 = h.factor_cd
 AND a.start_dt = h.start_dt
 AND a.end_dt = h.end_dt
 AND a.target_cd = h.target_cd
 AND a.window_size = h.window_size
LEFT JOIN result_factor AS i
  ON a.factor_cd5 = i.factor_cd
 AND a.start_dt = i.start_dt
 AND a.end_dt = i.end_dt
 AND a.target_cd = i.target_cd
 AND a.window_size = i.window_size
LEFT JOIN result_factor AS j
  ON a.factor_cd6 = j.factor_cd
 AND a.start_dt = j.start_dt
 AND a.end_dt = j.end_dt
 AND a.target_cd = j.target_cd
 AND a.window_size = j.window_size
LEFT JOIN result_factor AS k
  ON a.factor_cd7 = k.factor_cd
 AND a.start_dt = k.start_dt
 AND a.end_dt = k.end_dt
 AND a.target_cd = k.target_cd
 AND a.window_size = k.window_size
LEFT JOIN result_factor AS l
  ON a.factor_cd8 = l.factor_cd
 AND a.start_dt = l.start_dt
 AND a.end_dt = l.end_dt
 AND a.target_cd = l.target_cd
 AND a.window_size = l.window_size
LEFT JOIN result_factor AS m
  ON a.factor_cd9 = m.factor_cd
 AND a.start_dt = m.start_dt
 AND a.end_dt = m.end_dt
 AND a.target_cd = m.target_cd
 AND a.window_size = m.window_size
LEFT JOIN item AS n
    ON a.factor_cd0 = n.cd
LEFT JOIN item AS o
    ON a.factor_cd1 = o.cd
LEFT JOIN item AS p
    ON a.factor_cd2 = p.cd
LEFT JOIN item AS q
    ON a.factor_cd3 = q.cd
LEFT JOIN item AS r
    ON a.factor_cd4 = r.cd
LEFT JOIN item AS s
    ON a.factor_cd5 = s.cd
LEFT JOIN item AS t
    ON a.factor_cd6 = t.cd
LEFT JOIN item AS u
    ON a.factor_cd7 = u.cd
LEFT JOIN item AS v
    ON a.factor_cd8 = v.cd
LEFT JOIN item AS w
    ON a.factor_cd9 = w.cd
, (
	select start_dt 			 as 'start_dt'
		  , end_dt 				 as 'end_dt'
		  , target_cd			 as 'target_cd'
		  , term_type			 as 'term_type'
		  , max(model_profit) as 'model_profit'
	from result_last
	where start_dt in ('2012-01-01')
	and end_dt = '2018-08-31'
	group by start_dt, end_dt, target_cd, term_type
) b
, item c

where a.start_dt = b.start_dt
and a.end_dt = b.end_dt
and a.target_cd = b.target_cd
and a.model_profit = b.model_profit
and a.term_type = b.term_type
and a.target_cd = c.cd
;