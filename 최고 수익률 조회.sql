SELECT a.start_dt AS "시물 시작일"
     , a.end_dt AS "시뮬 기준일"
     , c.nm AS "타겟 INDEX"
     , a.window_size AS "Z-Score 샘플크기"
     , a.signal_cd AS "SIGNAL"
     , a.score AS "Z-Score"
     , round(a.model_profit-1,4) AS "모델 수익률(누적)"
     , round(a.bm_profit-1,4) AS "INDEX 수익률(누적)"
     , a.term_type AS "모델 기간타입"
     , a.factors_num AS "팩터수(최대10)"
     , n.nm AS "1번 팩터"
     , format(d.factor_profit - d.index_profit,2) AS "1번 초과수익률"
     , d.score AS "1번 스코어"
     , o.nm AS "2번 팩터"
     , format(e.factor_profit - e.index_profit,2) AS "2번 초과수익률"
     , e.score AS "2번 스코어"
     , p.nm AS "3번 팩터"
     , format(f.factor_profit - f.index_profit,2) AS "3번 초과수익률"
     , f.score AS "3번 스코어"
     , q.nm AS "4번 팩터"
     , format(g.factor_profit - g.index_profit,2) AS "4번 초과수익률"
     , g.score AS "4번 스코어"
     , r.nm AS "5번 팩터"
     , format(h.factor_profit - h.index_profit,2) AS "5번 초과수익률"
     , h.score AS "5번 스코어"
     , s.nm AS "6번 팩터"
     , format(i.factor_profit - i.index_profit,2) AS "6번 초과수익률"
     , i.score AS "6번 스코어"          
     , t.nm AS "7번 팩터"
     , format(j.factor_profit - j.index_profit,2) AS "7번 초과수익률"
     , j.score AS "7번 스코어"          
     , u.nm AS "8번 팩터"
     , format(k.factor_profit - k.index_profit,2) AS "8번 초과수익률"
     , k.score AS "8번 스코어"          
     , v.nm AS "9번 팩터"
     , format(l.factor_profit - l.index_profit,2) AS "9번 초과수익률"
     , l.score AS "9번 스코어"          
     , w.nm AS "10번 팩터"
     , format(m.factor_profit - m.index_profit,2) AS "10번 초과수익률"
     , m.score AS "10번 스코어"
     , a.multi_factors_nm AS "멀티팩터"
     , c.cd AS "타켁 cd"
     , a.multi_factors_cd AS "멀티cd"
FROM result_last a
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
	SELECT start_dt 			 AS 'start_dt'
		  , end_dt 				 AS 'end_dt'
		  , target_cd			 AS 'target_cd'
		  , term_type			 AS 'term_type'
--		  , window_size       AS 'window_size'
		  , MAX(model_profit) AS 'model_profit'
--		  , MAX(factors_num)  AS 'factors_num'
	FROM result_last
	WHERE start_dt = '2011-12-31'
	  AND end_dt = '2020-05-31'
	GROUP BY start_dt, end_dt, target_cd, term_type-- , window_size
) b
, item c
WHERE a.start_dt = b.start_dt
  AND a.end_dt = b.end_dt
  AND a.target_cd = b.target_cd
--  AND a.window_size = b.window_size
  AND a.model_profit = b.model_profit
--  AND a.factors_num = b.factors_num
  AND a.term_type = b.term_type
  AND a.target_cd = c.cd
ORDER BY a.start_dt, a.end_dt, a.target_cd, a.model_profit DESC, a.score DESC, a.factors_num
