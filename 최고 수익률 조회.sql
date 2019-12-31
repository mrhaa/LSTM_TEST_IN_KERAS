SELECT a.start_dt AS "시물 시작일"
     , a.end_dt AS "시뮬 기준일"
     , c.nm AS "타겟 INDEX"
     , a.multi_factors_nm AS "멀티펙터"
     , a.window_size AS "Z-Score 샘플크기"
     , a.signal_cd AS "SIGNAL"
     , a.model_profit AS "모델 수익률(누적)"
     , a.bm_profit AS "INDEX 수익률(누적)"
     , a.term_type AS "모델 기간타입"
     , a.factors_num AS "펙터수(최대10)"
     , n.nm AS "1번 펙터"
     , d.factor_profit - d.index_profit AS "1번 수익률"
     , o.nm AS "2번 펙터"
     , e.factor_profit - e.index_profit AS "2번 수익률"
     , p.nm AS "3번 펙터"
     , f.factor_profit - f.index_profit AS "3번 수익률"
     , q.nm AS "4번 펙터"
     , g.factor_profit - g.index_profit AS "4번 수익률"
     , r.nm AS "5번 펙터"
     , h.factor_profit - h.index_profit AS "5번 수익률"
     , s.nm AS "6번 펙터"
     , i.factor_profit - i.index_profit AS "6번 수익률"
     , t.nm AS "7번 펙터"
     , j.factor_profit - j.index_profit AS "7번 수익률"
     , u.nm AS "8번 펙터"
     , k.factor_profit - k.index_profit AS "8번 수익률"
     , v.nm AS "9번 펙터"
     , l.factor_profit - l.index_profit AS "9번 수익률"
     , w.nm AS "10번 펙터"
     , m.factor_profit - m.index_profit AS "10번 수익률"
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
		  , MAX(model_profit) AS 'model_profit'
	FROM result_last
	WHERE start_dt = '2011-12-31'
	  AND end_dt = '2019-11-30'
	GROUP BY start_dt, end_dt, target_cd, term_type
) b
, item c
WHERE a.start_dt = b.start_dt
  AND a.end_dt = b.end_dt
  AND a.target_cd = b.target_cd
  AND a.model_profit = b.model_profit
  AND a.term_type = b.term_type
  AND a.target_cd = c.cd

