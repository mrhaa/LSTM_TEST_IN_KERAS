SELECT c.start_dt AS "시물 시작일"
	  , c.end_dt AS "시물 기준일"
	  , d.nm AS "타겟 INDEX"
	  , c.window_size AS "Z-Score 샘플크기"
	  , c.factors_num AS "팩터수(최대10)"
	  , c.model_imp AS "Z-Score Growth"
	  , n.nm AS "1번 팩터"
	  , c.factor_imp0 AS "1번 영향"
	  , o.nm AS "2번 팩터"
	  , c.factor_imp1 AS "2번 영향"
	  , p.nm AS "3번 팩터"
	  , c.factor_imp2 AS "3번 영향"
	  , q.nm AS "4번 팩터"
	  , c.factor_imp3 AS "4번 영향"
	  , r.nm AS "5번 팩터"
	  , c.factor_imp4 AS "5번 영향"
	  , s.nm AS "6번 팩터"
	  , c.factor_imp5 AS "6번 영향"
	  , t.nm AS "7번 팩터"
	  , c.factor_imp6 AS "7번 영향"
	  , u.nm AS "8번 팩터"
	  , c.factor_imp7 AS "8번 영향"
	  , v.nm AS "9번 팩터"
	  , c.factor_imp8 AS "9번 영향"
	  , w.nm AS "10번 팩터"
	  , c.factor_imp9 AS "10번 영향"
     , c.multi_factors_nm AS "멀티팩터"
     , d.cd AS "타켓cd"
     , c.multi_factors_cd AS "멀티cd"
FROM result_last a
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
, result_factor_impact c
, item d
WHERE a.target_cd = b.target_cd
  AND a.start_dt = b.start_dt
  AND a.end_dt = b.end_dt
  AND a.term_type = b.term_type
  AND a.model_profit = b.model_profit
  AND a.target_cd = c.target_cd
  AND a.start_dt = c.start_dt
  AND a.end_dt = c.end_dt
  AND a.term_type = c.term_type
  AND a.multi_factors_cd = c.multi_factors_cd
  AND a.factors_num = c.factors_num
  AND a.window_size = c.window_size
  AND a.target_cd = d.cd