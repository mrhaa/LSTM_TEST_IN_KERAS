WITH max_profit_profile AS (SELECT a.start_dt AS "start_dt"
     , a.end_dt AS "end_dt"
     , a.target_cd AS "target_cd"
     , a.term_type AS "term_type"
     , a.window_size AS "window_size"
     , a.multi_factors_nm AS "multi_factors_nm"
     , a.factors_num AS "factors_num"
     , a.model_profit AS "model_profit"
     , a.bm_profit AS "bm_profit"
FROM result_last a
, (
	SELECT start_dt 			 AS 'start_dt'
		  , end_dt 				 AS 'end_dt'
		  , target_cd			 AS 'target_cd'
		  , term_type			 AS 'term_type'
		  , MAX(model_profit) AS 'model_profit'
	FROM result_last
	WHERE start_dt = '2011-12-31'
	  AND end_dt = '2019-12-31'
	GROUP BY start_dt, end_dt, target_cd, term_type
) b
WHERE a.start_dt = b.start_dt
  AND a.end_dt = b.end_dt
  AND a.target_cd = b.target_cd
  AND a.model_profit = b.model_profit
  AND a.term_type = b.term_type
)
SELECT b.start_dt
     , b.end_dt
     , b.curr_dt
     , c.nm
     , b.multi_factors_nm
     , b.window_size
     , b.signal_cd
     , b.score
     , b.model_profit
     , b.bm_profit
     , d.value
  FROM max_profit_profile a
     , result b
     , item c
     , ivalues d
 WHERE a.start_dt = b.start_dt
   AND a.end_dt = b.end_dt
   AND a.target_cd = b.target_cd
   AND a.term_type = b.term_type
   AND a.window_size = b.window_size
   AND a.multi_factors_nm = b.multi_factors_nm
   AND a.factors_num = b.factors_num
   AND b.target_cd = c.cd
   AND c.cd = d.item_cd
   AND b.target_cd = d.item_cd
   AND b.curr_dt = d.date
