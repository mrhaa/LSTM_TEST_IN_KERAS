WITH max_profit_profile AS 
(	 SELECT a.start_dt AS "start_dt"
	      , a.end_dt AS "end_dt"
	      , a.target_cd AS "target_cd"
	      , a.min_max_check_term AS "min_max_check_term"
    	   , a.weight_check_term AS "weight_check_term"
	      , a.window_size AS "window_size"
	      , a.multi_factors_nm AS "multi_factors_nm"
	      , a.multi_factors_cd AS "multi_factors_cd"	      
	      , a.factors_num AS "factors_num"
	      , a.model_profit AS "model_profit"
	      , a.bm_profit AS "bm_profit"
		FROM result_last a
			, (
				SELECT start_dt 			 AS 'start_dt'
					  , end_dt 				 AS 'end_dt'
					  , target_cd			 AS 'target_cd'
					  , min_max_check_term AS 'min_max_check_term'
					  , weight_check_term AS 'weight_check_term'
					  , MAX(model_profit) AS 'model_profit'
				FROM result_last
				WHERE start_dt = '2011-12-31'
--				WHERE start_dt = '2018-12-31'
--				WHERE start_dt = '2018-08-31'
				  AND end_dt = '2020-09-30'
				GROUP BY start_dt, end_dt, target_cd, min_max_check_term, weight_check_term
			) b
	  WHERE a.start_dt = b.start_dt
		 AND a.end_dt = b.end_dt
		 AND a.target_cd = b.target_cd
		 AND a.min_max_check_term = b.min_max_check_term
		 AND a.weight_check_term = b.weight_check_term
		 AND a.model_profit = b.model_profit
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
     , a.min_max_check_term
	  , a.weight_check_term
  FROM max_profit_profile a
     , item c
     , result b
 LEFT JOIN ivalues AS d 
   ON b.curr_dt = d.date
  AND b.target_cd = d.item_cd
 WHERE a.start_dt = b.start_dt
   AND a.end_dt = b.end_dt
   AND a.target_cd = b.target_cd
--   AND a.term_type = b.term_type
   AND a.window_size = b.window_size
   AND a.multi_factors_cd = b.multi_factors_cd
   AND a.factors_num = b.factors_num
   AND b.target_cd = c.cd
 ORDER BY a.min_max_check_term, a.weight_check_term, a.target_cd, b.multi_factors_nm, b.curr_dt