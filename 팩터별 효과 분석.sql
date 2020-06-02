SELECT d.nm
     , c.multi_factors_nm
	  , c.start_dt
	  , c.end_dt
	  , c.factors_num
	  , c.window_size
	  , c.model_imp
	  , c.factor_imp0
	  , c.factor_imp1
	  , c.factor_imp2
	  , c.factor_imp3
	  , c.factor_imp4
	  , c.factor_imp5
	  , c.factor_imp6
	  , c.factor_imp7
	  , c.factor_imp8
	  , c.factor_imp9
FROM result_last a
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