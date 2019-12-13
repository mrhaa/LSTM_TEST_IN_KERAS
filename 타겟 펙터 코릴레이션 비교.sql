
SELECT a.target_nm
     , a.factor_nm
     , a.end_dt
     , a.norm_yn
     , a.lag
     , a.window_size AS 기간
     , a.value AS 값
     , a.hit_ratio AS HR
  FROM target_factor_corr a,
	(SELECT target_nm
	      , end_dt
	      , norm_yn
--			, MAX(value) MAX_V
--			, MIN(value) MIN_V
			, MAX(hit_ratio) HR
	   FROM target_factor_corr
	  WHERE value <> 1
	    AND hit_ratio <> 0
	    AND lag > 1
	    AND norm_yn = 1
	  GROUP BY target_nm, end_dt, norm_yn) b
  WHERE a.target_nm = b.target_nm
    AND a.end_dt = b.end_dt
    AND a.norm_yn = b.norm_yn
    AND a.hit_ratio = b.HR
--    AND (a.value = b.MAX_V OR a.value = b.MIN_V)
    AND a.lag > 0
  ORDER BY a.target_nm, a.value DESC