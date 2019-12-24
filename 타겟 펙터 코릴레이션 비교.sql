
SELECT a.target_nm
     , a.end_dt
     , a.norm_yn
     , a.lag
     , a.factor_nm AS 펙터
     , a.window_size AS 기간
     , a.value AS CORR
     , a.hit_ratio AS HR
  FROM target_factor_corr a,
	(SELECT target_nm
	      , end_dt
	      , norm_yn
	      , lag
			, MAX(value) MAX_V
			, MIN(value) MIN_V
			, MAX(hit_ratio) MAX_HR
			, MIN(hit_ratio) MIN_HR			
	   FROM target_factor_corr
	  GROUP BY target_nm, end_dt, norm_yn, lag) b
  WHERE a.target_nm = b.target_nm
    AND a.end_dt = b.end_dt
    AND a.norm_yn = b.norm_yn
    AND a.lag = b.lag
    AND (a.hit_ratio = b.MAX_HR OR a.hit_ratio = b.MIN_HR)
--    AND (a.value = b.MAX_V OR a.value = b.MIN_V)
  ORDER BY a.target_nm, a.value DESC