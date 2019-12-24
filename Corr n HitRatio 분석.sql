WITH TMP AS 
((SELECT target_nm
       , factor_nm
		 , end_dt
		 , AVG(value) / STD(value) AS corr
		 , AVG(hit_ratio) / STD(hit_ratio) AS hit_ratio
    FROM target_factor_corr
   GROUP BY target_nm, factor_nm, end_dt
   ORDER BY AVG(hit_ratio)
   LIMIT 10)
  UNION
 (SELECT target_nm
       , factor_nm
		 , end_dt
		 , AVG(value) / STD(value) AS corr
		 , AVG(hit_ratio) / STD(hit_ratio) AS hit_ratio
    FROM target_factor_corr
   GROUP BY target_nm, factor_nm, end_dt
   ORDER BY AVG(hit_ratio) DESC
   LIMIT 10))
SELECT *, corr * hit_ratio FROM TMP
 ORDER BY corr * hit_ratio DESC
 