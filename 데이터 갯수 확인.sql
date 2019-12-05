SELECT a.cd
     , a.nm AS nm
     , a.ticker AS ticker
     , a.field AS field
	  , c.element_cd
	  , c.element_nm AS group_nm
	  , MIN(b.date) AS MIN_date
	  , MAX(b.date) AS MAX_date
	  , CASE WHEN c.element_cd = '10005' 
	         THEN FORMAT(DATEDIFF(MAX(b.date), MIN(b.date)) / 7 * 5, 0)
	         WHEN c.element_cd = '10004'
	         THEN FORMAT(DATEDIFF(MAX(b.date), MIN(b.date)) / 7, 0)
	         WHEN c.element_cd = '10003' 
	         THEN FORMAT(DATEDIFF(MAX(b.date), MIN(b.date)) / 30, 0)
	         WHEN c.element_cd = '10002' 
	         THEN FORMAT(DATEDIFF(MAX(b.date), MIN(b.date)) / 90, 0)
	         WHEN c.element_cd = '10001' 
	         THEN FORMAT(DATEDIFF(MAX(b.date), MIN(b.date)) / 365, 0)
	         WHEN c.element_cd = '10006' 
	         THEN FORMAT(DATEDIFF(MAX(b.date), MIN(b.date)) / 10, 0)
	         ELSE FORMAT(DATEDIFF(MAX(b.date), MIN(b.date)), 0)
	    END AS calc_num
	  , COUNT(b.value) AS count_num
	  , a.use_yn AS 사용
 FROM item a, ivalues b, code_element c
WHERE a.use_yn = 2
  AND a.cd = b.item_cd
  AND c.group_cd = '10003'
  AND a.period_unit = c.element_cd
GROUP BY a.cd, a.nm, c.element_cd, c.element_nm, a.use_yn
ORDER BY COUNT(b.value) DESC