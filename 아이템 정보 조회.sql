SELECT a.cd 		  AS 아이템코드
     , a.nm			  AS 아이템명
	  , b.element_nm AS 아이템타입
	  , c.element_nm AS 수치단위
	  , d.element_nm AS 기간단위
	  , e.element_nm AS 출처
	  , a.ticker	  AS 티커
	  , g.element_nm AS 그룹
	  , f.element_nm AS 통화
	  , MAX(h.date)  AS 마지막일
	  , a.use_yn     AS 사용여부
  FROM item AS a
 LEFT JOIN code_element AS f
    ON a.currency = f.element_cd
   AND f.group_cd = 10004 
 LEFT JOIN value    AS h
    ON a.cd = h.item_cd
     , code_element AS b
     , code_element AS c
     , code_element AS d
     , code_element AS e
     , code_element AS g    
 WHERE a.value_type = b.element_cd
   AND b.group_cd = 10001
   AND a.value_unit = c.element_cd
   AND c.group_cd = 10002
   AND a.period_unit = d.element_cd
   AND d.group_cd = 10003
   AND a.source = e.element_cd
   AND e.group_cd = 10005
   AND a.group = g.element_cd
   AND g.group_cd = 10006
	/*AND a.use_yn = 1*/
	/*AND a.period_unit = 10005*/ /*일별 데이터*/
 GROUP BY a.cd, a.nm, b.element_nm, c.element_nm, d.element_nm, e.element_nm, a.ticker, g.element_nm, f.element_nm, a.use_yn


  

   