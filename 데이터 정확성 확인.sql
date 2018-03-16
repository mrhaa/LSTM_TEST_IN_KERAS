SELECT a.cd AS 아이템코드, a.nm AS 아이템명, count(*) AS 개수, min(date) AS 시작일, max(date) AS 마지막일
  FROM item as a     
	LEFT JOIN value AS b
    ON a.cd = b.item_cd
GROUP BY a.cd, a.nm
