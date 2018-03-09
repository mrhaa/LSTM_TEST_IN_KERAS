SELECT a.item_cd AS 아이템코드, b.nm AS 아이템명, count(*) AS 개수, min(date) AS 시작일, max(date) AS 마지막일
  FROM value as a
     , item as b
 WHERE a.item_cd = b.cd
GROUP BY a.item_cd, b.nm
