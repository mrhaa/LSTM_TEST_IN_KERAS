SELECT a.event_cd, a.nm_us, a.link
     , IFNULL(a.all_cnt,0) AS all_cnt
     , IFNULL(b.old_cnt,0) AS old_cnt
	  , IFNULL(c.new_cnt,0) AS new_cnt
	  , IFNULL(d.update_cnt,0) AS up_cnt
	  , IFNULL(all_cnt,0) - IFNULL(old_cnt,0) - IFNULL(new_cnt,0) - IFNULL(update_cnt,0) AS diff
FROM 
(SELECT a.event_cd, b.nm_us, b.link, count(*) AS all_cnt
   FROM economic_events_schedule a, economic_events b
  WHERE a.event_cd = b.cd
  GROUP BY a.event_cd, b.nm_us, b.link ) a
LEFT OUTER JOIN (SELECT event_cd, count(*) AS old_cnt
					     FROM economic_events_schedule
						 WHERE create_time IS NULL
						   AND update_time IS NULL
					   GROUP BY event_cd) b
ON a.event_cd = b.event_cd
LEFT OUTER JOIN (SELECT event_cd, count(*) AS new_cnt
					     FROM economic_events_schedule
					    WHERE create_time IS NOT NULL
					      AND update_time IS NOT NULL
					    GROUP BY event_cd) c
ON a.event_cd = c.event_cd
LEFT OUTER JOIN (SELECT event_cd, count(*) AS update_cnt
					     FROM economic_events_schedule
					    WHERE create_time IS NULL
					      AND update_time IS NOT NULL
					    GROUP BY event_cd) d
ON a.event_cd = d.event_cd

