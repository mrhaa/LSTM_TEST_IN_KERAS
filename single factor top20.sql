with tmp2 as (
with tmp as (
select a.start_dt as start_dt
              , a.end_dt as end_dt
				  , a.target_cd as target_cd
				  , a.factor_cd as factor_cd
				  , b.nm as target_nm
				  , c.nm as factor_nm
				  , a.factor_profit
				  , a.index_profit
				  , a.signal_cd as signal_cd
  from result_factor a
     , item b
     , item c

where a.start_dt = '2018-08-31'
  and a.end_dt = '2020-08-31'
  and a.target_cd = 6
  and a.target_cd = b.cd
  and a.factor_cd = c.cd
order by factor_profit desc 
limit 100
)
select * 
	  , (CASE @cd WHEN a.factor_cd THEN @rownum:=@rownum+1 ELSE @rownum:=0 END) as num
	  , (@cd:=a.factor_cd)
  from tmp a
     , (SELECT @cd:='', @rownum:=0 FROM DUAL) d
order by factor_cd desc
)
select * from tmp2
where num = 0
order by factor_profit desc
limit 20