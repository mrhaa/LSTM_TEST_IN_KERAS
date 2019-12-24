select ai.date, ai.value, bi.value, ci.value, di.value, ei.value, fi.value, gi.value, hi.value, ii.value
from item a, item b, item c, item d, item e, item f, item g, item h, item i
   , ivalues ai, ivalues bi, ivalues ci, ivalues di, ivalues ei, ivalues fi, ivalues gi, ivalues hi, ivalues ii
where a.nm = 'S&P500'
and b.nm = '중국 OECD 경기선행지수'
and c.nm = 'ISM 제조업지수(계조)'
and d.nm = '달러 리보 3M'
and e.nm = '러시아 원유 수출(USD백만) YTD'
and f.nm = 'RTSI P/E'
and g.nm = 'Nikkei225 EPS'
and h.nm = 'TOPIX EPS'
and i.nm = '한국 평균 가동률 YoY'
and a.cd = ai.item_cd
and b.cd = bi.item_cd
and c.cd = ci.item_cd
and d.cd = di.item_cd
and e.cd = ei.item_cd
and f.cd = fi.item_cd
and g.cd = gi.item_cd
and h.cd = hi.item_cd
and i.cd = ii.item_cd
and ai.date = bi.date
and ai.date = ci.date
and ai.date = di.date
and ai.date = ei.date
and ai.date = fi.date
and ai.date = gi.date
and ai.date = hi.date
and ai.date = ii.date
