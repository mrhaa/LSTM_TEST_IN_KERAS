select a.cd, a.nm, a.ticker, MAX(STR_TO_DATE(b.date, '%Y-%m-%d'))
from item a, value b
where a.use_yn = 1
and a.period_unit = 10003
and a.cd = b.item_cd
and a.ticker in ('BISBUSR Index','BISBEUR Index','BISBJPR Index','BISBCNR Index','BISBINR Index','BISBKRR Index','BISBBRR Index','M3% YOY Index','M2% YOY Index','M1% YOY Index','KOMSM3Y Index','KOMSM2Y Index','KOMSM1Y Index','KOCGCGS Index','SKBSICSA Index','KOIVCONY Index','SKLILY Index','KOIVMOY Index','KOEXPCHN Index','KOTPSISA Index','KOSVTOTY Index','KOIVCPSY Index','KOIVCPEY Index','KORSTY Index','KORSTY Index','KOPM01ES Index','KOTTNBTT Index','KOTPTOTY Index','KOPM01DO Index','KOIPMY Index','KOPSDS Index','KORST Index','KOTSVARR Index','OEKRA046 Index','CHHEAVG Index','CNCIBCS Index','CNCILI Index','CNCILI Index','CHRCCLIM Index','CHBNINDX Index','CNLNNFI Index','CNVSTTL Index','IISICHIN Index','OECNA035 Index','JCPNEFEY Index','OLE3JAPA Index','MSJWWAGE Index','OEJPA049 Index','EUA3EU27 Index','EUA1EU27 Index','EUA5EU27 Index','SNTEEUH6 Index','SNTEEUGX Index','GRIPIYOY Index','MPMIDEMU Index','EPUCCEUM Index','EACPI Index','BCMPCIGD Index','EUNGEZY Index','O11EA013 Index','MARGDEBT Index','MARGDEBT Index','MARGCRBL Index','LMCILMCC Index','WGTROVER Index','OEUSA044 Index','RUESEXOI Index','RUCUCRD$ Index','OERUA046 Index','IPECPRAD Index','MWT VEAD Index','OLE35ASI Index','OLE3G7 Index','MEPRGLEI Index','OTTLC005 Index','OEBRA042 Index','OEINA044 Index','MWT VWT Index','IPECPRWO Index','IPECPREM Index','MWT VEEM Index','OLEDMAJ6 Index')
group by a.cd, a.nm, a.ticker

