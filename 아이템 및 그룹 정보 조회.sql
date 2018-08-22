select c.element_nm, a.nm
from item a, value b, code_element c
where b.date = '2018-07-31'
and b.item_cd = a.cd
and c.group_cd = 10006
and c.element_cd = a.`group`




