#Last Not Null Value
create table brands 
(
category varchar(20),
brand_name varchar(20)
);
insert into brands values
('chocolates','5-star')
,(null,'dairy milk')
,(null,'perk')
,(null,'eclair')
,('Biscuits','britannia')
,(null,'good day')
,(null,'boost');

with cte as(
Select *
,count(category)over(order by (Select null) rows between unbounded preceding and 0 following ) as rn 
from brands
)
Select first_value(category)over(partition by rn order by rn ) as category, brand_name from cte
