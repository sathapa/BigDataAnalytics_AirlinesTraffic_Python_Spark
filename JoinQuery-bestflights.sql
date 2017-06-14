use flights;
select sbf.month,sbf.day,sbf.unique_carrier,sbf.delayed15
from spark_besttimeflights sbf 
inner join carriers c where c.code=sbf.unique_carrier
ORDER BY CAST(delayed15 as SIGNED INTEGER) DESC;
