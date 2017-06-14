use flights;
select sda.iata_code, sda.flights_delayed15, 
a.airport,a.city,a.state,a.country,a.latitude,a.longitude 

  from spark_delayedairports sda
inner join airports a where sda.iata_code = a.IATA_code
ORDER BY CAST(flights_delayed15 as SIGNED INTEGER) DESC;
