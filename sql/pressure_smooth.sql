Barometric pressure has negative spikes, frequently -3 but sometimes -10 or more.

-- last 20 values
select 
    paii_time, pressure
from paii_raw 
order by
    paii_time
limit 20
;

-- last 20 values
select 
    paii_time, 
    pressure,
    avg(pressure) over(order by paii_time rows between 5 preceding and current row) as smooth_pressure
from paii_raw 
order by
    paii_time
limit 20
;
