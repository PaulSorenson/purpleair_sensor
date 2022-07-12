
select paii_time, pm2_5_cf_1, pm2_5_cf_1_b from paii_raw where pm2_5_cf_1 > 150;

-- has divide by zero
select paii_time, greatest(pm2_5_cf_1, pm2_5_cf_1_b) / least(pm2_5_cf_1, pm2_5_cf_1_b) from paii_raw where pm2_5_cf_1 > 3;

-- view maxes
with cmp as (
    select 
        paii_time, 
        greatest(pm2_5_cf_1, pm2_5_cf_1_b) biggest,
        least(pm2_5_cf_1, pm2_5_cf_1_b) smallest,
        200 as clip
    from paii_raw 
)
select
    *
from cmp
where biggest > cmp.clip;


-- view mismatches
with cmp as (
    select 
        paii_time, 
        greatest(pm2_5_cf_1, pm2_5_cf_1_b) biggest,
        least(pm2_5_cf_1, pm2_5_cf_1_b) smallest,
        200 as clip  -- see below
    from paii_raw 
)
select
    *
from cmp
where biggest - smallest > 20;


-- update mismatches: biggest = smallest
with cmp as (
    select 
        paii_time, 
        greatest(pm2_5_cf_1, pm2_5_cf_1_b) biggest,
        least(pm2_5_cf_1, pm2_5_cf_1_b) smallest
    from paii_raw 
)
update paii_raw
set 
    pm2_5_cf_1 = smallest,
    pm2_5_cf_1_b = smallest
from cmp
where 
    cmp.biggest - cmp.smallest > 200
    and cmp.paii_time = paii_raw.paii_time
returning *
;


-- clip
with cmp as (
    select 
        paii_time, 
        greatest(pm2_5_cf_1, pm2_5_cf_1_b) biggest,
        least(pm2_5_cf_1, pm2_5_cf_1_b) smallest,
        200 as clip
    from paii_raw 
)
update paii_raw
set 
    pm2_5_cf_1 = cmp.clip,
    pm2_5_cf_1_b = cmp.clip
from cmp
where 
    cmp.biggest > cmp.clip
    and cmp.paii_time = paii_raw.paii_time
returning *
;

