luke missing about 5 hours of paii rows
the config.ini file had hostname=postgres not host=postgres

select max(paii_time) from paii_raw where paii_time < '2020-08-30 16:00:00+10';
          max
------------------------
 2020-08-30 15:10:00+10

select min(paii_time) from paii_raw where paii_time > '2020-08-30 16:00:00+10';
          min
------------------------
 2020-08-30 19:50:00+10


on spidey

select min(paii_time) from paii_raw where paii_time > '2020-08-30 12:00:00+10';
          min
------------------------
 2020-08-30 15:10:30+10

select max(paii_time) from paii_raw where paii_time > '2020-08-30 12:00:00+10';
          max
------------------------
 2020-08-30 19:49:30+10

select count(paii_time) from paii_raw where paii_time > '2020-08-30 12:00:00+10';
 count
-------
   555


# on spidey (as postgres supervisor)
copy
    (
        select * from paii_raw
        where
            paii_time > '2020-08-30 12:00:00+10'
            and
            paii_time < '2020-08-30 20:00:00+10'
    )
to '/tmp/spidey_paii_raw.csv' csv;

to '/tmp/spidey_paii_raw.dat' binary;

# on luke
copy paii_raw from '/tmp/spidey_paii_raw.dat' binary;
