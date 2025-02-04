select table_name, count(column_name) as column_count from information_schema.columns
group by table_name
order by column_count desc;
------------------------------
-- Count the total number of columns from tables in database
select table_name, count(column_name) as column_count
from information_schema.columns
where table_schema='public'
group by table_name;

----------------------------

--To list the rows count in each table.	
DO $$
DECLARE
	rec RECORD;
	tbl_name TEXT;
	row_count INT;
BEGIN
	FOR rec IN (SELECT table_name FROM information_schema.tables WHERE table_schema='public') LOOP
    	tbl_name := rec.table_name;
    	EXECUTE 'SELECT COUNT(*) FROM ' || quote_ident(tbl_name) INTO row_count;
    	RAISE NOTICE '%: %', tbl_name, row_count;
	END LOOP;
END $$;


select * from baseline
select count(patient_id) as patient_count, infectionsite from baseline 
group by infectionsite
order by patient_count desc;


select infectionsite, round(avg(age)) as avg_age, count(distinct sex) as gender_count from baseline
group by infectionsite;

select * from baseline
--Trends over time
select extract(month from icu_discharge_time) as month, infectionsite, count(patient_id) as patient_count 
from baseline
group by infectionsite, month
order by month, patient_count desc;



-- outcome

select * from icd

select count(distinct b.patient_id) as patient_count, status_discharge, infectionsite from baseline b join icd
on b.patient_id = icd.patient_id 
group by infectionsite, status_discharge
order by patient_count desc;

--Analayzing patients outcomes by infectionsite : Pivoted view of Discharge status
create extension if not exists tablefunc;

select * from crosstab (
$$
	select infectionsite, 
	       coalesce(status_discharge, 'Unknown') as status_discharge,
	       count(distinct b.patient_id) as patient_count            
	from baseline b join icd
    on b.patient_id = icd.patient_id 
    group by infectionsite, status_discharge
    order by infectionsite, status_discharge
$$,
$$ select distinct coalesce(status_discharge, 'Unknown') as status_discharge
   from icd 
   order by status_discharge 
$$
) as ct(infectionsite text, 
		"Dead" int,
	    "Cured" int,
		"Recovered" int,
		"Others" int,
		"Uncovered" int,
		"Blanks" int 
	   );

-- To calculate total number of patients per department and returns the result as table.
create or replace function pats_count_dept()
returns table (admit_dept text, patients_count bigint)
language plpgsql
as $$
begin
   return query
   
   select baseline.admitdept, count(baseline.patient_id) as patients_count
   from baseline
   group by baseline.admitdept
   order by baseline.admitdept, count(baseline.patient_id);
   
 end;
 $$;

select * from pats_count_dept();
--------------------------------------------
-- Display the 3 oldest patients admitted in each department
with agerank as(
        select patient_id,
           admitdept,
           age,
           row_number() over (partition by admitdept order by age desc) as rank
from baseline)
select patient_id,
           admitdept,
                age
from agerank
where rank < 4;



--------
--For each department, find the percentage of alive patients whose general health was poor after discharge.
select discharge_dept,
    round((count(case when sf36_generalhealth like '%Poor' then 1 end) * 100.0 / count(*)),1) 
        as poor_hlth_pct
from outcome
where follow_vital = 'Alive'  
group by discharge_dept;


--Find instances where a patient was transferred into the same department twice within a day.
select t1.inp_no, 
    t1.transferdept as dept1, 
    t1.starttime as time1, 
    t2.transferdept as dept2, 
    t2.starttime as time2
from transfer t1
join transfer t2 
on t1.inp_no = t2.inp_no 
and t1.starttime::date = t2.starttime::date 
and t1.starttime < t2.starttime
and t1.transferdept = t2.transferdept
order by 1,3;


--To Show the 9th youngest patient and if they are alive or not.
with young9 as
(select patient_id as y9patient
from baseline
order by age
offset 8 rows
fetch first 1 row only
)

select o.patient_id, follow_vital
from outcome o, young9 
where o.patient_id = y9patient;


--Write a function that calculates the percentage of people who had moderate body pain after 4 weeks.
create or replace function mod_pain_pct()
returns numeric as $$
declare
    tot_pat numeric;
    mod_pain numeric;
begin
    select count(*) into tot_pat from outcome;
    select count(*) into mod_pain
    from outcome
    where sf36_pain_bodypainpast4wk like '%Moderate';
    return (mod_pain / tot_pat);
end;
$$ language plpgsql;
select mod_pain_pct();


--Create a view on baseline table with a check option on admit department .
create view baseline_view1 as
select *
from baseline
where admitdept in ('ICU', 'Medical Specialties','Surgery')
with check option;

insert into baseline_view1(patient_id, inp_no, age, sex, admitdept, infectionsite,icu_discharge_time) 
values (100,99,70,'Female','Oncology','pancreas',current_timestamp);





create view baseline_view1 as
select *
from baseline
where admitdept in ('ICU', 'Medical Specialties','Surgery')
with check option;

insert into baseline_view1(patient_id, inp_no, age, sex, admitdept, infectionsite,icu_discharge_time) 
values (100,99,70,'Female','ICU','pancreas',current_timestamp);





Find the average, minimum, and maximum systolic blood pressure for patients in each department

select b.admitdept, avg(invasive_sbp) as avg_sbp, 
min(invasive_sbp) as min_sbp,
max(invasive_sbp) as max_sbp 
from baseline b
join nursingchart nc 
on b.inp_no = nc.inp_no
group by b.admitdept;
--Alternate query
select b.admitdept,
       avg(n.blood_pressure_high) as avgsbp,
       min(n.blood_pressure_high) as minsbp,
       max(n.blood_pressure_high) as maxsbp
from nursingchart n, baseline b 
where n.inp_no = b.inp_no
group by b.admitdept;

------------------------------------------------------------------
--3. List all patients who had a systolic blood pressure higher than the median value in the ICU. Use windows functions to achieve this. 
with Median_SBP as(
	select percentile_cont(0.5) within group (order by invasive_sbp) as median_sbp
	from nursingchar nc 
	join baseline b 
	on nc.inp_no = b.inp_no
	where b.admitdept ='ICU' 
	)
	
select b.inp_no, b.admitdept, nc.invasive_sbp 
from baseline b join nursingchart nc on
b.inp_no = nc.inp_no
where b.admitdept= 'ICU'
)


--window functionsenables users to compare one row with another row without concept of using joins.
-- we can use all aggregate fucntions such as sum, max, count, avg, min .. inside window
--

select * from baseline

select * from nursingchart
--3. List all patients who had a systolic blood pressure higher than the median value in the ICU. Use windows functions to achieve this.


select nc.inp_no, nc.invasive_sbp

from (select b.inp_no,
    b.patient_id, 
    b.admitdept 
from baseline b
where b.admitdept ='ICU'
group by b.inp_no, b.patient_id, b.admitdept) A

join nursingchart nc on A.inp_no = nc.inp_no


select inp_no,
  percentile_cont(0.5) within group (order by invasive_sbp) over() Median_SBP
from
(select inp_no, invasive_sbp 
from nursingchart
group by inp_no, invasive_sbp ) A

------------------------------------
SELECT 
    inp_no, invasive_sbp,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY invasive_sbp) AS Median_SBP
FROM nursingchart
GROUP BY inp_no, invasive_sbp;
------------------------------
--3. List all patients who had a systolic blood pressure higher than the median value in the ICU. Use windows functions to achieve this.
 


select b.inp_no, b.admitdept, A.Median_SBP
from baseline b
join
  ( select 
    inp_no, 
    percentile_cont(0.5) within group (order by invasive_sbp) as Median_SBP
from nursingchart
group by inp_no) A

on
b.inp_no = A.inp_no
where b.admitdept = 'ICU';


------------------------------------------
--- using distinct function

with median_value as (
select inp_no,
	percentile_cont(0.5) within group (order by invasive_sbp) as Median_SBP
from nursingchart
group by inp_no
),

high_sbp as (
select nc.inp_no, nc.invasive_sbp, mv.Median_SBP
    from nursingchart nc 
    join median_value mv on
	nc.inp_no = mv.inp_no
	where nc.invasive_sbp > mv.Median_SBP
)

select  distinct b.inp_no, b.admitdept, h.invasive_sbp, h.Median_SBP
from high_sbp h
join baseline b on
b.inp_no = h.inp_no
where b.admitdept = 'ICU';


-----------------------------------------
-- by using group bu clause


with median_value as (
select inp_no,
	percentile_cont(.5) within group (order by invasive_sbp) as Median_SBP
from nursingchart
group by inp_no
),

high_sbp as (
select nc.inp_no, nc.invasive_sbp, mv.Median_SBP
    from nursingchart nc 
    join median_value mv on
	nc.inp_no = mv.inp_no
	where nc.invasive_sbp > mv.Median_SBP
)

select b.inp_no, b.admitdept, h.invasive_sbp, h.Median_SBP
from high_sbp h
join baseline b on
b.inp_no = h.inp_no
where b.admitdept = 'ICU'
group by b.inp_no,b.admitdept, h.invasive_sbp, h.Median_SBP;

----------------------------------------------------------------------------------
-- 4. Give the highest temperature, and highest heart rate recorded of all the patients in surgery for each day

select * from transfer
select * from nursingchart
select * from baseline


select nc.inp_no, b.admitdept, date(nc.charttime) as chart_date,
       max(temperature) as highest_temp,
	   max(heart_rate) as highest_heart_rate
from nursingchart nc join baseline b on
nc.inp_no = b.inp_no
where b.admitdept='Surgery'
group by nc.inp_no,b.admitdept, nc.charttime;

-------------------------------------------------

select date(n.charttime) as chart_dt,
    max(n.temperature) AS max_temp,
    max(n.heart_rate) AS max_hr
from nursingchart n, baseline b
where n.inp_no = b.inp_no
and b.admitdept = 'Surgery'
group by 1
order by 1;

---------------------------------------------------------------

select t.inp_no, t.transferdept, date(nc.charttime) as chart_date,
       max(temperature) as highest_temp,
	   max(heart_rate) as highest_heart_rate
from nursingchart nc join transfer t on
nc.inp_no = t.inp_no
where t.transferdept ='Surgery'
group by t.inp_no, t.transferdept, nc.charttime;

----------------------------------------------------
select nc.inp_no, b.admitdept, date(nc.charttime) as chart_date,
       max(temperature) as highest_temp,
           max(heart_rate) as highest_heart_rate
from nursingchart nc join baseline b on
nc.inp_no = b.inp_no
where b.admitdept='Surgery'
group by nc.inp_no,b.admitdept, nc.charttime;


--5.Using a recursive query, show a list of patients that were transferred to various departments after they were admitted, 
--along with the departments and the time of transfer. Hint, use earliest start time as time of admission.

select  t.inp_no, b.admitdept, t.transferdept, 
        date(t.starttime) as date_starttime, 
		starttime::timestamp::time
		
from baseline b join transfer t on
b.inp_no = t.inp_no
order by t.inp_no, t.starttime;

-------------------------------------------------

select  t.inp_no, b.admitdept, t.transferdept,-- t.starttime as time_transfer,
        --date(t.starttime) as date_starttime, 
		min(t.starttime)::time as earliest_time_transfer		
from baseline b join transfer t on
b.inp_no = t.inp_no
group by t.inp_no, b.admitdept, t.transferdept--, t.starttime
order by t.inp_no, earliest_time_transfer ;




with recursive  tr_history as (
   select inp_no, patient_id, transferdept, starttime, stoptime, 1 as seq
    from transfer
    where starttime =  (select min(starttime)
        from transfer t
        where t.patient_id = transfer.patient_id)
union all
    select t.inp_no, t.patient_id, t.transferdept, t.starttime, t.stoptime, th.seq + 1
    from transfer t
    inner join tr_history th on t.patient_id = th.patient_id
        and t.starttime = (select min(starttime)
            from transfer as t1
            where t1.patient_id = t.patient_id
            and t1.starttime > th.starttime)
)
select patient_id, transferdept, starttime as start, stoptime as stop, seq as sequence
from tr_history
order by patient_id, seq




--Write a stored procedure to calculate the total number of patients per department and return the results as a table.

select * from public.outcome

select distinct follow_vital from outcome
order by follow_vital

select distinct admitdept, count(patient_id)
from baseline
group by admitdept
order by admitdept, count(patient_id)


CREATE OR REPLACE PROCEDURE greetings()
language plpgsql
AS $$
BEGIN 
   raise notice 'Hello world'; 
END; 
$$;

call greetings();



--------  stored procedure with return table
create or replace function patcount_per_dept()
returns table (admitdept text, patient_count bigint)
language plpgsql
as $$
begin
   return query
   select admitdept, count(patient_id) 
   from baseline
   group by admitdept
   order by admitdept, count(patient_id) 
 end;
 $$;
   
select * from patscount_per_dept()   
   
  create function pat_per_dept()
returns table (
    department_name text,
    patient_count bigint
) as $$
begin
    return query
    select transferdept, count(distinct patient_id)
    from transfer
    group by 1
    order by 2;
end;
$$ language plpgsql;

select * from pat_per_dept() 
   
-----------------------
create function pat_per_dept()
returns table (
    department_name text,
    patient_count bigint
) as $$
begin
    return query
    select transferdept, count(distinct patient_id)
    from transfer
    group by 1
    order by 2;
end;
$$ language plpgsql;

select * from pat_per_dept()

----------------------------------
   
   
create or replace function pats_count_dept()
returns table (admit_dept text, patients_count bigint)
language plpgsql
as $$
begin
   return query
   
   select baseline.admitdept, count(baseline.patient_id) as patients_count
   from baseline
   group by baseline.admitdept
   order by baseline.admitdept, count(baseline.patient_id);
   
 end;
 $$;

select * from pats_count_dept();


----------------------------------------------------------------

--78 List patients who had milk and soft food but produced no urine.

select inp_no, milk, soft_food
from nursingchart
where 
milk is not null and
soft_food is not null and
urine_volume is null;

------------------------------------------
-- 77. Show patients whose critical-care pain observation tool score is 0

--COPT is Critical-care Pain Observation Tool
-- score =0 indicates nuteral, no pain, relaxed.

select distinct inp_no, cpot_pain_score
from nursingchart
where cpot_pain_score = '0';

-----------------------------------------------------
-- 76. Identify patients whose breathing tube has been removed.
select distinct inp_no, extubation 
from nursingchart
where extubation ='t'
order by inp_no;

---------------------------------------------------------
-- 75. Find the average heart rate of patients under 40.

select round(avg(nc.heart_rate),2) as avg_heartrate
from nursingchart nc join baseline b on 
b.inp_no = nc.inp_no
where b.age<40;

select * from baseline


select avg(heart_rate) as avg_heartratee
from nursingchart n, baseline b
where n.inp_no = b.inp_no
and age < 40


SELECT ROUND(AVG(nc.heart_rate), 2) AS avg_heartrate
FROM nursingchart nc
JOIN baseline b ON b.inp_no = nc.inp_no
WHERE b.age < 40;


select avg(nc.heart_rate) as avg_heartrate
from nursingchart nc join baseline b on 
b.inp_no = nc.inp_no
where b.age<40;


--alter table nursingchart
--alter column heart_rate
--set data type numeric 

--other approach

--SELECT ROUND(AVG(nc.heart_rate)::numeric, 2) AS avg_heartrate
--FROM nursingchart nc
--JOIN baseline b ON b.inp_no = nc.inp_no
--WHERE b.age < 40;

------------------------------------------------------

--74. List the last 100 patients that were discharged.

select patient_id, icu_discharge_time as discharged
from baseline
order by icu_discharge_time desc
limit 100;


------------------------------------------------------------------

select patient_id, icu_discharge_time
from baseline
order by 2 desc
limit 100

-----------------------------------------------


-- 73. Find the patients who are happy all the time.

select patient_id, sf36_emotional_happyperson from outcome
where sf36_emotional_happyperson = '1_All of the time'


--select patient_id  from outcome
--where sf36_emotional_happyperson like '%All%'

------------------

-- 72. List all transfers that started due to a change in disease.
select patient_id, startreason, transferdept as transferred
from transfer
where startreason = 'Disease change';


----------------------------

-- 71. Use regular expression to find disease names that end in 'itis'

select distinct icd_desc from icd
where icd_desc ~ 'itis$';

select icd_desc from icd
where icd_desc ~ 'itis$';


--------------------------------------------------------------

-- 5. Using a recursive query, show a list of patients that were transferred to various departments after they were admitted, 
--along with the departments and the time of transfer.
--Hint, use earliest start time as time of admission.

--code explanation 
with recursive CTE_name as 
   (
     select query (base query or non_recursive query)
	    union all
	 select query  (recursive query using CTE_name [with termination condition])
   
   )

select * from CTE_name;
------------------------------------
--Display numbers from 1 to 10 without using inbuit functions
--eg:
with recursive numbers as 
    (
      select 1 as n 
		union
	  select n + 1 
		from numbers
		where n < 10
     )

select * from numbers

--------------------
select * from baseline
select * from transfer

select distinct b.inp_no,  b.Patient_id, b.admitdept, t.transferdept, t.starttime from transfer t
join baseline b on 
b.inp_no = t.inp_no
-------------------------------------------
with recursive pats_transferred as
 (
    
   select distinct b.inp_no,  b.Patient_id, b.admitdept, t.transferdept, t.starttime  from transfer t
   join baseline b on 
   b.inp_no = t.inp_no
	 
   union
	 
   select distinct pt.inp_no,  pt.Patient_id, pt.admitdept, t.transferdept, t.starttime 
   from transfer t 
   join pats_transferred pt on 
   t.inp_no = pt.inp_no 
   and  t.starttime > pt.starttime
 
 )

select Patient_id,admitdept, transferdept, starttime as time_transfer from pats_transferred
order by time_transfer

-----------------------------------------------------------------
-- Examples Functions in pgsql

create or replace function fn_example(varchar,integer,integer)
returns varchar
as
$$
begin

  return substring($1, $2, $3);

end;
$$
language plpgsql;

select fn_example('software',1,4)
-----------------------

--using Alias 

create or replace function fn_example1(varchar,integer,integer)
returns varchar as
$$
declare word1 alias for $1;
        word2 alias for $2;
		word3 alias for $3;

begin
    return substring(word1,word2,word3);

end;
$$
language plpgsql;

select * from fn_example1('software',2,2);

----------------------------------------------------------------

--Q35. Write a function that takes a date and returns the average temperature recorded for that day.





select avg(temperature) , charttime 
from nursingchart
group by avg(temperature), charttime

