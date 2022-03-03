-- First contact with our data
select * from data


-- Check for null values
select * from data
where metricname is NULL 


-- Returns records with Version1 and Version2 as a metricname
SELECT * FROM data
where metricname in ('Version1', 'Version2') 
ORDER by deviceid, metricname


-- Returns deviceids that appear 2 times, but we know that they have different metricnames. So if we also group by metricname, it don't returns anything 
/* SELECT deviceid, count(*) FROM data
where metricname in ('Version1', 'Version2') 
GROUP by deviceid
HAVING count(*)>1 */


-- Returns non duplicated pairs with first time appeared each metricname
SELECT deviceid, MIN(datetime) as firstTime, metricname, metricvalue FROM data
where metricname in ('Version1', 'Version2') 
GROUP by deviceid, metricname, metricvalue


-- Pivot metricnames
select * from (
SELECT deviceid, MIN(datetime) as firstTime, metricname, metricvalue FROM data
where metricname in ('Version1', 'Version2') 
GROUP by deviceid, metricname, metricvalue
) as FocusedTable
pivot(MIN(metricvalue) for  metricname in ([Version1], [Version2])) as PivotedTable 
ORDER by deviceid, firstTime


-- Find a way to fix Null values. Forward filling in Version1
SELECT deviceid, firstTime, Version1, Version2,
count(Version1) OVER (PARTITION BY deviceid ORDER by firstTime) as Version1Grouper,
count(Version2) OVER (PARTITION BY deviceid ORDER by firstTime) as Version2Grouper
from 
(
  select * from 
  (
    SELECT deviceid, MIN(datetime) as firstTime, metricname, metricvalue FROM data
    where metricname in ('Version1', 'Version2') 
    GROUP by deviceid, metricname, metricvalue
  ) as FocusedTable
  pivot(MIN(metricvalue) for  metricname in ([Version1], [Version2])) as PivotedTable 
) as InnerTable


--Temp table with CTE
with FinalTable AS
(
  SELECT deviceid, firstTime, Version1, Version2,
  count(Version1) OVER (PARTITION BY deviceid ORDER by firstTime) as Version1Grouper,
  count(Version2) OVER (PARTITION BY deviceid ORDER by firstTime) as Version2Grouper
  from 
  (
    select * from 
    (
      SELECT deviceid, MIN(datetime) as firstTime, metricname, metricvalue FROM data
      where metricname in ('Version1', 'Version2') 
      GROUP by deviceid, metricname, metricvalue
    ) as FocusedTable
    pivot(MIN(metricvalue) for  metricname in ([Version1], [Version2])) as PivotedTable 
  ) as InnerTable
)
SELECT 
deviceid, firstTime, 
--Version1, Version2, Version1Grouper, Version2Grouper,
MAX(Version1) OVER (PARTITION BY deviceid, Version1Grouper ORDER by firstTime) as NewVersion1,
MAX(Version2) OVER (PARTITION BY deviceid, Version2Grouper ORDER by firstTime) as NewVersion2
from FinalTable


