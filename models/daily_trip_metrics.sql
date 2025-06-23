select pickup_date, avg(drive_time) as avg_drive_time, sum(drive_time) as total_drive_time ,
avg(total_amount) as avg_amount, sum(total_amount) as total_amount 
from {{ ref('tripss') }} group by pickup_date order by 1 desc
