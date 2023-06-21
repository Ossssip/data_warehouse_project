
{{ config(materialized='table') }}

with times as(
Select year, circuits.name as name, fastest_lap_rank, fastest_lap_time,fastest_lap_speed,  constructors.name as team,
 TIME_DIFF(fastest_lap_time, '00:00:00', MILLISECOND) as fastest_lap_time_ms,
 ST_GEOGPOINT(lng,lat) as coordinates
FROM {{ref('results_base')}} as results
left join {{ref('races_base')}} as races
using(race_id)
left join {{ref('circuits_base')}} as circuits
using(circuit_id)
left join {{ref('constructors_base')}} as constructors
using(constructor_id)
where fastest_lap_time is not NULL
order by year, name
)


select *
from times
order by year, name