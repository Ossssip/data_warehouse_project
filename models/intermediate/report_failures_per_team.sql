
{{ config(materialized='table') }}


with accidents as(
  SELECT year, constructors.name as team, sts.status as status,  count( sts.status) as number_of_failures 
FROM {{ref('results_base')}} as results
left join {{ref('status_base')}} as sts
using(status_id)
left join {{ref('constructors_base')}} as constructors
using(constructor_id)
left join {{ref('races_base')}} as races
using(race_id)
Group by year,constructors.name, sts.status
order by year, team, sts.status
), races_per_year as(
  SELEct year, constructors.name as team, count(constructors.name) as entrants_per_year
  from {{ref('results_base')}}
  left join {{ref('races_base')}} as races
  using(race_id)
  left join {{ref('constructors_base')}} as constructors
  using(constructor_id)
  group by year, constructors.name
) 

select a.year, team, sum(number_of_failures)/sum(entrants_per_year)*100 as failure_percent

from accidents as a 
left join races_per_year as r
using(year, team)
where status not like '+%'
and status NOT IN (
  'Finished',
  'Accident',
  'Collision',
  'Disqualified',
  'Spun off',
  'Not classified',
  'Injured',
  '107% Rule',
  'Did not qualify',
  'Injury',
  'Safety concerns',
  'Safety',
  'Excluded',
  'Did not prequalify',
  'Driver unwell',
  'Fatal accident',
  'Eye injury',
  'Collision damage',
  'Damage',
  'Debris',
  'Illness'
)
group by year, team
order by year, team
