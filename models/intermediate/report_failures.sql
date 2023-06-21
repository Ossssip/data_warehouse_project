
{{ config(materialized='table') }}


with accidents as(
  SELECT year, constructors.name as team, races.name as race, sts.status as status
FROM {{ref('results_base')}} as results
left join {{ref('status_base')}} as sts
using(status_id)
left join {{ref('constructors_base')}} as constructors
using(constructor_id)
left join {{ref('races_base')}} as races
using(race_id)
order by year, team, sts.status
), races_per_year as(
  SELEct year, constructors.name as team, count(year) as entrants_per_year
  from {{ref('results_base')}}
  left join {{ref('races_base')}} as races
  using(race_id)
  left join {{ref('constructors_base')}} as constructors
  using(constructor_id)
  group by year, constructors.name
) 

select a.year, race, team, status, 
CASE when status not like '+%'
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
) then 1 else 0 END as is_technical_failure

from accidents as a 
left join races_per_year as r
using(year, team)
where status not like '+%'

order by year
