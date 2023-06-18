
{{ config(materialized='table') }}


with source_results as (

    SELECT
	    CAST(driverStandingsId as int) as driver_standings_id,
		CAST(raceId as INT) as race_id,
		CAST(points as FLOAT64) as points,
		CAST(position as INT) as position,
		positionText as position_text,
		CAST(wins as int) as wins
	from {{ source('bigquery','driver_standings')}}
)

select *
from source_results
