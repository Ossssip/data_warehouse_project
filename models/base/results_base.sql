
{{ config(materialized='table') }}


with source_results as (

    SELECT
		cast(resultId as int) as result_id,
		cast(raceId as int) as race_id,
		cast(driverId as int) as driver_id,
		cast(constructorId as int) as constructor_id,

		CASE WHEN number = '\\N' THEN NULL
		ELSE cast(number as int)  END as number,
		 
		CASE WHEN grid = '\\N' THEN NULL
		ELSE cast (grid as int) END as grid,

		CASE WHEN position = '\\N' THEN NULL
		ELSE cast (position as int) END as position,

		positionText as position_text,

		CASE WHEN positionOrder = '\\N' THEN NULL
		ELSE cast(positionOrder as int)  END as position_order,

		CASE WHEN points = '\\N' THEN NULL
		ELSE cast(points as float64) END as points,

		CASE WHEN laps = '\\N' THEN NULL
		ELSE cast(laps as int) END as laps,


		CASE WHEN milliseconds = '\\N' THEN NULL
		ELSE cast (milliseconds as int) END as time_ms,

		CASE WHEN fastestLap = '\\N' THEN NULL
		ELSE cast(fastestLap as int) END as fastest_lap,
		CASE WHEN rank = '\\N' THEN NULL
		ELSE cast (rank as int) END as fastest_lap_rank,

		CASE WHEN fastestLapTime = '\\N' THEN NULL
		ELSE parse_time('%M:%E*S', fastestLapTime) END as fastest_lap_time,
		CASE WHEN fastestLapSpeed = '\\N' THEN NULL
		ELSE cast (fastestLapSpeed as float64) END as fastest_lap_speed,

		CASE WHEN statusId = '\\N' THEN NULL
		ELSE cast(statusId as int) END as status_id
		
	from {{ source('bigquery','results')}}
)

select *
from source_results
