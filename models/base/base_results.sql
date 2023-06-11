
{{ config(materialized='table') }}


with source_results as (

    SELECT
	    CASE WHEN fastestLap = '\\N' THEN NULL
		ELSE fastestLap END
		as fastest_lap, 
		resultId as result_id,
		raceId as race_id,
		driverId as driver_id,
		constructorId as constructor_id
	from {{ source('bigquery','results')}}
)

select *
from source_results
