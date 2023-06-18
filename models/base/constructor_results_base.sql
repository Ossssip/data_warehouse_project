
{{ config(materialized='table') }}


with source_results as (

    SELECT
	    CAST(constructorResultsId as int) as constructor_result_id,
		CAST(raceId as INT) as race_id,
		CAST(constructorId as INT) as constructor_id,
		CAST(points as FLOAT64) as points,
		CASE WHEN status = '\\N' THEN NULL
		ELSE status END
		as status,
	from {{ source('bigquery','constructor_results')}}
)

select *
from source_results
