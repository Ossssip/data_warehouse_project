
{{ config(materialized='table') }}


with source_results as (

    SELECT
	    CAST(circuitId as int) as circuit_id,
		circuitRef as circuit_ref,
        name, 
        location,
        country,
        CAST(lat as float64) as lat,
        CAST(lng as float64) as lng,
        CASE WHEN alt = '\\N' THEN NULL
        ELSE Cast(alt as int)  END as alt,
		url
	from {{ source('bigquery','circuits')}}
)

select *
from source_results
