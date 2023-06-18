
{{ config(materialized='table') }}


with source_results as (

    SELECT
        CAST(raceId as int) as race_id,
        CAST(year as int) as year,
        CAST(round as int) as round,
        cast(circuitId as int) as circuit_id,
        name,
        DATE(date) as date,
        CASE WHEN time = '\\N' THEN NULL
        ELSE parse_time('%H:%M:%S', time) END as time,
        url
 		
	from {{ source('bigquery','races')}}
)

select *
from source_results
