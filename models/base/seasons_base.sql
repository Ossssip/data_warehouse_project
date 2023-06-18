
{{ config(materialized='table') }}


with source_results as (

    SELECT
	    CAST(year as int) as year,
		url
	from {{ source('bigquery','seasons')}}
)

select *
from source_results
