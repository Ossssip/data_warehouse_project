
{{ config(materialized='table') }}


with source_results as (

    SELECT
	    CAST(constructorId as int) as constructor_id,
		constructorRef as constructor_ref,
        name, 
        nationality,
		url
	from {{ source('bigquery','constructors')}}
)

select *
from source_results
