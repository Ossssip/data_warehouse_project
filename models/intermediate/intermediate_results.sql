
{{ config(materialized='table') }}


with int_results as (

    SELECT
	    *
	from {{ ref('base_results')}}
)

select *
from int_results
