
{{ config(materialized='table') }}


with int_results as (

    SELECT
	    *
	from {{ ref('results_base')}}
)

select *
from int_results
