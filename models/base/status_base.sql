
{{ config(materialized='table') }}


with source_results as (

    SELECT
        cast(statusId as int) as status_id,
        status.status
		
	from {{ source('bigquery','status')}}
)

select *
from source_results
