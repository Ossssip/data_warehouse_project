
{{ config(materialized='table') }}


with source_results as (

    SELECT
	    CAST(driverId as int) as driver_id,
		driverRef as driver_ref,
        CASE WHEN number = '\\N' THEN NULL
        ELSE cast(number as int) END as number,
		code,
		forename,
        surname,
        DATE(dob) as date_of_birth,
        nationality,
		url
	from {{ source('bigquery','drivers')}}
)

select *
from source_results
