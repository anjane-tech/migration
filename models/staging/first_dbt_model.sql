
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (
    SELECT abs(random()) as val FROM table(generator(rowCount => 1000))
)

select row_number() over(order by val) as id, val
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
