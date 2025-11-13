{{ config(
    materialized="materialized_view",
    on_configuration_change="continue",
    backup=false
) }}

select
    *
from {{ source('jaffle_shop', 'raw_items') }}