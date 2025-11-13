{{ config(
    materialized="materialized_view",
    on_configuration_change="apply",
    backup=false
) }}

select
    *
from {{ source('baz', 'source_baz') }}