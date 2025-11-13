{{ config(
    materialized="materialized_view",
    on_schema_change="fail",
    backup=false
) }}

select
    *
from {{ source('baz', 'source_baz') }}