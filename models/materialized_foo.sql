{{ config(
    materialized="materialized_view",
    on_schema_change="drop",
    backup=false
) }}

select
    *
from {{ source('baz', 'source_baz') }}