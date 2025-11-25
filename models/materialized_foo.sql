{{ config(
    materialized="materialized_view"
) }}

select
    *
from {{ source('baz', 'source_baz') }}