{{ config(materialized="table", schema="mart_grp2") }}

select *
from {{ ref('clean_test_widi') }}
