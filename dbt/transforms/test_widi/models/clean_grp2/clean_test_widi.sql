{{ config(materialized="table", schema="clean_grp2") }}

-- Source columns are already Nullable with correct types.
-- Keep them as-is to preserve nullability and avoid insert errors.

select *
from {{ source('raw_grp2', 'raw___test_widi') }}
