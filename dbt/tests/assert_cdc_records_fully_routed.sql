-- Mỗi bản ghi CDC phải đi đúng MỘT trong hai đường: hoặc được Silver nhận,
-- hoặc vào quarantine. Không bản ghi nào được biến mất im lặng, cũng không
-- bản ghi nào được đếm hai lần.
--
-- Đây là bất biến giữ cho quarantine và Silver không lệch nhau khi ai đó sửa
-- macro normalize_priority mà quên một trong hai model.

with cdc as (
    select count(*) as n from {{ source('bronze', 'bronze_tickets_cdc') }}
),

nhan as (
    select count(*) as n
    from {{ source('bronze', 'bronze_tickets_cdc') }}
    where {{ normalize_priority('priority_raw') }} is not null
),

loai as (
    select count(*) as n from {{ ref('quarantine_tickets') }}
)

select
    cdc.n  as n_cdc,
    nhan.n as n_hop_le,
    loai.n as n_quarantine
from cdc, nhan, loai
where cdc.n <> nhan.n + loai.n
