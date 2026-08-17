-- Grain của quarantine_tickets là 1 hàng / 1 BẢN GHI CDC, khoá (ticket_id,
-- cdc_seq) — không phải 1 hàng / 1 ticket. Test này bắt trường hợp điều kiện
-- lọc vô tình nhân bản bản ghi (ví dụ khi thêm join vào model).

select
    ticket_id,
    cdc_seq,
    count(*) as n
from {{ ref('quarantine_tickets') }}
group by 1, 2
having count(*) > 1
