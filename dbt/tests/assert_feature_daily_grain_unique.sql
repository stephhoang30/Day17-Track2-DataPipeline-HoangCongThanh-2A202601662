-- Grain của gold_feature_daily là (event_date, customer_id).
-- Test này bắt đúng lỗi mà lookback window có thể gây ra: cửa sổ rộng hơn
-- khiến một cặp được tính lại nhiều lượt; nếu model chỉ biết insert thì mỗi
-- lượt tính lại là một hàng mới thay vì ghi đè.

select
    event_date,
    customer_id,
    count(*) as n
from {{ ref('gold_feature_daily') }}
group by 1, 2
having count(*) > 1
