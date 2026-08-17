-- Dashboard "Sức khoẻ hội thoại theo khách hàng" của đội CSKH.
-- Người dùng chọn MỘT khách hàng và MỘT ngày, rồi bấm Load.
--
-- Ba tháng trước truy vấn này chạy 2 giây. Bây giờ 38 giây.
-- Không ai sửa dòng nào trong file này.
--
-- Bạn ĐƯỢC PHÉP viết lại truy vấn, miễn là kết quả trả về không đổi
-- (tools/explain.py kiểm tra điều đó bằng hash của kết quả).

select
    customer_name,
    count(*)                                        as n_events,
    count(distinct ticket_id)                       as n_tickets,
    round(avg(latency_ms), 1)                       as avg_latency_ms,
    quantile_cont(latency_ms, 0.95)::int            as p95_latency_ms,
    sum(case when is_escalated then 1 else 0 end)   as n_escalated,
    sum(tokens_in + tokens_out)                     as tokens_total
-- Dataset mới do tools/compact.py sinh ra: partition theo event_date, trong
-- mỗi file hàng được sắp theo customer_name.
--   * hive_partitioning = true  -> engine đọc event_date TỪ ĐƯỜNG DẪN và bỏ
--     qua 13/14 thư mục mà không mở file nào trong đó.
--   * điều kiện lọc được viết lại cho sargable: cột đứng một mình một vế,
--     không bị bọc trong strftime(), nên so được với tên thư mục partition và
--     với min/max của từng row group.
from read_parquet('data/gold_events_v2/**/*.parquet', hive_partitioning = true)
where customer_name = 'ACME'
  and event_date = DATE '2026-08-09'
group by 1
