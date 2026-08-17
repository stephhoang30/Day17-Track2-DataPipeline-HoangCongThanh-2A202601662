#!/usr/bin/env python3
"""Tái cấu trúc dataset Parquet của dashboard — NHIỆM VỤ 4.  CHƯA CÓ LOGIC.

Hiện trạng: `data/gold_events/` gồm 5.000 file, mỗi file vài chục KB, không
partition, thứ tự hàng ngẫu nhiên.

Yêu cầu: đọc toàn bộ dataset cũ, ghi ra dataset mới có layout hợp lý hơn, sau đó cập
nhật `queries/dashboard.sql` để trỏ vào dataset mới.

    python tools/compact.py       # ghi dataset mới
    python tools/explain.py       # đo lại và so với baseline

KHUNG THỰC HIỆN

    COPY (
        SELECT *
        FROM   read_parquet('data/gold_events/*.parquet')
        ORDER  BY <cột A>, <cột B>
    ) TO 'data/gold_events_v2' (
        FORMAT          parquet,
        PARTITION_BY    (<cột partition>),
        OVERWRITE_OR_IGNORE,
        ROW_GROUP_SIZE  <?>
    )

Ba quyết định, mỗi quyết định cần một lý do viết được ra giấy:

  <cột partition>   Engine chỉ bỏ qua được file mà nó biết là vô ích TRƯỚC khi
                    mở file. Thông tin đó đến từ đường dẫn. Vậy cột nào của
                    truy vấn dashboard nên xuất hiện trong tên thư mục? Cột đó
                    có bao nhiêu giá trị phân biệt — tức bao nhiêu thư mục?
                    Partition theo cột có 650 giá trị thì hệ quả là gì?

  <cột A>, <cột B>  Thứ tự hàng trong file quyết định thống kê min/max của mỗi
                    row group có ích hay vô dụng. Sắp thế nào để các hàng cùng
                    một khách hàng nằm liền nhau?

  ROW_GROUP_SIZE    Mặc định 122.880 hàng. Một ngày có khoảng bao nhiêu hàng?
                    Nếu cả ngày gói gọn trong MỘT row group thì min/max của
                    row group đó phủ những gì, và còn tác dụng lọc không?

Sau khi chạy xong, kiểm tra lại bằng `python tools/explain.py`: `rows scanned`
phải giảm, `files` phải giảm, và `result hash` phải GIỮ NGUYÊN.
"""

from __future__ import annotations

import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA  # noqa: E402

SRC = DATA / "gold_events"
DST = DATA / "gold_events_v2"


# Ba quyết định của bài mở rộng A, và lý do của từng cái:
#
#   PARTITION_BY (event_date)
#       Dashboard lọc theo hai cột: customer_name và ngày. Chỉ một trong hai
#       nên nằm trong đường dẫn — thứ engine đọc được TRƯỚC khi mở file.
#       event_date có 14 giá trị phân biệt -> 14 thư mục, mỗi thư mục một file
#       cỡ vài trăm KB. customer_name có 650 giá trị -> 650 thư mục nhỏ xíu,
#       tức là tái tạo lại đúng small-file problem đang phải chữa.
#
#   ORDER BY customer_name, event_time
#       Trong một file, hàng của cùng một khách nằm liền nhau, nên min/max của
#       mỗi row group hẹp và có tác dụng lọc. Không sắp thì mỗi row group đều
#       chứa cả 650 khách, khoảng min..max phủ toàn bộ bảng chữ cái và thống kê
#       trở thành vô dụng.
#
#   ROW_GROUP_SIZE 2048
#       Một ngày có ~9.300 hàng. Với mặc định 122.880 thì cả ngày gói trong
#       MỘT row group: min/max của nó là ('ACME' .. 'Cust_0650'), không loại
#       được gì. 2.048 chia ngày thành ~5 row group, ACME chỉ nằm trong một
#       vài group đầu.
#
#       Đo được: ở quy mô này (một partition ~9.300 hàng) hai quyết định
#       ORDER BY và ROW_GROUP_SIZE KHÔNG làm `rows scanned` giảm thêm — cùng
#       một dataset để nguyên thứ tự và để row group mặc định vẫn cho 9.324.
#       Toàn bộ mức giảm 536× đến từ partition pruning. Hai quyết định kia
#       chỉ bắt đầu có giá trị khi một partition lớn hơn nhiều lần row group;
#       giữ lại vì đó là layout đúng khi dữ liệu tăng, không phải vì chúng
#       đóng góp vào con số hôm nay.
ROW_GROUP_SIZE = 2_048


def main() -> int:
    con = duckdb.connect()

    n_src = len(list(SRC.glob("*.parquet")))
    print(f"  nguồn : {SRC}  ({n_src:,} file)")

    n_before = con.execute(
        f"select count(*) from read_parquet('{SRC}/*.parquet')"
    ).fetchone()[0]

    con.execute(f"""
        copy (
            select *
            from read_parquet('{SRC}/*.parquet')
            order by customer_name, event_time
        ) to '{DST}' (
            format          parquet,
            partition_by    (event_date),
            overwrite_or_ignore,
            row_group_size  {ROW_GROUP_SIZE}
        )
    """)

    n_after = con.execute(
        f"select count(*) from read_parquet('{DST}/**/*.parquet', hive_partitioning = true)"
    ).fetchone()[0]
    assert n_before == n_after, f"mất hàng: {n_before:,} -> {n_after:,}"

    n_dst = len(list(DST.rglob("*.parquet")))
    print(f"  đích  : {DST}  ({n_dst:,} file, {n_after:,} hàng)")
    print(f"  layout: partition_by(event_date) · order by customer_name, event_time"
          f" · row_group_size {ROW_GROUP_SIZE:,}")
    print("\n  xong. Đo lại bằng:  make explain\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
