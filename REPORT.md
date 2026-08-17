# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Hoàng Công Thành  **Lớp:** AICB-P2T2  **MSSV:** 2A202601662  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details>
<summary>Output ba lượt chạy (sau khi sửa)</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 13.5s
  run 2/3 … 11.9s
  run 3/3 … 12.2s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    f8d3f591f0    f8d3f591f0    f8d3f591f0   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 21/21 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Chạy thêm lượt **thứ tư và thứ năm** (`python tools/verify.py --runs 2 --no-reset`):
checksum không đổi — `8dd7c98653` / `f8d3f591f0` / `92d8e50131` / `ebb89036fb`.

Tổng kết: **4 / 4 tiêu chí đạt** · hai bài mở rộng đều đạt.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | `gold_training_set` phình thêm sau mỗi lượt chạy lại: 13.478 → 25.958 → 38.438 hàng, trong khi `count(distinct ticket_id)` luôn đúng 12.480. Không lỗi, không cảnh báo. |
| **Nguyên nhân** | Model khai `materialized='incremental'` nhưng **không khai `unique_key`**, nên dbt sinh ra một câu `INSERT INTO … SELECT …` thuần (kiểm chứng ở `dbt/target/run/lab17/models/gold/gold_training_set.sql`). Câu lệnh đó không có khái niệm "hàng này đã tồn tại": ghi lại cùng một partition là **ghi thêm**, không phải ghi đè. Bản thân phép ghi không idempotent, nên **mọi** cơ chế retry ở tầng trên — Clear Task trong Airflow, retry của scheduler, backfill — đều biến thành cơ chế nhân bản. Mỗi lượt chạy lại cộng đúng 12.480 hàng, bằng số ticket sống. |
| | Thêm một chi tiết quyết định *chiến lược sửa*: nguồn là CDC có `op='u'`, nên một ticket tạo ngày D1 rồi sửa ngày D2 đi qua mệnh đề `WHERE _ingested_at ∈ [run_date, run_date+1)` **hai lần trong một lượt chạy**, ở hai partition ngày khác nhau. Vì vậy `delete+insert` theo partition ngày *không* gộp được hai lần ghi đó về một hàng — grain của bảng là **entity**, không phải sự kiện, nên khoá phải là `ticket_id` chứ không phải ngày. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key='ticket_id'` + `incremental_strategy='merge'`. Mệnh đề `WHERE` theo `run_date` giữ nguyên (nó phục vụ backfill, không phải lỗi).<br>`dags/ai_training_pipeline.py`: `catchup=False`, `max_active_runs=1`. |
| **Bằng chứng** | trước: `make verify` trên repo gốc cho **38.750 hàng** sau ba lượt (thừa 26.270). Đo riêng cơ chế tăng — chỉ hoàn tác `config()` của model này, giữ nguyên nhiệm vụ 3 để loại nhiễu từ 312 bản ghi hỏng — được **13.478 → 25.958 → 38.438**, mỗi lượt **+12.480** đúng bằng số ticket sống.<br>sau: **12.480 hàng ở cả năm lượt**, checksum `8dd7c98653` không đổi, 0 ticket bị lặp. |

> **Hai tham số DAG không phải root cause.** `catchup=True` khiến một lần bật lại DAG tự
> schedule bù hàng chục ngày quá khứ, và `max_active_runs` không giới hạn cho phép nhiều
> run cùng ghi vào một bảng. Chúng chỉ **tăng tần suất kích hoạt** lỗi. Sửa DAG mà không
> sửa materialization thì `make verify` vẫn đỏ; sửa materialization thì một lần chạy lại
> bất kỳ — do người, do scheduler, hay do backfill — đều vô hại.

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` có 8.645 / 9.100 hàng — thiếu 455 cặp (ngày, khách), và chỉ thiếu ở **các ngày đã chạy xong từ lâu**; ngày mới thì đủ. |
| **P99 độ trễ đo được** | **2,73 ngày** *(p50 = 0,13 · p95 = 1,81 · max = 2,94 · 5,05% bản ghi tới muộn hơn một ngày; đo trên 129.462 hàng của `bronze_events`)* |
| **Lookback đã chọn** | **3 ngày** — làm tròn lên từ P99 = 2,73. Phân bố có hai cụm rời hẳn nhau: 0–6 giờ (đường bình thường) và 43–71 giờ (đường về muộn), nên mọi lookback từ 0 đến 1 ngày đều nằm gọn trong khe trống giữa hai cụm và không cứu được hàng nào. |
| **Nguyên nhân** | Điều kiện lọc incremental là `event_date > (select max(event_date) from {{ this }})` — nó dùng **thời điểm sự kiện xảy ra** (event time) làm con trỏ tiến độ, trong khi thứ thực sự quyết định "dữ liệu nào vừa tới" là **thời điểm dữ liệu tới kho** (`_ingested_at`, processing time). Hai đại lượng này lệch nhau tới 2,73 ngày ở P99. Con trỏ theo event time chỉ tiến, không lùi: ngay khi một `event_date` mới hơn xuất hiện trong bảng đích, mọi bản ghi của ngày cũ tới sau đó đều vĩnh viễn nằm dưới ngưỡng và **không bao giờ được xử lý lại**. Đúng 455 cặp (ngày, khách) mà *toàn bộ* dữ liệu về muộn nên chưa từng có mặt ở lượt chạy đúng ngày, và cũng không có lượt chạy nào sau đó quay lại nhặt. Không exception, không hàng lỗi — chỉ thiếu, nên monitoring theo lỗi không thấy gì. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: đổi điều kiện thành `event_date >= (select coalesce(max(event_date), DATE '1970-01-01') - interval 3 day from {{ this }})`, kèm `unique_key=['event_date','customer_id']` + `incremental_strategy='merge'`. |
| **Bằng chứng** | trước: 8.645 hàng (thiếu 455) · sau: **9.100 hàng**, checksum `f8d3f591f0` giống nhau ở cả năm lượt; `gold_training_set` vẫn 12.480 (nhiệm vụ 1 không bị ảnh hưởng). |

**Vì sao nới window bắt buộc phải đi kèm `unique_key`.** Window rộng hơn nghĩa là cùng
một cặp `(event_date, customer_id)` được tính lại ở nhiều lượt chạy. Nếu model chỉ biết
`insert`, các lần tính sẽ cộng dồn — tức là tái tạo đúng lỗi của nhiệm vụ 1 trên một bảng
khác. Grain ở đây gồm hai cột nên `unique_key` là một list. Test
`dbt/tests/assert_feature_daily_grain_unique.sql` được thêm để bắt đúng tình huống đó.

**Vì sao lấy P99 làm căn cứ chứ không lấy `max`.** `max` là **một** bản ghi: chỉ cần một
sự cố mạng cá biệt là nó nhảy từ 3 ngày lên 30 ngày, và window sẽ bị neo theo giá trị của
một điểm ngoại lai. P99 là đại lượng của phân bố, đo lại tuần sau vẫn ra con số tương tự.
Chi phí cũng lệch nhau: mỗi ngày lookback thêm phải trả **ở mọi lượt chạy về sau**, không
phải trả một lần — window 30 ngày nghĩa là mỗi đêm tính lại 30 ngày dữ liệu, vĩnh viễn,
để phục vụ 1% bản ghi. Cách xử lý phần đuôi vượt window không phải là nới window mà là
cảnh báo (đếm số bản ghi rơi ngoài cửa sổ) cộng với một lần `full-refresh` định kỳ.

*Ở bộ dữ liệu này, con số 3 ngày còn được kiểm chứng thêm bằng một đường độc lập:*
`max(ingested_date - event_date) = 3` ngày, và mốc `max(event_date)` của bảng đích luôn
chậm hơn ngày vận hành một ngày, nên cửa sổ thực tế phủ tới `run_date − 4` — dư một ngày
biên an toàn.

---

## 3 · Kiểu dữ liệu cột `priority` thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Từ 08-10 model phân loại dự đoán kém hẳn, nhưng pipeline không dừng và `dbt test` vẫn xanh 9/9. Trong Silver: 6.606 hàng có `priority` sai — 6.488 hàng NULL và 118 hàng là số nhưng ngoài miền 1..4 (`0`, `5`, `-1`). |
| **Nguyên nhân** | Phép chuẩn hoá là `try_cast(priority_raw as integer)`, và `try_cast` **không bao giờ báo lỗi — nó trả NULL**. Khi backend đổi cách biểu diễn từ số sang nhãn chữ (`urgent`/`high`/`medium`/`low`) ngày 08-10, đó là **schema evolution**: ý nghĩa không đổi, chỉ đổi cách ghi. Nhưng `try_cast` đọc nhãn chữ thành NULL, nên ~99% bản ghi mỗi ngày kể từ 08-10 (977/984 hàng ngày 08-10, so với 15/1.054 hàng ngày 08-09) mất sạch tín hiệu `priority` — một cột đặc trưng biến thành hằng số NULL mà không có bất kỳ tín hiệu lỗi nào. Cùng lúc đó `try_cast` lại **chấp nhận** `'0'`, `'5'`, `'-1'` vì chúng đúng là số nguyên, dù contract quy định 1..4. Hỏng theo hai hướng ngược nhau. Contract thì đang `enforced: false` nên không kiểm tra kiểu, và không có test nào ràng buộc miền giá trị — hai lớp phòng vệ đều tắt, nên sự cố đi thẳng tới model. |
| **Ba nhóm giá trị và cách xử lý** | **Nhóm 1 — số hợp lệ** `1 2 3 4` (6.846 hàng): đúng contract cũ → giữ nguyên.<br>**Nhóm 2 — nhãn chữ** `urgent high medium low` (7.142 hàng): schema evolution → **map** về 1..4 theo tài liệu API (`urgent=1, high=2, medium=3, low=4`).<br>**Nhóm 3 — giá trị lỗi thật** `P1 P2 unknown 0 5 -1 '' NULL` (312 hàng): không mang thông tin của contract cũ → **quarantine**.<br>Tiêu chí phân biệt nhóm 2 với nhóm 3: *giá trị này có mang đúng thông tin của contract cũ, chỉ khác cách biểu diễn không?* Xử lý nhóm 2 như nhóm 3 sẽ vứt đi 7.142 bản ghi hoàn toàn hợp lệ và đẩy quarantine lên hàng nghìn hàng. |
| **Cách khắc phục** | `dbt/macros/normalize_priority.sql`: khối `CASE` xử lý đủ ba nhóm, `between 1 and 4` để loại `0/5/-1`, trả NULL cho nhóm 3; macro `priority_reject_reason` phân biệt bốn loại lỗi.<br>`dbt/models/silver/silver_tickets.sql`: **lọc bản ghi hỏng trước, xếp hạng sau** — loại *bản ghi*, không loại *ticket*.<br>`dbt/models/silver/quarantine_tickets.sql`: `where {{ normalize_priority('priority_raw') }} is null` — dùng chung macro nên hai model không thể lệch nhau.<br>`dbt/models/silver/schema.yml`: `contract.enforced: true`, thêm `not_null` + `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = **312 hàng** (đúng grain 1 hàng / 1 bản ghi CDC, checksum `ebb89036fb` ổn định) · `silver_tickets` giữ đủ **12.480 ticket** · `silver_tickets.priority` sạch, phân bố 1:3.134 · 2:3.029 · 3:3.115 · 4:3.202 · `dbt test` **21/21 pass** (bản gốc 9 test). |

**Vì sao thứ tự lọc/xếp hạng quyết định số hàng.** 312 bản ghi hỏng đều là bản ghi `op='u'`
đến *sau* bản ghi tạo ticket. Nếu xếp hạng trước rồi mới lọc, ticket nào có bản ghi mới
nhất bị hỏng sẽ biến mất hoàn toàn khỏi Silver (12.480 → 12.168), và `gold_training_set`
hụt theo. Lọc trước thì ticket đó vẫn còn, với trạng thái hợp lệ gần nhất — ta loại một
*bản ghi cập nhật*, không loại cả *thực thể*.

**Câu hỏi thiết kế 1 — chặn ở Bronze hay Silver?** Chặn ở **Silver**. Bronze phải giữ
nguyên payload nguồn, kể cả payload sai: đó là bản sao duy nhất còn lại của thứ nguồn thực
sự đã gửi. Nếu Bronze từ chối bản ghi lỗi thì khi điều tra sự cố này ta sẽ không còn gì để
đối chiếu — không trả lời được "nguồn gửi cái gì, từ lúc nào, bao nhiêu bản ghi" (chính là
bảng phân bố theo ngày ở trên, thứ đã chỉ đúng mốc 08-10). Ngoài ra ranh giới hợp lệ là
thứ **thay đổi theo thời gian**: nhãn `urgent` hôm nay là hợp lệ, hôm qua thì không. Đặt
phán xét đó ở Bronze nghĩa là mỗi lần contract đổi, dữ liệu đã mất không lấy lại được;
đặt ở Silver thì chỉ cần chạy lại transform.

**Câu hỏi thiết kế 2 — vì sao không để `dbt test` fail và dừng DAG?** Cân nhắc quy mô:
312 bản ghi hỏng so với 14.300 bản ghi CDC, 129.462 event và 31.200 chunk hoàn toàn bình
thường đang chờ tới tay người dùng. Dừng DAG nghĩa là để 0,2% dữ liệu lỗi chặn 99,8% dữ
liệu tốt — và người trực bị gọi dậy lúc 3 giờ sáng cho một sự cố không cần xử lý gấp.
Định tuyến bản ghi lỗi vào một bảng quarantine kèm `reject_reason` cho cả hai: pipeline
chạy tiếp, còn bản ghi lỗi trở thành **hàng đợi có thể xử lý trong giờ hành chính** thay
vì một dòng stack trace. Cái *phải* dừng pipeline là lỗi ở tầng khác: contract sai kiểu
trên toàn bảng, mất nguồn, hoặc tỷ lệ lỗi vọt lên hàng chục phần trăm — lúc đó số lượng
tự nó là bằng chứng rằng nguồn đã hỏng chứ không phải vài bản ghi cá biệt.

---

## 4 · Bài mở rộng

### Bài A — Query dashboard chậm

| | |
|---|---|
| **Triệu chứng** | Dashboard 38 giây, ba tháng trước 2 giây, không ai sửa dòng code nào. |
| **Nguyên nhân** | Hai lớp, cùng một gốc: **layout lưu trữ không mang thông tin lọc**. (1) `data/gold_events/` là 5.000 file Parquet tí hon, không partition — tên file không cho biết file nào chứa ngày nào, nên engine buộc phải mở **toàn bộ** 5.000 file mới biết file nào có ích; DuckDB lại đọc Parquet theo lô và làm tròn lên theo từng file, nên tập 130.683 hàng tốn 5.000.000 đơn vị công quét (gấp 38 lần số hàng thật). (2) Điều kiện lọc `strftime(event_time,'%Y-%m-%d') = '2026-08-09'` bọc cột trong một function call, nên engine không so được nó với tên thư mục partition, cũng không so được với thống kê min/max của row group — predicate mất tính sargable. Query không chậm dần vì query đổi, nó chậm dần vì **số file tăng** theo mỗi ngày ghi thêm. |
| **Cách khắc phục** | `tools/compact.py`: `COPY … TO 'data/gold_events_v2' (partition_by (event_date), order by customer_name, event_time, row_group_size 2048)`. `queries/dashboard.sql`: đọc dataset mới với `hive_partitioning = true`, viết lại điều kiện thành `event_date = DATE '2026-08-09'` (cột đứng một mình một vế). |
| **Bằng chứng** | `rows scanned` **5.000.000 → 9.324** (giảm **536×**, yêu cầu ≥ 10×) · `files` **5.000 → 14** · dung lượng 20 MB → 3,8 MB · `result hash` **4379e4c5d9f3 không đổi** · thời gian 557 ms → 23 ms. |

**Ba quyết định layout, và một ghi chú trung thực về đóng góp thật của từng cái.**
Partition theo `event_date` (14 giá trị → 14 thư mục) chứ không theo `customer_name`
(650 giá trị → 650 thư mục nhỏ, tức là tái tạo lại đúng small-file problem đang phải chữa).
Sắp `order by customer_name, event_time` để hàng cùng một khách nằm liền nhau, và
`row_group_size 2048` để một ngày (~9.300 hàng) không gói gọn trong một row group duy nhất
có min/max phủ toàn bộ bảng chữ cái. **Đo lại thì toàn bộ mức giảm 536× đến từ partition
pruning**: dựng thêm một dataset đối chứng để nguyên thứ tự và để `row_group_size` mặc
định vẫn cho đúng 9.324 rows scanned. Hai quyết định còn lại chỉ bắt đầu có giá trị khi
một partition lớn hơn nhiều lần một row group; giữ chúng vì đó là layout đúng khi dữ liệu
tăng, không phải vì chúng đóng góp vào con số hôm nay.

### Bài B — Consumer gặp sự cố giữa batch

| | |
|---|---|
| **Triệu chứng** | `make crash-test`: chạy một mạch được 20.000 hàng; bị giết ở lô 7 rồi khởi động lại chỉ còn **19.500 hàng — mất đúng 500 hàng**, bằng đúng một batch. Không trùng hàng nào. |
| **Nguyên nhân** | Thứ tự thao tác trong `consume()` là `commit() → write_batch()`: offset được ghi nhận **trước** khi dữ liệu được ghi. Khi tiến trình chết ở khoảng giữa, offset đã dịch qua lô hiện tại nhưng lô đó chưa từng chạm tới kho; lần khởi động lại đọc từ offset mới nên **bỏ qua vĩnh viễn** lô đó. Đây là ngữ nghĩa **at-most-once**, và điều tệ nhất của nó là im lặng: với consumer, lô đó "đã xử lý xong", không log nào báo thiếu. |
| **Cách khắc phục** | `ingest/consumer.py`: đảo thành `write_batch() → commit()` (**at-least-once**), và làm cho phép ghi **idempotent** — `event_id varchar primary key` trong `DDL`, `insert … on conflict (event_id) do update set …` trong `write_batch()`. |
| **Bằng chứng** | trước: 19.500 / 20.000 hàng, ✗ mất 500 · sau: **20.000 hàng / 20.000 event_id khác nhau**, không mất ✓ không trùng ✓ `C == A` ✓ — `BÀI MỞ RỘNG B: ĐẠT ✓`. `make verify` vẫn 4/4. |

**`DO UPDATE` hay `DO NOTHING`?** Chọn `DO UPDATE`. Hai lệnh chỉ khác nhau khi message
được phát lại với **nội dung đã đổi** — cùng `event_id` nhưng producer đã gửi bản sửa.
`DO NOTHING` giữ mãi bản ghi đọc được lần đầu, nên kho lặng lẽ lệch khỏi nguồn và không
có cách nào phát hiện; `DO UPDATE` hội tụ về bản ghi mới nhất. Cả hai đều chống trùng,
nhưng chỉ `DO UPDATE` giữ cho phép ghi thực sự idempotent theo nghĩa "chạy một lần hay
mười lần đều cho cùng một trạng thái cuối".

**Exactly-once không tồn tại ở tầng giao vận.** Thứ chọn được là at-least-once cộng một
phép ghi idempotent — và đó chính xác là cùng một bài học với nhiệm vụ 1: khi phép ghi
idempotent thì mọi retry ở tầng trên đều vô hại, còn khi nó không idempotent thì mọi retry
đều biến thành cơ chế nhân bản.

---

## 4b · Bảng tự chấm (theo RUBRIC.md)

| | Của tôi | Kỳ vọng | ✓/✗ |
|---|---|---|---|
| `gold_training_set` — số hàng | 12.480 | 12.480 | ✓ |
| `gold_training_set` — ổn định 3 lượt | `8dd7c98653` ×3 (và ×5) | ✓ | ✓ |
| `gold_feature_daily` — số hàng | 9.100 | 9.100 | ✓ |
| `gold_feature_daily` — ổn định 3 lượt | `f8d3f591f0` ×3 (và ×5) | ✓ | ✓ |
| `gold_doc_chunks` — số hàng | 31.200 | 31.200 | ✓ |
| `quarantine_tickets` — số hàng | 312 | 312 | ✓ |
| `silver_tickets` — số ticket | 12.480 | 12.480 | ✓ |
| `dbt test` | 21/21 pass | pass, > 9 test | ✓ |
| P99 độ trễ đo được | **2,73 ngày** | (ghi số) | ✓ |
| **Tổng verify** | 4/4 | 4/4 tiêu chí | ✓ |
| *(thưởng)* Bài A — rows scanned | 5.000.000 → 9.324 (536×) | ≥ 10× | ✓ |
| *(thưởng)* Bài B — `make crash-test` | 20.000 / 20.000, không trùng | ĐẠT | ✓ |

**Các file đã sửa:** `dbt/models/gold/gold_training_set.sql` · `dbt/models/gold/gold_feature_daily.sql` ·
`dbt/models/gold/schema.yml` · `dbt/models/silver/silver_tickets.sql` ·
`dbt/models/silver/quarantine_tickets.sql` · `dbt/models/silver/schema.yml` ·
`dbt/macros/normalize_priority.sql` · `dbt/tests/` (3 test mới) · `dags/ai_training_pipeline.py` ·
`tools/compact.py` · `queries/dashboard.sql` · `ingest/consumer.py`.
Không sửa `expected/`, `seed/generate.py`, `tools/verify.py`, `tools/explain.py`, `tools/common.py`.

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Chạy pipeline **hai lần liên tiếp** rồi so số hàng và checksum. Đây là phép thử rẻ nhất và nó phân biệt được "chạy được" với "chạy lại được". Với mỗi model incremental, đọc `dbt/target/run/…` để xem dbt thực sự sinh ra `INSERT` hay `MERGE` — đừng đoán từ `config()`. |
| 2 | Đo **hiệu số giữa event time và processing time** trước khi tin bất kỳ bộ lọc incremental nào. Một bộ lọc lấy mốc theo event time là một giả định ngầm rằng dữ liệu tới kho đúng thứ tự nó xảy ra; phân bố độ trễ nói cho biết giả định đó sai bao nhiêu. Và luôn tách hai câu hỏi "chạy lại có ổn định không" với "kết quả có đúng không" — một bảng có thể ổn định mà vẫn sai. |
| 3 | Tìm những chỗ dùng `try_cast` / `coalesce` / `on_error='ignore'` — mọi hàm **biến lỗi thành NULL một cách im lặng**. Kèm theo đó, kiểm tra contract có thật sự được bật không và có test nào ràng buộc **miền giá trị** không: contract giữ kiểu, test giữ miền, thiếu một trong hai thì `priority = 99` vẫn đi lọt. Cuối cùng, hỏi xem dữ liệu lỗi đang **đi đâu** — nếu câu trả lời là "không đi đâu cả" thì hệ thống đang mất dữ liệu mà không ai biết. |
