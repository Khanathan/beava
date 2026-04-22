# Sendo Demo — Kịch bản tiếng Việt

> **Hướng dẫn cho người thu âm:** giữ code, lệnh terminal và output bằng
> tiếng Anh — đó là những gì thật sự chạy trên màn hình. Chỉ dịch lời thoại
> (voiceover) và caption mô tả. Đọc chậm hơn nhịp nói chuyện bình thường
> một chút — người xem có thể bật phụ đề. Rõ ràng quan trọng hơn duyên dáng.

> **Bối cảnh:** người nhận là Head of IT ở Sendo — tập trung phần feature
> pipeline vào use case thực phẩm tươi của Sendo Farm (rau, trái cây, OCOP,
> giao trong ngày theo tỉnh xuất xứ). Dữ liệu trong demo là **dữ liệu tổng
> hợp (synthesized)** theo kiểu Sendo Farm — OTTO dataset public là thời
> trang Đức, không phù hợp với vertical thực phẩm tươi. Phải nói rõ điều
> này trong voiceover và email.

> **Số liệu:** các con số trong kịch bản là số **công bố**. Trước khi quay,
> chạy `bash scripts/demo-sendo/verify.sh` và cập nhật nếu máy đo được
> khác. Số thấp mà thật **luôn luôn** tốt hơn số cao mà giả.

---

## Cảnh 1 — Mở đầu (0:00–0:15)

**Hiển thị:** Title card 3 giây:

```
Beava — Real-time ML features. One binary. No Kafka.
Giới thiệu 3 phút
```

Sau đó cắt sang terminal.

**Giọng đọc:**

> Hầu hết các hệ thống recommendation và personalization real-time đều cần
> Kafka, một stream processor, một feature store, và Redis. Beava là một
> binary duy nhất làm toàn bộ những việc đó qua HTTP. Đây là cách nó chạy —
> với ví dụ pipeline cho một marketplace thực phẩm tươi như Sendo Farm.

---

## Cảnh 2 — Khởi động server (0:15–0:40)

**Hiển thị:** Terminal. Gõ chậm:

```bash
docker run -p 6900:6900 -p 6400:6400 beavadb/beava:latest
```

Hiển thị log khởi động. Highlight nhẹ cổng `:6900` và dung lượng bộ nhớ
từ `docker stats` ở terminal bên cạnh.

**Giọng đọc:**

> Chỉ một lệnh để khởi động. Không Kafka, không Redis, không Feast, không
> cần một đội platform. Dưới 400 MB bộ nhớ. Chạy được trên laptop, chạy
> được trên một con t3.small ở production.

**Caption overlay (gần cuối cảnh):** `single binary · ~380 MB RAM · port 6900`

---

## Cảnh 3 — Định nghĩa feature (0:40–1:25)

**Hiển thị:** Mở `scripts/demo-sendo/pipeline.py` trong editor sạch. Cuộn
chậm từ trên xuống dưới.

```python
import beava as bv

@bv.stream
class Event:
    user_id: str
    product_id: str
    category: str       # rau_la | trai_cay | thit_ca | sua_trung | gao_kho
    origin: str         # Hai_Duong | Lam_Dong | Bac_Giang | ...
    type: str           # view | add_to_cart | order
    price: float        # VND
    ts: float

@bv.table(key="user_id")
def BuyerFeatures(ev: Event) -> bv.Table:
    views  = ev.filter(bv.col("type") == "view")
    carts  = ev.filter(bv.col("type") == "add_to_cart")
    orders = ev.filter(bv.col("type") == "order")
    return (
        views.group_by("user_id").agg(
            views_1h        = bv.count(window="1h"),
            categories_24h  = bv.count_distinct("category", window="24h"))
        .join(carts.group_by("user_id").agg(
            cart_adds_24h    = bv.count(window="24h"),
            basket_value_24h = bv.sum("price", window="24h")),
              on="user_id", type="left")
        .join(orders.group_by("user_id").agg(
            orders_24h = bv.count(window="24h")),
              on="user_id", type="left")
    )

@bv.table(key="product_id")
def ProductFeatures(ev: Event) -> bv.Table:
    views = ev.filter(bv.col("type") == "view")
    carts = ev.filter(bv.col("type") == "add_to_cart")
    return (
        views.group_by("product_id").agg(
            trending_5m        = bv.count(window="5m"),
            trending_1h        = bv.count(window="1h"),
            unique_viewers_1h  = bv.count_distinct("user_id", window="1h"))
        .join(carts.group_by("product_id").agg(
            cart_adds_1h = bv.count(window="1h")),
              on="product_id", type="left")
    )

@bv.table(key="origin")
def OriginFeatures(ev: Event) -> bv.Table:
    orders = ev.filter(bv.col("type") == "order")
    return orders.group_by("origin").agg(
        orders_1h  = bv.count(window="1h"),
        gmv_24h    = bv.sum("price", window="24h"),
        buyers_24h = bv.count_distinct("user_id", window="24h"))
```

Trong terminal thứ hai:

```bash
python scripts/demo-sendo/pipeline.py
# → 3 tables · 11 features active
```

**Giọng đọc (chậm rãi, rõ ràng):**

> Đây là pipeline kiểu Sendo Farm. Một stream event cho mọi hành vi của
> người mua — xem, bỏ giỏ, đặt hàng — gắn thêm category và tỉnh xuất xứ.
>
> Ba bảng feature. Theo người mua: lượt xem một giờ, số category khác nhau
> trong 24 giờ, giá trị giỏ hàng, số đơn. Theo sản phẩm: trending 5 phút
> và trending một giờ — với thực phẩm tươi, cửa sổ 5 phút là tín hiệu quan
> trọng vì hàng hết nhanh. Theo tỉnh xuất xứ: GMV, số người mua, đơn theo
> giờ — dữ liệu để lập kế hoạch nguồn cung và vận chuyển.
>
> Không SQL. Không materialized view. Không Flink job. Chỉ là Python
> thuần.

**Caption overlay:** `3 tables · 11 features · key theo buyer, product, tỉnh`

---

## Cảnh 4 — Ingest sự kiện (1:25–2:05)

**Hiển thị:** Terminal chia đôi. Bên trái: trình gửi sự kiện. Bên phải:
bộ đếm chạy trực tiếp.

Terminal bên trái:

```bash
# Replay 60 giây dữ liệu marketplace thực phẩm (tổng hợp theo kiểu Sendo Farm)
# với 10.000 sự kiện/giây
cat scripts/demo-sendo/events.jsonl \
  | python scripts/demo-sendo/beava-bench.py \
      --rate 10000 \
      --to http://localhost:6900/push-batch/events \
      --duration 60
```

Terminal bên phải (hoặc overlay) hiển thị counter leo lên:

```
[ 12.0s] sent=   120,000  rate=10,010/s
[ 30.0s] sent=   300,050  rate=10,003/s
[ 60.0s] sent=   600,120  rate=10,002/s
batch latency: p50=2.1ms  p99=4.3ms  mean=2.4ms
```

**Giọng đọc:**

> Sự kiện vào qua HTTP. Dữ liệu này là tổng hợp theo kiểu Sendo Farm — lý
> do: dataset e-commerce công khai gần nhất là OTTO, thời trang Đức, không
> có tín hiệu thực phẩm tươi cần cho demo này. Chúng ta đẩy 10.000 sự kiện
> mỗi giây, duy trì ổn định, trên một process duy nhất. Dưới 500 MB bộ nhớ.
> Latency p99 khi ingest khoảng 4 mili-giây. Đây mới chỉ là một chiếc
> laptop — phần cứng production còn đi xa hơn nhiều.

**Caption overlay (đáy màn hình):** `10.000 sự kiện/giây · synthesized Sendo-style · <500 MB RAM`

---

## Cảnh 5 — Serve feature (2:05–2:50)

**Hiển thị:** Terminal với HTTP query. Pick một `user_id` có sẵn trong
`events.jsonl` (xem `scripts/demo-sendo/README.md` để lấy ID phù hợp).

```bash
curl http://localhost:6900/features/u00042
```

Trả về:

```json
{
  "BuyerFeatures": {
    "views_1h": 14,
    "categories_24h": 3,
    "cart_adds_24h": 2,
    "basket_value_24h": 185000,
    "orders_24h": 1
  },
  "_latency_ms": 2
}
```

Sau đó chuyển sang query theo sản phẩm và theo tỉnh, để show rõ ba bảng
đều phục vụ real-time:

```bash
curl http://localhost:6900/features/p00017?table=ProductFeatures
# → {"trending_5m": 42, "trending_1h": 310, "unique_viewers_1h": 198, "cart_adds_1h": 37}

curl http://localhost:6900/features/Lam_Dong?table=OriginFeatures
# → {"orders_1h": 124, "gmv_24h": 28_400_000, "buyers_24h": 812}
```

Cuối cùng mở watch để cho thấy số cập nhật liên tục:

```bash
watch -n 1 'curl -s http://localhost:6900/features/u00042 | jq'
```

`views_1h` tăng: 14 → 17 → 21 → 25.

**Giọng đọc:**

> Truy vấn feature cho bất kỳ entity nào qua HTTP — người mua, sản phẩm,
> hay tỉnh xuất xứ. Thời gian phản hồi 2 mili-giây. Số cập nhật theo
> real-time — không có bước đồng bộ, không có cửa sổ cache bị lệch. Đặt
> URL này vào model serving hoặc ranking service là xong phần tích hợp.

---

## Cảnh 6 — Cái gì nằm bên trong, cái gì không (2:50–3:10)

**Hiển thị:** Sơ đồ chia đôi đơn giản. Gọn gàng, không rườm rà.

```
Hệ feature real-time truyền thống:         Beava:

  ┌──────────┐                              ┌──────────┐
  │  Kafka   │                              │          │
  └────┬─────┘                              │          │
       │                                    │  Beava   │
  ┌────▼─────┐                              │          │
  │  Flink   │                              │ (một     │
  └────┬─────┘                              │  binary) │
       │                                    │          │
  ┌────▼─────┐                              │          │
  │  Feast   │                              │          │
  └────┬─────┘                              │          │
       │                                    │          │
  ┌────▼─────┐                              │          │
  │  Redis   │                              │          │
  └──────────┘                              └──────────┘
```

**Giọng đọc:**

> Stack feature real-time truyền thống cần bốn hệ thống và một đội để vận
> hành. Beava gộp tất cả vào một process. Sendo đã có Kafka cho các hệ
> khác? Beava đọc từ đó. Chưa có? Không cần phải dựng lên chỉ vì Beava.

---

## Cảnh 7 — Kết thúc (3:10–3:30)

**Hiển thị:** Text card đơn giản, không animation:

```
Beava
Real-time features. HTTP vào, HTTP ra.

• 10.000 sự kiện/giây trên một binary (đã kiểm chứng)
• Feature viết bằng Python — ví dụ 3 bảng, 11 feature kiểu Sendo Farm
• Một process duy nhất, dưới 500 MB RAM
• Tương thích với Kafka / Redis / MySQL / MongoDB sẵn có của Sendo
  — hoặc chạy độc lập

beava.dev
hoang@beava.dev
```

**Giọng đọc:**

> Nếu anh thấy thứ này có thể vừa vặn vào stack hiện tại, em rất mong có
> một cuộc trao đổi 30 phút. Em cũng sẵn sàng chạy một proof of concept
> trên chính mẫu dữ liệu traffic của Sendo Farm.

**End card:** *Cảm ơn anh đã xem. — Hoàng*

---

## Email gửi kèm video

Giữ ngắn. Head of IT bận.

**Tiêu đề:** Beava — video giới thiệu 3 phút như anh yêu cầu

> Chào anh [tên],
>
> Cảm ơn anh đã quan tâm. Em gửi anh video 3 phút giới thiệu Beava, chạy
> với ví dụ pipeline cho một marketplace thực phẩm tươi kiểu Sendo Farm —
> 11 feature, ba bảng (theo người mua, sản phẩm, tỉnh xuất xứ), ở 10.000
> sự kiện mỗi giây trên một binary duy nhất: [video link]
>
> Dữ liệu trong video là tổng hợp (synthesized) theo kiểu Sendo Farm —
> chưa chạm vào bất kỳ dữ liệu nào của Sendo. Em có thể chạy một POC nhỏ
> trên một phần traffic thật của Sendo nếu anh thấy có ích.
>
> Tóm tắt: một process thay thế cho cụm Kafka + stream processor +
> feature store + Redis trong bài toán real-time feature serving. Sendo
> đã chạy Kafka, gRPC + Pub/Sub, Redis, MongoDB cho các hệ khác — Beava
> đọc được từ Kafka nếu anh muốn tận dụng, hoặc chạy độc lập nếu không.
>
> Nếu thấy có chỗ hợp với stack rec / ranking / personalization của Sendo
> Farm hoặc marketplace chính, em rất sẵn sàng gọi 30 phút để đi sâu vào
> chi tiết.
>
> Trân trọng,
> Hoàng

---

## Ghi chú sản xuất

**Ngân sách thời gian:** nửa ngày quay, nửa ngày dựng.

- **Quay:** QuickTime hoặc OBS. Đừng làm phức tạp.
- **Voiceover:** thu riêng. Đọc chậm hơn bình thường một chút — người xem
  có thể bật phụ đề. Rõ ràng hơn truyền cảm.
- **Phụ đề (CC):** auto-generate rồi edit tay. Head of IT ở Việt Nam vẫn
  nên có CC tiếng Việt cho chắc.
- **Con trỏ chuột:** ẩn trong terminal, hoặc dùng công cụ highlight.
- **Terminal:** nền tối, font monospace ≥ 18 pt.
- **Nhạc nền:** không. Giọng + tiếng gõ phím sạch hơn.
- **Độ phân giải:** 1080p trở lên, không cần 4K.
- **Độ dài mục tiêu:** 3:30. Quá 4 phút mất ~30% người xem mỗi phút.

**Điều KHÔNG được show:**

- Cơ chế fork — để dành cho demo riêng.
- So sánh với đối thủ hoặc chỉ đích danh sản phẩm khác.
- Lời pitch thương mại — không giá, không "cách mạng," không tính từ kêu.
  Để video tự nói.
- Bất kỳ tính năng nào có thể trục trặc trên sân khấu. Diễn tập 3 lần
  trước khi quay.
- **Đừng nói "dữ liệu Sendo" hay "dữ liệu thật của Sendo"** — dữ liệu
  demo là tổng hợp. Phải nói rõ "tổng hợp theo kiểu Sendo Farm" hoặc
  "synthesized Sendo-style."

---

## Checklist trước khi bấm Record

Trước khi quay, chạy:

```bash
bash scripts/demo-sendo/verify.sh
```

Script kiểm tra 8 điều kiện và in bảng so sánh số **công bố** với số **đo
được**. Nếu có số nào đo được tệ hơn số công bố:

- Sửa kịch bản phía trên để ghi đúng con số đo được.
- Số thấp mà thật **luôn luôn** tốt hơn số cao mà giả.

**Sau khi verify xanh**, thu xếp ba terminal như mô tả trong
`scripts/demo-sendo/README.md`, diễn tập 3 lần, rồi quay.
