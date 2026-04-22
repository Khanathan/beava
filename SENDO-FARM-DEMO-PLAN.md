# Sendo Farm Interactive Demo — Build Plan

**Artifact:** a single-page interactive demo at `178.104.164.100/sendo-farm` that shows Beava powering real-time recommendations, trending, and provincial supply signals for a Sendo-Farm-style marketplace. Paired with a 90-second Loom tour video. Delivered via an existing LinkedIn DM thread to the Head of IT at Sendo.

**Supersedes** the 3:30 video demo plan in `SENDO-DEMO.md` and the redesigned 90s video in `SENDO-DEMO-VI.md`. Both are still useful as voiceover source material, but the primary artifact is now the interactive page, not the video.

**Design doc (why):** `~/.gstack/projects/tally/petrpan26-arch-tpc-full-shard-design-20260419-103411.md` (office-hours output).

---

## Why a page instead of just a video

- He can **click, not just watch.** Dwell time ~3x on an interactive page vs a passive video.
- He can **share the URL with his CTO and rec team lead.** A video link goes to one inbox; a demo URL goes to three meetings.
- The page **IS the pitch running live.** Latency numbers are real, features tick in real time, queries hit a real Beava.
- He can come back to it. A video gets watched once.
- Follows the existing pattern of your internal demos at `178.104.164.100/05-recommendation` and `/07-ad-bidding` — this is `/sendo-farm` in the same series.

The video becomes a **60-90s tour guide** pointing him at the page. Opener + close on camera, middle is a screen recording walking through the page.

---

## Observations from Sendo Farm screenshots (2026-04-19)

Driving design decisions:

| Observation | Design implication |
|---|---|
| "Đề xuất" tab exists — static, feels batch | Demo page shows what this tab looks like live: personalized per user, <5ms |
| "BÁN CHẠY" badge — appears on products, unclear window | Show "Trending 5 phút" badge — perishability-appropriate window |
| "Đã bán 69.249" — all-time count | Show "Đã bán hôm nay" and "Đang trending" — time-windowed social proof |
| Regional provenance: "Ổi Nữ Hoàng Tiền Giang," "Cam sành Vĩnh Long" | Exactly matches OriginFeatures; province is a first-class entity |
| Green / red / white palette, Roboto-ish Vietnamese body font | Clone palette so the demo feels like a Sendo Farm feature, not a product pitch |
| "Sản phẩm tương tự" link on each card | Future: item-to-item reco. Not in v1, flag as v2 |
| Categories: CJ Foods, Rau Củ Quả, Thịt-Trứng, Thực Phẩm Đông Mát, Trái Cây, Mì Miến Phở Cháo, Bánh Kẹo Trà Cafe | Seed catalog uses these exact category labels |

---

## Page sections

### 1. Hero (sticky)

```
Beava × Sendo Farm (demo)
Recommendations real-time cho marketplace thực phẩm tươi
Dữ liệu tổng hợp kiểu Sendo Farm · latency đo thật

[Live counter bar:]
  Events: 8,423/sec  ·  Ingest p99: 4ms  ·  Query p99: 1.8ms  ·  Uptime: 02:14:33
```

### 2. "Đề xuất cho bạn" — personalized rec with user picker

Dropdown of 3-4 synthetic personas:
- `u12034` — thường xuyên mua rau củ, TP.HCM, basket trung bình 180,000đ
- `u44201` — user mới, vừa xem trái cây Tiền Giang
- `u08915` — mua OCOP, thích sản phẩm Đà Lạt
- `u99001` — user ít hoạt động (cold-start test)

Click a persona → page calls `GET /features/u12034` on Beava → renders:
- 6 product cards (clone Sendo Farm card style: image placeholder, "BÁN CHẠY" badge, price in VND, "Đã bán" count, "+" button)
- Latency badge on each card from actual query RTT
- Explanation panel: "Based on: views_1h=14, categories_24h=3 (rau_la, trai_cay), basket_value_24h=185,000đ · query 2.1ms"

### 3. "Sản phẩm đang bán mạnh" — trending grid

12-product grid, real-time updating every 2s. Each card shows:
- Product name (VN) + placeholder image
- **Trending 5m: ↑ 47** (live number)
- **Unique viewers 1h: 312**
- Latency badge
- Numbers visibly tick every 2s as background event replay keeps features moving

### 4. "Theo tỉnh xuất xứ" — province view

Vietnam map or province dropdown (map is nicer but dropdown ships faster). Click a province → `GET /features/Vinh_Long` → renders:
- **GMV 24h:** 28,400,000đ (updating live)
- **Buyers 24h:** 812
- **Orders 1h:** 124
- Top 5 products from that origin
- Latency: 2ms

This is the slot Sendo Farm doesn't have in their app today. Supply-side signal the Head of IT has probably never seen surfaced this cleanly.

### 5. "Batch hôm nay vs Beava real-time" — side-by-side

Only section that explicitly compares. Two columns:
- **Left — Sendo Farm hôm nay (batch):** static "Đã bán 69,249" (screenshot from their app)
- **Right — Với Beava (real-time):** "Đã bán hôm nay 2,840 · Trending 5m: 47 · Unique viewers 1h: 312"

One sentence under it: "Cùng sản phẩm. Tín hiệu khác hẳn."

### 6. Footer CTA

```
Muốn chạy demo này trên event data thật của Sendo?
Em sẽ set up Beava local, pipeline giống trang này, trên 1 tuần data mẫu. Miễn phí.

[DM Hoàng trên LinkedIn]   beava.dev   hoang@beava.dev
```

---

## Data model

**Beava backend:** the already-registered 11-feature pipeline from `scripts/demo-sendo/pipeline.py`. No changes needed to the pipeline itself.

**API calls the page makes:**

| Endpoint | Called when | Polled? |
|---|---|---|
| `GET /features/{user_id}` | Persona dropdown changes | No, on click |
| `GET /features/{product_id}` | Each product card in grid | Yes, every 2s |
| `GET /features/{province}` | Province dropdown changes | No, on click |
| `GET /debug/stats` (or equivalent) | Hero counter bar | Yes, every 1s |

**Latency measurement:** use `performance.now()` client-side, bracket the `fetch()` call, display the wall-clock RTT. This is slightly worse than server-reported latency (includes network hop) but it's what a real integrator would see, so it's the honest number.

**Product catalog seed:** ~30 SKUs. Use product names from the Sendo Farm screenshots (transcribed, not scraped images — IP safety) mapped onto our synthesized `product_id`s. Example:
```
p00001 → "Ổi Nữ Hoàng Tiền Giang, túi 0.9-1.1kg" (category: trai_cay, origin: Tien_Giang, price: 24000)
p00002 → "Cam sành da cám Vĩnh Long, túi 3kg" (category: trai_cay, origin: Vinh_Long, price: 30000)
p00003 → "Sủi Cảo Tôm Thịt Cầu Tre 240g" (category: thit_ca, origin: Ha_Noi, price: 54900)
... (30 total)
```

**Images:** use colored squares with emoji (🥬🍊🥩🐟🍎) as placeholders. Clearly labeled "Demo data — not Sendo Farm products" in small text at page bottom. No actual Sendo product images (avoids IP/trademark concerns).

**Background event replay:** a low-rate bench process runs continuously, ~200-500 EPS, keeping feature values fresh so the page shows movement. Uses the existing `scripts/demo-sendo/events.jsonl`, loops forever.

---

## File layout

```
site/
├── sendo-farm/
│   ├── index.html            ← page shell, sections 1-6
│   ├── app.js                ← fetch loop + DOM updates
│   ├── catalog.js            ← 30-SKU product catalog (name, category, origin, price, emoji)
│   ├── personas.js           ← 4 synthetic buyer personas
│   ├── styles.css            ← Sendo Farm palette clone
│   └── README.md             ← what this is, how to run locally

scripts/demo-sendo/
├── serve-replay.sh           ← starts continuous low-rate event replay to Beava
└── (existing files unchanged)
```

---

## Build order (half a day)

| # | Task | Owner | Time | Dependencies |
|---|---|---|---|---|
| 1 | Static HTML shell + CSS with Sendo Farm palette | - | ~90 min | None |
| 2 | Product catalog seed (30 SKUs from screenshots) | - | ~45 min | None |
| 3 | JS: polling loop + DOM updates for sections 1-3 | - | ~60 min | 1, 2 |
| 4 | JS: province section (4) | - | ~30 min | 3 |
| 5 | JS: batch vs real-time side-by-side (5) | - | ~20 min | 3 |
| 6 | Hero counter bar (section 1) | - | ~20 min | 3 |
| 7 | `serve-replay.sh` — low-rate continuous event replay | - | ~20 min | None |
| 8 | Deploy to `178.104.164.100/sendo-farm` | - | ~30 min | all above |
| 9 | Beava config: CORS headers for the page origin | - | ~15 min | 8 |
| 10 | Smoke test end-to-end from a fresh browser | - | ~15 min | all above |
| 11 | Record 90s Loom tour | - | ~30 min | 10 |
| 12 | Send Loom MP4 + page URL in LinkedIn DM | - | ~5 min | 11 |

**Total:** ~6 hours. Pages 1-6 can be shipped in 4 hours if we skip the province map (use dropdown). Video + delivery adds 35 min on top.

---

## Open questions / decisions

| # | Question | Recommendation |
|---|---|---|
| 1 | Province picker: map or dropdown? | Dropdown for v1. Map is prettier but 2-3 extra hours and he's watching on his phone. |
| 2 | Event replay rate: 200 EPS or 5000 EPS? | 500 EPS. Enough for visible movement on the grid without stressing the host during a long demo window. |
| 3 | Product images: emoji placeholders vs stock photos? | Emoji + colored squares. Stock photos look cheap; emoji reads as intentional "demo data" styling. |
| 4 | Hero hook text — lead with "real-time" or "không cần platform team"? | Lead with "real-time cho thực phẩm tươi" (vertical + category specific), secondary line on operational simplicity. |
| 5 | Beava auth: open read endpoint or token-gated? | Token-gated with a demo token embedded in the page. `GET` only; `POST /push/*` stays admin-only. |
| 6 | Does the page need to survive long-term or is it ephemeral? | Ephemeral. Spin up for the demo window, leave it running ~2 weeks, take down after the prospect responds (or doesn't). |
| 7 | Language: 100% Vietnamese, or English subtitles under VN? | 100% VN in the UI. English only in the small-print footer credit. He's VN, his team is VN. |

---

## Success criteria

Shipped:
- [ ] Page reachable at `178.104.164.100/sendo-farm` from any browser
- [ ] Every number on the page updates visibly at least every 5 seconds during demo hours
- [ ] Persona dropdown shows 4 different feature profiles (not cached identical responses)
- [ ] Province dropdown shows 10 different GMV/buyer profiles
- [ ] Page works on mobile (he will check it on his phone)
- [ ] Latency badges show numbers < 10ms under normal load
- [ ] 90s Loom video recorded in one take, under 100 MB, downloadable as MP4

Delivered:
- [ ] Loom MP4 + page URL sent in the LinkedIn DM thread within 48 hours of page going live
- [ ] Message in Vietnamese, 2 lines max
- [ ] No YouTube link, no Loom link, no PDF, no deck

Converted (stretch):
- [ ] He replies within 7 days
- [ ] He shares the URL internally (signal: second person from Sendo accesses the page — check nginx logs)
- [ ] He agrees to the 1-week POC offer

---

## What's out of scope

- Scraping Sendo Farm's actual product images (IP risk, not worth it)
- Interactive Vietnam SVG map (dropdown ships faster, 95% of the value)
- Real auth (token is embedded in page JS, fine for ephemeral demo)
- Item-to-item "Sản phẩm tương tự" recommendation (requires a different Beava pipeline — flag as v2 only if he asks)
- Mobile app mockup (web page on mobile is good enough)
- A/B comparison against their actual recsys (we don't have their data)
- Publishing the `beava-sendo-farm-reference` repo publicly (keep this 1:1 for now; open-source it only if multiple prospects ask about it)
- Docker image publication (local binary works for the demo host; Docker can wait for v1.1 launch)

---

## What happens after he replies (or doesn't)

**If he replies within 7 days:**
- Send the 1-week POC offer concretely: "Anh gửi em 1 tuần event data ẩn danh, em sẽ đứng pipeline y như demo nhưng trên data thật và show anh kết quả cuối tuần."
- If he says yes, the page becomes reusable — duplicate `site/sendo-farm/` to `site/sendo-farm-real/` pointed at the POC Beava instance.

**If silence after 14 days:**
- Do NOT follow up through LinkedIn DM with "just checking in." Kills the relationship.
- Do reuse the page pattern for the next prospect: rename `/sendo-farm` to `/[next-marketplace]`, swap province weights, swap category labels. Each prospect gets their own page. ~2 hours of work per reuse.
- The pattern itself is the asset now.

**If he ghosts and a competitor marketplace reaches out:**
- The page + pipeline are already 80% built. Swap catalog + province weights. Ship to the new prospect in half a day. This is the compounding move.
