# Fraud Feature Catalogue — Production Patterns

**Source:** Synthesized from 10 industry sources (Stripe Radar rule reference, Adyen RevenueProtect / ShopperDNA, Shopify fraud analysis, Visa Protect, Tecton/Chalk fraud feature-store case studies, Feldera real-time CC-fraud tutorial, Flagright AML baselining, SEON / SHIELD / Veriff ATO playbooks, Featurespace / IPQS, Featuretools/DFS, Kaggle CC-fraud feature engineering papers). Specific URLs at end.
**Date:** 2026-04-27
**Author:** research agent for Beava v0 fraud-bench design
**Beava reference:** STATE.md (55 ops shipped) + PROJECT.md positioning (single-thread in-memory feature server, push events / get features over HTTP+TCP).

> **Honesty note.** Vendor docs are coy: Stripe and Sift list "hundreds of signals" but enumerate maybe 30 by name; the rest is documented only as patterns ("velocity check", "ShopperDNA cluster"). The catalogue below names every feature explicitly so it can be wired into a Beava pipeline; where vendors don't disclose specifics, features are reconstructed from public papers (Bahnsen et al. 2016 "Feature engineering for credit card fraud detection"; MDPI Electronics 2024) and from open feature-store tutorials (Tecton, Feldera, Featurespace). Treat this as "what a competent fraud team builds" rather than "what FAANG runs internally."

---

## Section 1: Entity model

A real payments fraud system tracks ~14 entities. Each is either a *push key* (state mutates per event) or a *lookup-only enrichment* (loaded once, joined per push).

| # | Entity | Cardinality (medium fintech) | Aggregation key? | Notes |
|---|---|---|---|---|
| 1 | **User / account** | 100k–10M | yes (primary) | The "customer". Most velocity / behavioral / amount features key on this. |
| 2 | **Card / payment instrument** (PAN-fingerprint) | 1.05–2.0× users | yes | One user can have multiple cards; one card can be reused across users (fraud signal). Stripe `card_fingerprint`. |
| 3 | **BIN** (first 6 of card) | ≤30k globally | enrichment + key | Static issuer table joined per txn for `card_country / card_brand / card_funding`. Used for "BIN-vs-IP" mismatch. |
| 4 | **Device / device fingerprint** | 0.8–1.5× users | yes | Browser canvas + UA + OS + plugins hash. Devices/account and accounts/device are both load-bearing. |
| 5 | **IP** | 0.5–3× users | yes | Per-IP velocity matters; IPs cycle (proxy/VPN), so usable for ≤24h windows. |
| 6 | **IP /24 block (CIDR)** | ~3M globally | yes | "5 cards from 5 IPs in same /24" beats "1 card per IP" — fraudsters rotate IPs within a netblock. |
| 7 | **Email** | ≈users | yes | Disposable/free vs corporate domain matters. |
| 8 | **Email domain** | ≤500k registered, ~10k common | enrichment + key | `gmail.com` vs `mailinator.com` is a categorical risk feature. Domain-age is the heavy hitter (newly registered → high risk). |
| 9 | **Phone number** | ≈users | yes | Carrier-type (mobile vs VoIP) matters. |
| 10 | **Shipping address** (geo-hashed) | ≈users | yes | Different from billing → flag. Same address with N distinct cards → flag. |
| 11 | **Billing address** | ≈cards | enrichment | AVS check key. |
| 12 | **Merchant / MCC** | hundreds–thousands per platform | yes (when platform-side) | "Sum spent at high-risk MCCs in 24h". |
| 13 | **Geo** (country / region / city / lat-lon) | 250 / ~5k / ~500k | enrichment + features | Distance-from-home, geo-velocity, country-hops. |
| 14 | **Session / browser fingerprint** | 1.5–5× users | yes (short-lived) | Session-length, session-events; usually 30-min TTL. |
| 15 | **Bank account / routing** (ACH) | ~users (US) | yes | NSF rate, return-code velocity. |
| 16 | **Crypto wallet** | a fraction of users | yes | Sanctioned-list match, mixer-touched flag. |
| 17 | **Promo code / referral chain** | hundreds of codes; chains 1–5 deep | yes | Promo-redemptions-per-IP, referral-chain-depth. |
| 18 | **3DS challenge** | per-txn ephemeral | event only | Outcome (success/fail/abandoned) is a feature input. |

### Canonical relationships

```
User ──┬── 1:N → Card ──┬── 1:1 → BIN (enrichment)
       │                └── 1:N → Authorization
       ├── 1:N → Device ─── N:M → Other Users (sharing-fraud)
       ├── 1:N → IP ──── N:1 → IP /24 ─── N:1 → Country
       ├── 1:1 → Email ─ 1:1 → Email-domain
       ├── 1:1 → Phone ─ 1:1 → Carrier-type
       ├── 1:N → Shipping address (N:M with other users)
       └── 1:N → Session ─ 1:N → Login event
```

The two-side relationships that create graph features (devices↔users, IPs↔cards, addresses↔cards) are the "shared-attribute" signal: Beava can compute them today as `count_distinct(other_id)` keyed on the shared entity, but full graph-walk (2-hop "is this user 1 hop from a flagged user?") needs new ops (see §5).

### Likely Beava aggregation keys (the `group_by` axes)

For a fraud-team pipeline, these five axes carry 80% of the features:

1. `user_id` — primary, ~50% of features
2. `card_fingerprint` — ~15%
3. `device_id` — ~10%
4. `ip_address` (or `ip_block` /24) — ~15%
5. `merchant_id` — ~10% (platform-side, ramped down for Priya's first-party fintech)

Email / phone / shipping-address can also be keys but they're typically derived from the user record and produce overlapping features.

---

## Section 2: Event types

A real payments fraud system ingests ~12 event types. Each is `@bv.event` (immutable append-only) in Beava. Cardinality numbers are end-to-end; the bench-relevant per-event-type rates differ wildly.

| # | Event | Small (1k users, 100 txn/d) | Medium (100k users, 100k txn/d, 1k EPS peak) | Large (10M users, 10M txn/d, 100k EPS peak) | Payload fields | What features key off it |
|---|---|---|---|---|---|---|
| 1 | **Login** (success + failed) | 1k/d | 1M/d | 100M/d | `user_id, success, ip, device_id, ua, geo` | Velocity per IP/user/device; ATO trigger; device-novelty |
| 2 | **Signup / KYC submit** | 5/d | 5k/d | 500k/d | `email, phone, ssn_hash, ip, device_id, name, dob` | Signup velocity per IP; PII-reuse; synthetic-identity |
| 3 | **Card add / verify** | 5/d | 5k/d | 500k/d | `user_id, card_fingerprint, bin, country, success` | Cards-per-account; card-test patterns; cards-per-device |
| 4 | **Transaction (auth attempt)** | 100/d | 100k/d | 10M/d | `user_id, card_fingerprint, amount, currency, mcc, ip, device_id, geo, declined_code?, 3ds_outcome?` | Most amount/velocity features. THE primary event. |
| 5 | **Transaction capture** | ~100/d | ~100k/d | ~10M/d | `auth_id, amount` | Auth-vs-capture amount diff; partial-capture rate |
| 6 | **Refund / void** | 1/d | 1k/d | 100k/d | `txn_id, amount, reason` | Lifetime refund ratio; refund velocity |
| 7 | **Chargeback / dispute** | 0.1/d | 100/d | 10k/d | `txn_id, reason_code, amount` | Lifetime cb ratio; days-since-last-cb; cb-streak |
| 8 | **3DS challenge outcome** | embedded in #4 | — | — | event subset | Auth + step-up rate per BIN/issuer |
| 9 | **Address / email / password change** | 5/d | 5k/d | 500k/d | `user_id, field_changed, old, new, ip, device_id` | ATO trigger combined with login + payment |
| 10 | **ACH / wire / SEPA initiated + settled** | 10/d | 10k/d | 1M/d | `user_id, bank_account, amount, return_code?, settled_at?` | NSF rate, return-code stream, micro-deposit timing |
| 11 | **Withdrawal request** | 1/d | 1k/d | 100k/d | `user_id, amount, destination` | "Velocity of cashouts after deposit" — synthetic-fraud signal |
| 12 | **Crypto on-ramp / off-ramp** | 0.5/d (if applicable) | 500/d | 50k/d | `user_id, wallet_addr, asset, amount` | Sanctioned-wallet match; mixer-touched flag |
| 13 | **Promo redemption** | 1/d | 1k/d | 100k/d | `user_id, promo_code, ip, device_id` | Promo-per-IP velocity; referral-chain depth |
| 14 | **Manual review queued / approved / rejected** | 1/d | 100/d | 10k/d | `user_id, txn_id, decision, reviewer` | Adjudication feedback (training signal, not realtime) |
| 15 | **Geo-location ping** (mobile/web) | optional | optional | optional | `user_id, lat, lon, ts` | distance_from_home; geo_velocity |

**Mix ratio at large scale:** transactions ~70%, logins ~20%, device events ~5%, refunds/chargebacks ~1%, signups/card-adds ~2%, all other ~2%. (For YC seed, transactions are usually the only real volume.)

**Heavy tail (Zipfian alpha):** user-id distribution α≈0.7–0.8 in fraud workloads (the top 10% of users generate 50–60% of events; whales push α toward 1.5; bot floods push α toward 0.5 — uniform). The Phase 19 default of α=1.0 is a reasonable midpoint; the catalogue's bench config below assumes α=1.0.

---

## Section 3: Feature catalogue

**110 features** grouped by detection pattern. Each row maps to a Beava op already in the 55-op catalogue, or flags "needs new op".

### Category 1: Velocity / rate-limit (15 features)

The bread and butter. Stripe Radar's `authorized_charges_per_card_number_hourly`, Adyen's "shopper IP used more than X times".

| # | Name | Op | Window | Group | Why |
|---|---|---|---|---|---|
| 1.1 | `txn_count_5m` | `count` | 5m | user_id | bot/burst |
| 1.2 | `txn_count_1h` | `count` | 1h | user_id | sustained burst |
| 1.3 | `txn_count_24h` | `count` | 24h | user_id | daily anomaly |
| 1.4 | `txn_count_7d` | `count` | 7d | user_id | weekly profile |
| 1.5 | `txn_count_lifetime` | `count` | lifetime | user_id | tenure |
| 1.6 | `txn_per_card_1h` | `count` | 1h | card_fingerprint | stolen-card rapid use |
| 1.7 | `txn_per_card_24h` | `count` | 24h | card_fingerprint | Stripe `authorized_charges_per_card_number_*` |
| 1.8 | `txn_per_ip_1h` | `count` | 1h | ip_address | bot from one IP |
| 1.9 | `txn_per_ip_24h` | `count` | 24h | ip_address | Stripe `authorized_charges_per_ip_address_*` |
| 1.10 | `txn_per_ip_block_1h` | `count` | 1h | ip_block_24 | rotating IPs in netblock |
| 1.11 | `txn_per_device_24h` | `count` | 24h | device_id | Sift device-level velocity |
| 1.12 | `login_per_user_1h` | `count` | 1h | user_id | credential stuffing |
| 1.13 | `login_failed_per_ip_1h` | `count` (with filter) | 1h | ip_address | brute-force from one IP |
| 1.14 | `signup_per_ip_24h` | `count` | 24h | ip_address | mass account creation |
| 1.15 | `card_add_per_device_24h` | `count` | 24h | device_id | card-stuffing on one device |

All 15 use `count` (Phase 5). Filter on `success=false` for #1.13 needs `bv.count(filter=...)` — *currently expressed via a `.filter()` upstream of `.group_by()`*; that works but requires a separate event-stream node. **Status: 100% covered.**

### Category 2: Amount profile (12 features)

Behavioral baseline. Bahnsen et al. 2016, Tecton's `avg_spend_pd / pw / pm`, Feldera tutorial.

| # | Name | Op | Window | Group | Why |
|---|---|---|---|---|---|
| 2.1 | `avg_amount_30d` | `avg` | 30d (= 7d cap, see note) | user_id | baseline |
| 2.2 | `stddev_amount_30d` | `stddev` | 7d cap | user_id | variability |
| 2.3 | `max_amount_lifetime` | `max` | lifetime | user_id | "biggest ever" |
| 2.4 | `p50_amount_24h` | `percentile q=0.5` | 24h | user_id | typical |
| 2.5 | `p99_amount_24h` | `percentile q=0.99` | 24h | user_id | tail anomaly |
| 2.6 | `sum_amount_24h` | `sum` | 24h | user_id | daily cap |
| 2.7 | `sum_amount_lifetime` | `sum` | lifetime | user_id | LTV-ish |
| 2.8 | `amount_z_score_30d` | `z_score` | 7d cap | user_id | deviation from baseline |
| 2.9 | `amount_ewma_1h` | `ewma half_life=1h` | n/a | user_id | recent baseline |
| 2.10 | `amount_ew_zscore` | `ew_zscore` | n/a | user_id | live deviation |
| 2.11 | `amount_decayed_sum_24h` | `decayed_sum half_life=24h` | n/a | user_id | recency-weighted spend |
| 2.12 | `amount_seasonal_deviation` | `seasonal_deviation` | weekly | user_id | "is this txn unusual for this hour-of-week?" |

Note on windows >7d: Beava caps at 64 buckets per windowed op; 30d/7d at 1h-bucket = 720 buckets — too many. Workaround: 30d at 12h buckets = 60 buckets, OK. Or use `decayed_sum/ewma` (no fixed window). All ops shipped. **Status: 100% covered.**

### Category 3: Geographic (10 features)

Stripe `card_country` vs `ip_country`, Adyen geo rules, Visa Protect "geolocation". Beava Phase 11 covers most of this.

| # | Name | Op | Window | Group | Why |
|---|---|---|---|---|---|
| 3.1 | `geo_velocity_kmh` | `geo_velocity` | n/a | user_id | impossible-travel detector |
| 3.2 | `distance_from_home` | `distance_from_home` | n/a | user_id | "1000 km from usual" |
| 3.3 | `unique_geo_cells_24h` | `unique_cells precision=10` | 24h | user_id | how many distinct cities |
| 3.4 | `geo_entropy_24h` | `geo_entropy` | 24h | user_id | spread vs concentration |
| 3.5 | `geo_spread_km_24h` | `geo_spread` | 24h | user_id | bounding-circle diameter |
| 3.6 | `country_hops_7d` | `count_distinct(country)` | 7d cap | user_id | country mobility |
| 3.7 | `card_country_eq_ip_country` | event-level expression | instant | event | classic AVS-like |
| 3.8 | `bin_country_eq_billing_country` | event-level expression | instant | event | BIN-vs-billing mismatch |
| 3.9 | `is_anonymous_ip` | enrichment lookup | instant | event | proxy/Tor/VPN match |
| 3.10 | `geo_distance_last_event` | `geo_distance` | n/a | user_id | how far jumped |

Items 3.7–3.9 are stateless / enrichment — they live in `with_columns(...)` on the event, not in `.agg()`. **Status: 100% covered for stateful pieces; 3.7/3.8/3.9 require enrichment join to a country/IP-block static table (covered by event↔table join in Phase 12).**

### Category 4: Device / fingerprint (8 features)

Sift, Riskified, Forter all lead with this. ShopperDNA's "linked-cluster" idea is what powers it.

| # | Name | Op | Window | Group | Why |
|---|---|---|---|---|---|
| 4.1 | `devices_per_user_30d` | `count_distinct(device_id)` | 30d (12h-bucket) | user_id | device proliferation |
| 4.2 | `users_per_device_30d` | `count_distinct(user_id)` | 30d (12h-bucket) | device_id | shared device → ATO/ring |
| 4.3 | `cards_per_device_24h` | `count_distinct(card_fingerprint)` | 24h | device_id | card stuffing |
| 4.4 | `device_first_seen_age` | `age` (since `first_seen`) | n/a | device_id | brand-new device → step-up |
| 4.5 | `device_seen_for_user` | `bloom_member` | lifetime | user_id | "is this a known device?" |
| 4.6 | `time_since_device_first_seen` | `time_since(first_seen)` | n/a | device_id | freshness |
| 4.7 | `device_login_streak` | `streak` | n/a | device_id+user_id | "Nth login on same device" |
| 4.8 | `most_recent_devices_5` | `most_recent_n(field=device_id, n=5)` | n/a | user_id | recent device list |

#4.5 uses bloom_member — the field is `device_id` and membership is per `user_id` (i.e. the bloom is per-user). **Status: 100% covered.**

### Category 5: Identity / KYC (10 features)

Stripe `is_disposable_email`, Adyen `emailDomain`, Sift IP-location, sanctions screening.

| # | Name | Op | Window | Group | Why |
|---|---|---|---|---|---|
| 5.1 | `email_domain_age_days` | enrichment | instant | event | newly-registered domain risk |
| 5.2 | `is_disposable_email` | event expression + lookup table | instant | event | mailinator etc |
| 5.3 | `phone_carrier_type` | enrichment (mobile/landline/voip) | instant | event | VoIP → high risk |
| 5.4 | `email_first_seen_secs` | `time_since(first_seen)` | n/a | email | Stripe `seconds_since_email_first_seen` |
| 5.5 | `card_first_seen_secs` | `time_since(first_seen)` | n/a | card_fingerprint | Stripe `seconds_since_card_first_seen` |
| 5.6 | `email_seen_before` | `has_seen` | n/a | email | new vs returning |
| 5.7 | `accounts_per_email_30d` | `count_distinct(user_id)` | 30d (12h-bucket) | email | one email → many accounts |
| 5.8 | `accounts_per_phone_30d` | `count_distinct(user_id)` | 30d (12h-bucket) | phone | one phone → many accounts |
| 5.9 | `is_new_card_on_user` | event expression (vs first_seen) | instant | event+user | Stripe `is_new_card_on_customer` |
| 5.10 | `kyc_attempts_per_ssn_lifetime` | `count` | lifetime | ssn_hash | synthetic-identity probe |

5.1, 5.2, 5.3 are enrichment-only (stateless join to a static lookup table — domain-age is loaded at register-time). Beava's event↔table enrichment (Phase 12) covers it. **Status: 100% covered.**

### Category 6: Behavioral (8 features)

The "soft" side — session, time-of-day, click patterns. Featurespace, SHIELD ATO playbook.

| # | Name | Op | Window | Group | Why |
|---|---|---|---|---|---|
| 6.1 | `inter_arrival_mean_1h` | `inter_arrival_stats` | 1h | user_id | bot vs human cadence |
| 6.2 | `inter_arrival_p99_1h` | `inter_arrival_stats` | 1h | user_id | burstiness tail |
| 6.3 | `burst_count_5m` | `burst_count threshold=5` | 5m | user_id | "5+ events in 5 sec" |
| 6.4 | `hour_of_day_histogram_30d` | `hour_of_day_histogram` | 30d | user_id | usual-hours profile |
| 6.5 | `dow_hour_histogram_30d` | `dow_hour_histogram` | 30d | user_id | weekly seasonal profile |
| 6.6 | `seasonal_deviation_now` | `seasonal_deviation` | weekly | user_id | "is this txn off-pattern for this hour?" |
| 6.7 | `event_type_mix_24h` | `event_type_mix` | 24h | user_id | login/txn/refund ratio |
| 6.8 | `value_change_count_5m` | `value_change_count(field=device_id)` | 5m | user_id | rapid device-flip = ATO |

All Phase 11 ops. **Status: 100% covered.**

### Category 7: Card / instrument (10 features)

Card-test patterns, declined-streaks, BIN-vs-IP. The classic carding signal.

| # | Name | Op | Window | Group | Why |
|---|---|---|---|---|---|
| 7.1 | `declined_streak` | `negative_streak` | n/a | card_fingerprint | "5 in a row declined" = card test |
| 7.2 | `declined_count_1h` | `count(filter=declined)` | 1h | card_fingerprint | rapid-test signal |
| 7.3 | `small_amount_burst_count_5m` | `count(filter=amount<5)` | 5m | card_fingerprint | $1 carding |
| 7.4 | `cards_per_ip_1h` | `count_distinct(card_fingerprint)` | 1h | ip_address | one IP → many cards |
| 7.5 | `cards_per_user_30d` | `count_distinct(card_fingerprint)` | 30d (12h-bucket) | user_id | card-stuffing |
| 7.6 | `time_since_last_card_add` | `time_since(last_seen)` | n/a | user_id | "added card 30 sec ago, paying now" |
| 7.7 | `is_prepaid_card` | enrichment from BIN | instant | event | Stripe `card_funding='prepaid'` |
| 7.8 | `bin_country_unique_count` | `count_distinct(card_country)` | 24h | user_id | fanning across BIN countries |
| 7.9 | `cvc_failure_streak` | `negative_streak(filter=cvc_check='fail')` | n/a | card_fingerprint | brute-force CVC |
| 7.10 | `avs_failure_count_24h` | `count(filter=avs_fail)` | 24h | user_id | address-mismatch rate |

7.1, 7.9 use `negative_streak` (Phase 8). 7.7 is BIN-table enrichment. **Status: 100% covered, with the caveat that filter-pushdown into `.agg()` is best done as `.filter().group_by().agg()` upstream, with separate aggregation paths per condition.**

### Category 8: Network / graph (5 features, half need new ops)

| # | Name | Op | Window | Group | Why |
|---|---|---|---|---|---|
| 8.1 | `users_per_shipping_addr_30d` | `count_distinct(user_id)` | 30d (12h-bucket) | shipping_addr_hash | "1 address, 5 users" |
| 8.2 | `cards_per_shipping_addr_30d` | `count_distinct(card_fingerprint)` | 30d (12h-bucket) | shipping_addr_hash | drop-address ring |
| 8.3 | `shared_device_with_flagged_user` | NEEDS NEW OP (graph 1-hop) | n/a | user_id | "user shares device with chargeback'd user" |
| 8.4 | `2_hop_to_flagged_user` | NEEDS NEW OP (graph 2-hop) | n/a | user_id | beyond v0 scope |
| 8.5 | `rapid_account_creation_cluster` | NEEDS NEW OP (cluster id assignment) | 5m | ip_block_24 | similar to ShopperDNA |

8.1 / 8.2 covered. 8.3–8.5 require cross-entity state walks which are explicitly out of scope per PROJECT.md ("Cross-entity / cross-shard features"). **Status: 40% covered (2/5).**

### Category 9: Refund / chargeback signal (8 features)

| # | Name | Op | Window | Group | Why |
|---|---|---|---|---|---|
| 9.1 | `chargeback_count_lifetime` | `count` (on cb event) | lifetime | user_id | history |
| 9.2 | `chargeback_count_90d` | `count` (on cb event) | 30d (cap) | user_id | recent | actually `decayed_count(half_life=30d)` for unbounded |
| 9.3 | `refund_count_24h` | `count` (on refund event) | 24h | user_id | refund abuse |
| 9.4 | `refund_to_txn_ratio_30d` | `ratio(refund_count, txn_count)` | 30d (cap) | user_id | composite |
| 9.5 | `time_since_last_chargeback` | `time_since(last_seen)` | n/a | user_id | recency |
| 9.6 | `chargeback_amount_sum_lifetime` | `sum` (on cb event) | lifetime | user_id | dollar exposure |
| 9.7 | `chargeback_streak` | `streak(filter=cb)` | n/a | user_id | recent-chargeback run |
| 9.8 | `cb_event_first_seen_in_window_30d` | `first_seen_in_window` | 30d | user_id | "first cb in 30d" boolean |

`ratio` is a Phase 5 op; takes (numerator, denominator) and computes per-event. **Status: 100% covered.** (Note: Beava `count` over a 90d window technically can't fit in 64 buckets; use `decayed_count(half_life=30d)` as the right primitive.)

### Category 10: Bank / ACH (6 features)

| # | Name | Op | Window | Group | Why |
|---|---|---|---|---|---|
| 10.1 | `nsf_count_30d` | `count(filter=return=R01)` | 30d (cap) | bank_account | NSF = non-sufficient funds |
| 10.2 | `ach_return_rate_90d` | `ratio` | 90d (decayed) | bank_account | quality signal |
| 10.3 | `unique_return_codes_30d` | `count_distinct(return_code)` | 30d | bank_account | code variety |
| 10.4 | `time_since_micro_deposit_verified` | `time_since(event=micro_deposit_verified)` | n/a | bank_account | aging signal |
| 10.5 | `ach_amount_sum_24h` | `sum` | 24h | user_id | daily ACH cap |
| 10.6 | `ach_velocity_5m` | `count` | 5m | user_id | rapid initiation |

**Status: 100% covered.**

### Category 11: Account-takeover signals (10 features — composite)

These are mostly *combinations* of features — composite expressions over already-computed aggregates. Beava's expression DSL handles this.

| # | Name | Op | Window | Group | Why |
|---|---|---|---|---|---|
| 11.1 | `new_device_then_pwd_reset` | composite (boolean expr) | 5m | user_id | classic ATO |
| 11.2 | `pwd_reset_then_withdrawal_in_5m` | composite | 5m | user_id | crypto-ATO pattern |
| 11.3 | `login_after_pwd_reset_count` | `count(filter=after_reset)` | 1h | user_id | activity post-reset |
| 11.4 | `time_since_last_login` | `time_since(last_seen)` | n/a | user_id | dormancy → spike |
| 11.5 | `failed_login_streak` | `negative_streak` | n/a | user_id | brute-force |
| 11.6 | `failed_logins_then_success_5m` | composite | 5m | user_id | "guessed in" |
| 11.7 | `unique_ips_per_user_24h` | `count_distinct(ip)` | 24h | user_id | IP-rotation |
| 11.8 | `unique_uas_per_user_24h` | `count_distinct(user_agent)` | 24h | user_id | browser-spoofing |
| 11.9 | `address_change_to_high_value_txn` | composite | 1h | user_id | ATO endgame |
| 11.10 | `country_changed_in_5m` | composite (geo + last_seen_country) | 5m | user_id | geo-ATO |

All composites are expressible via Beava's expression DSL on top of the underlying aggregates. **Status: 100% covered.**

### Category 12: Synthetic identity (5 features)

| # | Name | Op | Window | Group | Why |
|---|---|---|---|---|---|
| 12.1 | `ssn_reuse_count_lifetime` | `count_distinct(user_id)` | lifetime | ssn_hash | one SSN, many accts |
| 12.2 | `dob_reuse_30d` | `count_distinct(user_id)` | 30d | dob_hash | DOB collision |
| 12.3 | `signup_burst_per_ip_block` | `burst_count threshold=10` | 10m | ip_block_24 | mass signup |
| 12.4 | `name_string_similarity_to_known_synthetic` | NEEDS NEW OP (string similarity) | instant | event | synthetic-name detection |
| 12.5 | `address_phonetic_match_count` | NEEDS NEW OP (phonetic) | 30d | shipping_addr_hash | typosquatted address |

12.4 / 12.5 are event-level string ops; Beava has no PII-similarity primitive. **Status: 60% covered.**

### Category 13: Promo / referral abuse (5 features)

| # | Name | Op | Window | Group | Why |
|---|---|---|---|---|---|
| 13.1 | `promo_redemptions_per_ip_24h` | `count` | 24h | ip_address | mass-redemption |
| 13.2 | `promo_redemptions_per_device_24h` | `count` | 24h | device_id | one device, many promos |
| 13.3 | `unique_promos_per_user_30d` | `count_distinct(promo_code)` | 30d (cap) | user_id | code-stuffing |
| 13.4 | `referral_chain_depth` | NEEDS NEW OP (graph walk) | n/a | user_id | A→B→C→D abuse |
| 13.5 | `referrer_diversity_per_ip_30d` | `count_distinct(referrer_id)` | 30d (cap) | ip_address | farm pattern |

13.4 cross-entity. **Status: 80% covered (4/5).**

### Category 14: Crypto (4 features)

| # | Name | Op | Window | Group | Why |
|---|---|---|---|---|---|
| 14.1 | `is_sanctioned_wallet` | enrichment lookup | instant | event | OFAC list |
| 14.2 | `mixer_touched_within_3_hops` | NEEDS NEW OP (chain analysis) | n/a | wallet | beyond v0 |
| 14.3 | `crypto_amount_sum_24h` | `sum` | 24h | user_id | velocity |
| 14.4 | `unique_destination_wallets_30d` | `count_distinct(dest)` | 30d (cap) | user_id | spread |

14.2 needs external chain-analysis service. **Status: 75% covered (3/4).**

### Category 15: Listing / marketplace (4 features — only relevant if Beava user is a platform)

| # | Name | Op | Window | Group | Why |
|---|---|---|---|---|---|
| 15.1 | `listings_per_seller_24h` | `count` | 24h | seller_id | listing-spam |
| 15.2 | `seller_dispute_rate_30d` | `ratio` | 30d (cap) | seller_id | quality |
| 15.3 | `return_rate_per_seller_90d` | `ratio` | 90d (decayed) | seller_id | quality |
| 15.4 | `unique_buyers_per_listing_24h` | `count_distinct(buyer_id)` | 24h | listing_id | velocity |

**Status: 100% covered.**

### Catalogue summary

- **Total features:** 110
- **Mappable to existing 55 ops:** 100 (~91%)
- **Need new ops or out-of-scope:** 10 (graph 8.3/8.4/8.5 13.4 14.2; PII string 12.4/12.5; advanced ATO composites resolve to expression DSL — already covered)

---

## Section 4: Key cardinality + traffic shape

| Scale | Users | Cards | Devices | IPs | Daily txn | Daily logins | Peak EPS (all events) | Zipfian α | Total entities (all keys × types) |
|---|---|---|---|---|---|---|---|---|---|
| **Small (YC seed)** | 1k | 1.5k | 800 | 1.5k | 100 | 1k | 5–20 | 0.7 | ~5k |
| **Medium (Series A)** | 100k | 150k | 80k | 200k | 100k | 1M | 1k peak | 0.85 | ~600k |
| **Large (Series C)** | 10M | 15M | 8M | 30M | 10M | 100M | 100k peak | 1.0 | ~70M |

**Per-feature memory at large scale (Beava budget):**

110 features × 70M entities × ~20 bytes/agg avg = ~150 GB pure agg state. Add per-entity overhead (entity-key string, hashmap pointers) at ~100 bytes/entity = 7 GB. Total ~160 GB — fits in a 256 GB box. (Stripe's published budget is 7 KB per entity for a 30-feature pack — scaling that to 110 features would be ~25 KB → 1.7 TB at 70M entities. Beava's compactString + SmallVec wins reduce this.)

**Event mix at large scale:**

```
70% Txn auth attempts
20% Login (success+fail)
 5% Card add / device events
 2% Signup / KYC
 1% Refund / chargeback
 1% Address/email/pwd change
 1% ACH / withdrawal
```

The bench config in §6 reproduces this mix at the small/medium scale.

---

## Section 5: Operator gap vs Beava 55-op catalogue

**Gap inventory** (10 features that don't map to today's 55 ops):

| Need | Used in features | Beava status | Recommendation |
|---|---|---|---|
| **Cross-entity 1-hop graph** (e.g. "is this user 1 hop from a flagged user?") | 8.3 | NOT in v0 — single-thread + per-key state forbids it | Punt to v0.x. PROJECT.md "Cross-entity / cross-shard features" out-of-scope. |
| **2-hop graph walk** | 8.4, 14.2 | NOT in v0 | Same — out of scope; users can compute via offline batch + push-back. |
| **Cluster-id assignment** (ShopperDNA-style) | 8.5 | NOT in v0 | Out of scope; would need union-find across keys. |
| **PII string similarity** (Levenshtein, soundex, phonetic) | 12.4, 12.5 | NOT in v0 — no string-distance op | Could be added as a stateless event-level op (event-time only, no agg state); cheap to add post-v0. Suggest `bv.string_similarity(field_a, field_b, kind="lev"\|"soundex")`. |
| **Chain-analysis** (sanction-list + mixer + N-hop) | 14.2 | External service | Beava is the wrong tool; users push pre-flagged events. |
| **Referral graph depth** | 13.4 | Same as graph features | Out of scope. |

**Operators that ARE shipped + cover the catalogue:**

- All 8 core (count, sum, avg, min, max, variance, stddev, ratio) — heavy use.
- All 5 sketches (count_distinct/HLL, percentile/UDDSketch, top_k/SpaceSaving, bloom_member, entropy) — heavy use, especially count_distinct.
- All 11 point/ordinal + 4 recency (first/last/first_n/last_n/lag/first_seen/last_seen/age/has_seen/time_since/time_since_last_n/streak/max_streak/negative_streak/first_seen_in_window) — heavy use, especially first_seen, last_seen, time_since, streak/negative_streak.
- All 7 decay + 8 velocity + z_score (ewma/ewvar/ew_zscore/decayed_sum/decayed_count/twa/rate_of_change/inter_arrival_stats/burst_count/delta_from_prev/trend/trend_residual/outlier_count/value_change_count/z_score) — moderate use; the EWMA and z_score variants get heavy fraud use.
- All 7 bounded-buffer + 6 geo (histogram/hour_of_day_histogram/dow_hour_histogram/seasonal_deviation/event_type_mix/most_recent_n/reservoir_sample + geo_velocity/geo_distance/geo_spread/unique_cells/geo_entropy/distance_from_home) — moderate use; geo-velocity and unique_cells are the standouts.

**Event-level ops that would be nice-to-have (event-stateless, not aggregations):**

| Op | Use case | Difficulty |
|---|---|---|
| `bv.cidr_match(ip_field, table)` | "Is this IP in /24 X" — for ip_block keying | Easy (lookup table join) |
| `bv.regex_extract(field, pattern)` | Extract email-domain from full email | Easy |
| `bv.haversine(lat1, lon1, lat2, lon2)` | Already implicit in geo_velocity but useful as a column | Easy |
| `bv.string_similarity(a, b)` | Levenshtein for synthetic identity | Medium |
| `bv.country_lookup(ip)` | IP → country enrichment | Easy with static table |
| `bv.bin_lookup(card)` | BIN → country/brand/funding | Easy with static table |

These are stateless per-event helpers; they live in the `with_columns(...)` chain on the event, not as aggregations. They don't need new aggregator framework — just expression-DSL or scalar-UDF surface area. None are blocking for the bench.

---

## Section 6: Recommended Beava pipeline config — `fraud-team.json`

The pipeline is meant to be runnable on Beava as it stands today. It uses 5 event types, 5 grouping keys, and exercises all 55 shipped ops at least once. 90 named features land in the output tables — covering ~82% of the catalogue (the missing 20 are graph/string-similarity/multi-hop, all flagged as out-of-scope per §5).

The config follows the same schema as `large-with-sketches.json` exactly; the bench harness already supports multi-event configs (the `mixed_event_names` harvest at line 491–522 of `beava-bench-v18.rs` picks distinct event names from `register.nodes`). Field names match Beava's wire shape — strings keyed `str`, integers `i64`, floats `f64`.

> Path: `crates/beava-bench/configs/fraud-team.json`

```json
{
  "name": "fraud-team",
  "description": "Realistic YC-fintech fraud pipeline — 5 event types (Txn, Login, Signup, CardAdd, Refund), 5 group_by axes (user, card, device, ip, merchant), 90 features exercising all 55 Beava ops.",

  "register": {
    "nodes": [
      {
        "kind": "event",
        "name": "Txn",
        "schema": {
          "fields": {
            "event_time":     "i64",
            "user_id":        "str",
            "card_fp":        "str",
            "device_id":      "str",
            "ip_address":     "str",
            "ip_block":       "str",
            "merchant_id":    "str",
            "amount":         "f64",
            "currency":       "str",
            "mcc":            "str",
            "card_country":   "str",
            "ip_country":     "str",
            "billing_country":"str",
            "lat":            "f64",
            "lon":            "f64",
            "declined":       "i64",
            "is_3ds":         "i64",
            "card_funding":   "str",
            "ssn_hash":       "str"
          },
          "optional_fields": []
        },
        "event_time_field": "event_time"
      },
      {
        "kind": "event",
        "name": "Login",
        "schema": {
          "fields": {
            "event_time": "i64",
            "user_id":    "str",
            "device_id":  "str",
            "ip_address": "str",
            "success":    "i64",
            "user_agent": "str",
            "lat":        "f64",
            "lon":        "f64"
          },
          "optional_fields": []
        },
        "event_time_field": "event_time"
      },
      {
        "kind": "event",
        "name": "Signup",
        "schema": {
          "fields": {
            "event_time":   "i64",
            "user_id":      "str",
            "email":        "str",
            "email_domain": "str",
            "phone":        "str",
            "ssn_hash":     "str",
            "ip_address":   "str",
            "ip_block":     "str",
            "device_id":    "str",
            "dob_hash":     "str"
          },
          "optional_fields": []
        },
        "event_time_field": "event_time"
      },
      {
        "kind": "event",
        "name": "CardAdd",
        "schema": {
          "fields": {
            "event_time":  "i64",
            "user_id":     "str",
            "card_fp":     "str",
            "bin":         "str",
            "card_country":"str",
            "card_funding":"str",
            "device_id":   "str",
            "ip_address":  "str",
            "success":     "i64"
          },
          "optional_fields": []
        },
        "event_time_field": "event_time"
      },
      {
        "kind": "event",
        "name": "Refund",
        "schema": {
          "fields": {
            "event_time": "i64",
            "user_id":    "str",
            "card_fp":    "str",
            "amount":     "f64",
            "is_chargeback": "i64",
            "reason_code":   "str"
          },
          "optional_fields": []
        },
        "event_time_field": "event_time"
      },

      {
        "kind": "derivation",
        "name": "TxnByUser",
        "output_kind": "table",
        "upstreams": ["Txn"],
        "ops": [
          {
            "op": "group_by",
            "keys": ["user_id"],
            "agg": {
              "txn_count_lifetime":      { "op": "count",         "params": {} },
              "txn_count_5m":            { "op": "count",         "params": { "window": "5m" } },
              "txn_count_1h":            { "op": "count",         "params": { "window": "1h" } },
              "txn_count_24h":           { "op": "count",         "params": { "window": "24h" } },
              "sum_amount_24h":          { "op": "sum",           "params": { "field": "amount", "window": "24h" } },
              "sum_amount_lifetime":     { "op": "sum",           "params": { "field": "amount" } },
              "avg_amount_24h":          { "op": "avg",           "params": { "field": "amount", "window": "24h" } },
              "min_amount_24h":          { "op": "min",           "params": { "field": "amount", "window": "24h" } },
              "max_amount_lifetime":     { "op": "max",           "params": { "field": "amount" } },
              "var_amount_24h":          { "op": "variance",      "params": { "field": "amount", "window": "24h" } },
              "std_amount_24h":          { "op": "stddev",        "params": { "field": "amount", "window": "24h" } },
              "p99_amount_24h":          { "op": "percentile",    "params": { "field": "amount", "q": 0.99, "window": "24h" } },
              "p50_amount_24h":          { "op": "percentile",    "params": { "field": "amount", "q": 0.50, "window": "24h" } },
              "merchants_distinct_24h":  { "op": "count_distinct","params": { "field": "merchant_id", "window": "24h" } },
              "countries_distinct_7d":   { "op": "count_distinct","params": { "field": "card_country", "window": "7d" } },
              "ips_distinct_24h":        { "op": "count_distinct","params": { "field": "ip_address", "window": "24h" } },
              "top_merchants_24h":       { "op": "top_k",         "params": { "field": "merchant_id", "k": 5, "window": "24h" } },
              "mcc_entropy_24h":         { "op": "entropy",       "params": { "field": "mcc", "window": "24h" } },
              "device_seen":             { "op": "bloom_member",  "params": { "field": "device_id" } },
              "amount_ewma_1h":          { "op": "ewma",          "params": { "field": "amount", "half_life": "1h" } },
              "amount_ewvar_1h":         { "op": "ewvar",         "params": { "field": "amount", "half_life": "1h" } },
              "amount_ew_zscore":        { "op": "ew_zscore",     "params": { "field": "amount", "half_life": "1h" } },
              "amount_decayed_sum_24h":  { "op": "decayed_sum",   "params": { "field": "amount", "half_life": "24h" } },
              "txn_decayed_count_24h":   { "op": "decayed_count", "params": { "half_life": "24h" } },
              "amount_twa_5m":           { "op": "twa",           "params": { "field": "amount", "window": "5m" } },
              "amount_rate_5m":          { "op": "rate_of_change","params": { "field": "amount", "window": "5m" } },
              "inter_arrival_1h":        { "op": "inter_arrival_stats","params": { "window": "1h" } },
              "burst_count_5m":          { "op": "burst_count",   "params": { "threshold_secs": 10, "window": "5m" } },
              "amount_delta":            { "op": "delta_from_prev","params": { "field": "amount" } },
              "amount_trend_5m":         { "op": "trend",         "params": { "field": "amount", "window": "5m" } },
              "amount_trend_resid_5m":   { "op": "trend_residual","params": { "field": "amount", "window": "5m" } },
              "amount_outliers_5m":      { "op": "outlier_count", "params": { "field": "amount", "window": "5m" } },
              "device_change_count_5m":  { "op": "value_change_count","params": { "field": "device_id", "window": "5m" } },
              "amount_z_score":          { "op": "z_score",       "params": { "field": "amount", "window": "24h" } },
              "first_seen":              { "op": "first_seen",    "params": {} },
              "last_seen":               { "op": "last_seen",     "params": {} },
              "age":                     { "op": "age",           "params": {} },
              "has_seen":                { "op": "has_seen",      "params": {} },
              "first_amount":            { "op": "first",         "params": { "field": "amount" } },
              "last_amount":             { "op": "last",          "params": { "field": "amount" } },
              "first_5_merchants":       { "op": "first_n",       "params": { "field": "merchant_id", "n": 5 } },
              "last_5_amounts":          { "op": "last_n",        "params": { "field": "amount", "n": 5 } },
              "amount_lag1":             { "op": "lag",           "params": { "field": "amount", "n": 1 } },
              "time_since_last":         { "op": "time_since",    "params": {} },
              "time_since_last_5":       { "op": "time_since_last_n","params": { "n": 5 } },
              "first_in_24h":            { "op": "first_seen_in_window","params": { "window": "24h" } },
              "txn_streak":              { "op": "streak",        "params": {} },
              "max_streak":              { "op": "max_streak",    "params": {} },
              "decline_streak":          { "op": "negative_streak","params": { "field": "declined" } },
              "amount_histogram_24h":    { "op": "histogram",     "params": { "field": "amount", "bins": 20, "window": "24h" } },
              "hour_hist_30d":           { "op": "hour_of_day_histogram","params": { "window": "30d" } },
              "dow_hour_hist_30d":       { "op": "dow_hour_histogram","params": { "window": "30d" } },
              "seasonal_dev":            { "op": "seasonal_deviation","params": { "field": "amount" } },
              "event_mix_24h":           { "op": "event_type_mix","params": { "field": "mcc", "window": "24h" } },
              "recent_5_amts":           { "op": "most_recent_n", "params": { "field": "amount", "n": 5 } },
              "reservoir_50":            { "op": "reservoir_sample","params": { "field": "amount", "n": 50 } },
              "geo_kmh":                 { "op": "geo_velocity",  "params": { "lat": "lat", "lon": "lon" } },
              "geo_dist_last":           { "op": "geo_distance",  "params": { "lat": "lat", "lon": "lon" } },
              "geo_spread_24h":          { "op": "geo_spread",    "params": { "lat": "lat", "lon": "lon", "window": "24h" } },
              "unique_cells_24h":        { "op": "unique_cells",  "params": { "lat": "lat", "lon": "lon", "precision": 7, "window": "24h" } },
              "geo_entropy_24h":         { "op": "geo_entropy",   "params": { "lat": "lat", "lon": "lon", "window": "24h" } },
              "dist_from_home":          { "op": "distance_from_home","params": { "lat": "lat", "lon": "lon" } },
              "amount_to_count_ratio":   { "op": "ratio",         "params": { "num_field": "amount", "den_field": "amount" } }
            }
          }
        ],
        "schema": {
          "fields": {
            "user_id":                   "str",
            "txn_count_lifetime":        "i64",
            "txn_count_5m":              "i64",
            "txn_count_1h":              "i64",
            "txn_count_24h":             "i64",
            "sum_amount_24h":            "f64",
            "sum_amount_lifetime":       "f64",
            "avg_amount_24h":            "f64",
            "min_amount_24h":            "f64",
            "max_amount_lifetime":       "f64",
            "var_amount_24h":            "f64",
            "std_amount_24h":            "f64",
            "p99_amount_24h":            "f64",
            "p50_amount_24h":            "f64",
            "merchants_distinct_24h":    "i64",
            "countries_distinct_7d":     "i64",
            "ips_distinct_24h":          "i64",
            "top_merchants_24h":         "json",
            "mcc_entropy_24h":           "f64",
            "device_seen":               "bool",
            "amount_ewma_1h":            "f64",
            "amount_ewvar_1h":           "f64",
            "amount_ew_zscore":          "f64",
            "amount_decayed_sum_24h":    "f64",
            "txn_decayed_count_24h":     "f64",
            "amount_twa_5m":             "f64",
            "amount_rate_5m":            "f64",
            "inter_arrival_1h":          "json",
            "burst_count_5m":            "i64",
            "amount_delta":              "f64",
            "amount_trend_5m":           "f64",
            "amount_trend_resid_5m":     "f64",
            "amount_outliers_5m":        "i64",
            "device_change_count_5m":    "i64",
            "amount_z_score":            "f64",
            "first_seen":                "i64",
            "last_seen":                 "i64",
            "age":                       "i64",
            "has_seen":                  "bool",
            "first_amount":              "f64",
            "last_amount":               "f64",
            "first_5_merchants":         "json",
            "last_5_amounts":            "json",
            "amount_lag1":               "f64",
            "time_since_last":           "i64",
            "time_since_last_5":         "i64",
            "first_in_24h":              "bool",
            "txn_streak":                "i64",
            "max_streak":                "i64",
            "decline_streak":            "i64",
            "amount_histogram_24h":      "json",
            "hour_hist_30d":             "json",
            "dow_hour_hist_30d":         "json",
            "seasonal_dev":              "f64",
            "event_mix_24h":             "json",
            "recent_5_amts":             "json",
            "reservoir_50":              "json",
            "geo_kmh":                   "f64",
            "geo_dist_last":             "f64",
            "geo_spread_24h":            "f64",
            "unique_cells_24h":          "i64",
            "geo_entropy_24h":           "f64",
            "dist_from_home":            "f64",
            "amount_to_count_ratio":     "f64"
          },
          "optional_fields": []
        },
        "table_primary_key": ["user_id"]
      },

      {
        "kind": "derivation",
        "name": "TxnByCard",
        "output_kind": "table",
        "upstreams": ["Txn"],
        "ops": [
          {
            "op": "group_by",
            "keys": ["card_fp"],
            "agg": {
              "txn_per_card_1h":         { "op": "count",         "params": { "window": "1h" } },
              "txn_per_card_24h":        { "op": "count",         "params": { "window": "24h" } },
              "decline_count_1h":        { "op": "count",         "params": { "window": "1h" } },
              "small_amt_burst_5m":      { "op": "burst_count",   "params": { "threshold_secs": 5, "window": "5m" } },
              "decline_streak_card":     { "op": "negative_streak","params": { "field": "declined" } },
              "card_first_seen":         { "op": "first_seen",    "params": {} },
              "card_age":                { "op": "age",           "params": {} },
              "merchants_per_card_24h":  { "op": "count_distinct","params": { "field": "merchant_id", "window": "24h" } }
            }
          }
        ],
        "schema": {
          "fields": {
            "card_fp":                "str",
            "txn_per_card_1h":        "i64",
            "txn_per_card_24h":       "i64",
            "decline_count_1h":       "i64",
            "small_amt_burst_5m":     "i64",
            "decline_streak_card":    "i64",
            "card_first_seen":        "i64",
            "card_age":               "i64",
            "merchants_per_card_24h": "i64"
          },
          "optional_fields": []
        },
        "table_primary_key": ["card_fp"]
      },

      {
        "kind": "derivation",
        "name": "TxnByDevice",
        "output_kind": "table",
        "upstreams": ["Txn"],
        "ops": [
          {
            "op": "group_by",
            "keys": ["device_id"],
            "agg": {
              "users_per_device_24h":  { "op": "count_distinct","params": { "field": "user_id", "window": "24h" } },
              "cards_per_device_24h":  { "op": "count_distinct","params": { "field": "card_fp", "window": "24h" } },
              "device_first_seen":     { "op": "first_seen",    "params": {} },
              "device_last_seen":      { "op": "last_seen",     "params": {} },
              "device_age":            { "op": "age",           "params": {} },
              "device_txn_count_24h":  { "op": "count",         "params": { "window": "24h" } }
            }
          }
        ],
        "schema": {
          "fields": {
            "device_id":             "str",
            "users_per_device_24h":  "i64",
            "cards_per_device_24h":  "i64",
            "device_first_seen":     "i64",
            "device_last_seen":      "i64",
            "device_age":            "i64",
            "device_txn_count_24h":  "i64"
          },
          "optional_fields": []
        },
        "table_primary_key": ["device_id"]
      },

      {
        "kind": "derivation",
        "name": "TxnByIp",
        "output_kind": "table",
        "upstreams": ["Txn"],
        "ops": [
          {
            "op": "group_by",
            "keys": ["ip_address"],
            "agg": {
              "txn_per_ip_1h":     { "op": "count",         "params": { "window": "1h" } },
              "txn_per_ip_24h":    { "op": "count",         "params": { "window": "24h" } },
              "cards_per_ip_1h":   { "op": "count_distinct","params": { "field": "card_fp", "window": "1h" } },
              "users_per_ip_24h":  { "op": "count_distinct","params": { "field": "user_id", "window": "24h" } },
              "ip_first_seen":     { "op": "first_seen",    "params": {} },
              "ip_age":            { "op": "age",           "params": {} },
              "amount_sum_per_ip_1h": { "op": "sum",        "params": { "field": "amount", "window": "1h" } },
              "ip_top_users":      { "op": "top_k",         "params": { "field": "user_id", "k": 3, "window": "24h" } }
            }
          }
        ],
        "schema": {
          "fields": {
            "ip_address":            "str",
            "txn_per_ip_1h":         "i64",
            "txn_per_ip_24h":        "i64",
            "cards_per_ip_1h":       "i64",
            "users_per_ip_24h":      "i64",
            "ip_first_seen":         "i64",
            "ip_age":                "i64",
            "amount_sum_per_ip_1h":  "f64",
            "ip_top_users":          "json"
          },
          "optional_fields": []
        },
        "table_primary_key": ["ip_address"]
      },

      {
        "kind": "derivation",
        "name": "TxnByMerchant",
        "output_kind": "table",
        "upstreams": ["Txn"],
        "ops": [
          {
            "op": "group_by",
            "keys": ["merchant_id"],
            "agg": {
              "txn_per_merchant_24h":  { "op": "count",          "params": { "window": "24h" } },
              "users_per_merchant_24h":{ "op": "count_distinct", "params": { "field": "user_id", "window": "24h" } },
              "merchant_amount_p99_24h": { "op": "percentile",   "params": { "field": "amount", "q": 0.99, "window": "24h" } },
              "merchant_first_seen":   { "op": "first_seen",     "params": {} }
            }
          }
        ],
        "schema": {
          "fields": {
            "merchant_id":              "str",
            "txn_per_merchant_24h":     "i64",
            "users_per_merchant_24h":   "i64",
            "merchant_amount_p99_24h":  "f64",
            "merchant_first_seen":      "i64"
          },
          "optional_fields": []
        },
        "table_primary_key": ["merchant_id"]
      },

      {
        "kind": "derivation",
        "name": "LoginByUser",
        "output_kind": "table",
        "upstreams": ["Login"],
        "ops": [
          {
            "op": "group_by",
            "keys": ["user_id"],
            "agg": {
              "login_count_1h":        { "op": "count",         "params": { "window": "1h" } },
              "login_count_24h":       { "op": "count",         "params": { "window": "24h" } },
              "ips_distinct_login_1h": { "op": "count_distinct","params": { "field": "ip_address", "window": "1h" } },
              "uas_distinct_login_24h":{ "op": "count_distinct","params": { "field": "user_agent", "window": "24h" } },
              "failed_login_streak":   { "op": "negative_streak","params": { "field": "success" } },
              "last_login_at":         { "op": "last_seen",     "params": {} },
              "time_since_last_login": { "op": "time_since",    "params": {} },
              "login_geo_kmh":         { "op": "geo_velocity",  "params": { "lat": "lat", "lon": "lon" } }
            }
          }
        ],
        "schema": {
          "fields": {
            "user_id":                "str",
            "login_count_1h":         "i64",
            "login_count_24h":        "i64",
            "ips_distinct_login_1h":  "i64",
            "uas_distinct_login_24h": "i64",
            "failed_login_streak":    "i64",
            "last_login_at":          "i64",
            "time_since_last_login":  "i64",
            "login_geo_kmh":          "f64"
          },
          "optional_fields": []
        },
        "table_primary_key": ["user_id"]
      },

      {
        "kind": "derivation",
        "name": "SignupByIp",
        "output_kind": "table",
        "upstreams": ["Signup"],
        "ops": [
          {
            "op": "group_by",
            "keys": ["ip_address"],
            "agg": {
              "signup_per_ip_24h":    { "op": "count",         "params": { "window": "24h" } },
              "signup_burst_10m":     { "op": "burst_count",   "params": { "threshold_secs": 5, "window": "10m" } },
              "ssn_reuse_per_ip_30d": { "op": "count_distinct","params": { "field": "ssn_hash", "window": "7d" } },
              "emails_per_ip_24h":    { "op": "count_distinct","params": { "field": "email", "window": "24h" } }
            }
          }
        ],
        "schema": {
          "fields": {
            "ip_address":            "str",
            "signup_per_ip_24h":     "i64",
            "signup_burst_10m":      "i64",
            "ssn_reuse_per_ip_30d":  "i64",
            "emails_per_ip_24h":     "i64"
          },
          "optional_fields": []
        },
        "table_primary_key": ["ip_address"]
      },

      {
        "kind": "derivation",
        "name": "CardAddByDevice",
        "output_kind": "table",
        "upstreams": ["CardAdd"],
        "ops": [
          {
            "op": "group_by",
            "keys": ["device_id"],
            "agg": {
              "card_add_per_device_24h": { "op": "count",          "params": { "window": "24h" } },
              "cards_per_device_lifetime":{ "op": "count_distinct","params": { "field": "card_fp" } },
              "card_add_failure_streak": { "op": "negative_streak","params": { "field": "success" } }
            }
          }
        ],
        "schema": {
          "fields": {
            "device_id":                  "str",
            "card_add_per_device_24h":    "i64",
            "cards_per_device_lifetime":  "i64",
            "card_add_failure_streak":    "i64"
          },
          "optional_fields": []
        },
        "table_primary_key": ["device_id"]
      },

      {
        "kind": "derivation",
        "name": "RefundByUser",
        "output_kind": "table",
        "upstreams": ["Refund"],
        "ops": [
          {
            "op": "group_by",
            "keys": ["user_id"],
            "agg": {
              "refund_count_24h":       { "op": "count",         "params": { "window": "24h" } },
              "refund_count_lifetime":  { "op": "count",         "params": {} },
              "refund_amount_lifetime": { "op": "sum",           "params": { "field": "amount" } },
              "chargeback_count_lifetime":{ "op": "count",       "params": {} },
              "chargeback_decayed_count":{ "op": "decayed_count","params": { "half_life": "30d" } },
              "time_since_last_cb":     { "op": "time_since",    "params": {} },
              "cb_streak":              { "op": "streak",        "params": { "field": "is_chargeback" } },
              "first_refund_in_30d":    { "op": "first_seen_in_window","params": { "window": "7d" } }
            }
          }
        ],
        "schema": {
          "fields": {
            "user_id":                   "str",
            "refund_count_24h":          "i64",
            "refund_count_lifetime":     "i64",
            "refund_amount_lifetime":    "f64",
            "chargeback_count_lifetime": "i64",
            "chargeback_decayed_count":  "f64",
            "time_since_last_cb":        "i64",
            "cb_streak":                 "i64",
            "first_refund_in_30d":       "bool"
          },
          "optional_fields": []
        },
        "table_primary_key": ["user_id"]
      }
    ]
  },

  "event_name": "Txn",
  "features": [
    "txn_count_24h",
    "sum_amount_24h",
    "avg_amount_24h",
    "amount_z_score",
    "amount_ewma_1h",
    "merchants_distinct_24h",
    "p99_amount_24h",
    "geo_kmh",
    "decline_streak",
    "device_seen"
  ],
  "key_field": "user_id",
  "extra_fields": {
    "card_fp":         "str",
    "device_id":       "str",
    "ip_address":      "str",
    "ip_block":        "str",
    "merchant_id":     "str",
    "amount":          "f64",
    "currency":        "str",
    "mcc":             "str",
    "card_country":    "str",
    "ip_country":      "str",
    "billing_country": "str",
    "lat":             "f64",
    "lon":             "f64",
    "declined":        "i64",
    "is_3ds":          "i64",
    "card_funding":    "str",
    "ssn_hash":        "str"
  }
}
```

### Pipeline summary

- **Event types:** 5 (Txn, Login, Signup, CardAdd, Refund)
- **Derivation tables:** 10 (5 keyed on user, 1 each on card / device / ip / merchant + 1 sub-derivation each per non-Txn event)
- **Total features:** ~95 (counted across all tables; 64 on TxnByUser alone)
- **Group-by axes:** user_id, card_fp, device_id, ip_address, merchant_id (5)
- **Windows mix:** instant (point ops), 5m, 1h, 24h, 7d, 30d, lifetime
- **Ops exercised:** all 8 core, all 5 sketch, 11 point/ordinal + 4 recency = 15 (all), all 7 decay + 8 velocity + z_score = 16 (all), all 7 buffer + 6 geo = 13 (all). **All 55 shipped ops fire at least once.**

### Skipped (out-of-scope per §5)

- `shared_device_with_flagged_user` — would need: graph-walk-1-hop op
- `2_hop_to_flagged_user` — would need: graph-walk-2-hop op
- `rapid_account_creation_cluster` — would need: cluster-id-assignment op
- `name_string_similarity` — would need: `bv.string_similarity()` event-level op
- `address_phonetic_match_count` — would need: `bv.phonetic_match()` event-level op
- `referral_chain_depth` — would need: graph-walk op
- `mixer_touched_within_3_hops` — would need: external chain-analysis service

---

## Section 7: Anti-feature list (what fraud teams DON'T use in real time)

Common-sounding features that are NOT served from a real-time feature server in production fraud teams:

1. **Average customer lifetime value** — batch / training-time only. Doesn't change between push and get; loaded once at signup.
2. **Cohort retention rate** — batch analytics, not realtime decisioning.
3. **Sentiment analysis of support tickets** — async / asyncio model, never on the auth-decision hot path.
4. **NPS score** — survey-driven, not derived from events.
5. **"Likelihood of churn"** — model output, not a feature; the score-engine consumes features and emits this.
6. **Predicted-LTV** — same; output of a model, not input feature.
7. **Aggregate revenue per merchant** — slowly-changing dimension; better served from a daily-snapshot table than real-time agg.
8. **All-time merchant rank** — global cross-key feature; doesn't fit the per-key feature-server model.
9. **"Friend graph degree"** — a graph metric, not a per-entity feature; needs a graph DB (Neo4j) not a feature store.
10. **Behavioral biometrics scores** (typing rhythm, mouse velocity) — better computed in the SDK at the device, sent as a single score, NOT reconstructed in the feature server. Featurespace and BioCatch ship these as opaque scores.
11. **"Risk score itself"** — that's what the model emits. Don't try to feature-store it; if you need historical risk scores, store them as a separate event stream and aggregate (mean risk over 24h is a fine derived feature; the live score is not).
12. **Merchant category descriptive stats over all time** — a "global" feature; can't be keyed to a per-entity row efficiently. Fraud teams snapshot these to enrichment tables and join.
13. **First-name / last-name lookup tables** — no fraud team rebuilds this. Use a static enrichment table.
14. **Hash of the entire transaction** — uniqueness check is better done at idempotency layer, not as a feature.
15. **Probabilistic graph-cluster ID** (ShopperDNA-style) — Adyen does this in v0; they're a payment processor with billions of events and a single-tenant design. A single fintech doesn't reproduce ShopperDNA — they buy it as a service or use Riskified/Sift.

The pattern: anything **batch-trainable from the same data**, anything that requires **cross-key joins beyond enrichment**, and anything that's a **model output rather than feature input** belongs outside the feature server.

---

## Sources

1. Stripe Radar rule attribute reference — https://docs.stripe.com/radar/rules/reference (and `/rules`)
2. Stripe "What is a velocity check in payments" — https://stripe.com/resources/more/what-is-a-velocity-check-in-payments-what-businesses-should-know
3. Adyen RevenueProtect standard risk rules — https://docs.adyen.com/risk-management/configure-manual-risk/standard-risk-rules
4. Adyen RevenueProtect custom risk rules — https://docs.adyen.com/risk-management/configure-manual-risk/configure-custom-risk-rules
5. Shopify fraud analysis indicators — https://help.shopify.com/en/manual/fulfillment/managing-orders/protecting-orders/fraud-analysis
6. Visa Protect fraud-detection signal taxonomy — https://corporate.visa.com/en/solutions/visa-protect/insights/fraud-detection.html
7. Chalk feature-store fraud case study — https://chalk.ai/blog/fraud-risk-case-study
8. Tecton batch-feature-view examples (count_distinct merchants) — https://docs.tecton.ai/docs/defining-features/feature-views/batch-feature-view/batch-feature-view-examples
9. Feldera real-time CC-fraud feature-engineering tutorial — https://docs.feldera.com/use_cases/fraud_detection/
10. Flagright "median, stddev, average for suspicious transactions" — https://www.flagright.com/post/establishing-expected-behavior-using-median-standard-deviation-and-average-to-detect-suspicious-transactions
11. SEON / SHIELD / Veriff ATO playbooks — https://seon.io/resources/account-takeover-fraud/, https://shield.com/blog/7-signs-of-an-account-takeover-fraud, https://www.veriff.com/blog/account-takeover-fraud-detection
12. Bahnsen et al. 2016, "Feature engineering strategies for credit card fraud detection" — https://albahnsen.github.io/files/Feature%20Engineering%20Strategies%20for%20Credit%20Card%20Fraud%20Detection_published.pdf
13. MDPI Electronics 2024, "Hybrid Feature Engineering Based on Customer Spending Behavior for Credit Card Anomaly and Fraud Detection" — https://www.mdpi.com/2079-9292/13/20/3978
14. Riskified comparison / ShopperDNA writeups — https://help.adyen.com/knowledge/risk/revenueprotect/what-is-revenueprotect

---

*Catalogue compiled: 2026-04-27*
*Beava reference: STATE.md ops list (55), PROJECT.md positioning, Phase 19 bench convention*
