# Sendo Demo Video Plan

## Context

**Target:** Head of IT at Sendo.vn (https://www.sendo.vn/)

**What Sendo is:**
- Vietnamese C2C e-commerce marketplace, subsidiary of FPT Corporation
- 10M+ buyers, 300K+ sellers
- Community/SMB/tier-2 cities focus — fighting for specialized segments against Shopee and TikTok Shop (combined 97% VN market share)
- Tech stack: microservices, gRPC + Pub/Sub, ElasticSearch + MySQL + MongoDB + Redis, React/PWA frontend
- Already invested in AI/big-data publicly; exact rec system architecture undisclosed

**What he asked for:** Just interested, wants something quick to see if it fits anywhere. Not asking for a meeting yet — asking for a video.

**What we're sending:** 3-minute screen-recorded walkthrough + short email.

**Positioning:** "Beava — real-time ML features. HTTP events in, features out. No Kafka required." Honest throughput claim: 10,000 events/sec sustained on a single binary.

**Explicitly NOT pitching:**
- Fork mechanism (save for a separate demo)
- Replacing their existing Kafka / Redis / MongoDB
- Comparisons with specific competitors

---

## Video script (target 3:30 total)

### Scene 1 — Hook (0:00–0:15)

**On screen:** Title card, 3 seconds:
```
Beava — Real-time ML features. One binary. No Kafka.
3-minute walkthrough
```
Then cut to terminal.

**Voiceover:**
> Most real-time recommendation and personalization stacks need Kafka, a stream processor, a feature store, and Redis. Beava is one binary that does all of it over HTTP. Here's what that looks like.

---

### Scene 2 — Start the server (0:15–0:40)

**On screen:** Terminal. Type slowly:
```bash
docker run -p 6900:6900 beavadb/beava:latest
```
Show the startup logs. Brief highlight on: `beava ready on :6900`, `memory: 380 MB`.

**Voiceover:**
> One command to start. No Kafka, no Redis, no Feast, no platform team. Under 400 megabytes of memory. Runs on a laptop, runs on a t3.small in production.

---

### Scene 3 — Define a feature (0:40–1:20)

**On screen:** Open `pipeline.py` in a clean editor. Fade in this code:

```python
# pipeline.py
from beava import feature

@feature(entity="user_id", window="1h")
def clicks_1h(events):
    return events.filter(type="click").count()

@feature(entity="user_id", window="24h")
def cart_adds_24h(events):
    return events.filter(type="add_to_cart").count()

@feature(entity="product_id", window="5m")
def trending_5m(events):
    return events.filter(type="view").count()
```

Hit save, show terminal: `beava reload pipeline.py` → `3 features active`.

**Voiceover (slow, deliberate):**
> Features are defined in Python. Each one says: for this entity, over this time window, compute this aggregation. No SQL. No materialized views. No Flink job. Just Python.

---

### Scene 4 — Ingest events (1:20–2:00)

**On screen:** Split terminal. Left: event sender. Right: Beava logs with live counter.

Left terminal:
```bash
# Replay 60 seconds of e-commerce events at 10,000 events/second
cat otto_events.jsonl | beava-bench --rate 10000 --to http://localhost:6900/events
```

Right terminal shows counter climbing:
```
7,200 events/sec
9,800 events/sec
10,100 events/sec
10,050 events/sec sustained
Memory: 462 MB
p99 ingest latency: 4ms
```

**Voiceover:**
> Events come in over HTTP. We're pushing 10,000 events per second sustained on a single process. Under 500 megabytes of memory. 4 millisecond p99 ingest latency. This is a single laptop — production hardware goes further.

**Caption overlay (bottom of screen):** `10,000 events/sec sustained · single binary · 462 MB RAM`

---

### Scene 5 — Serve features (2:00–2:45)

**On screen:** Terminal with HTTP queries.

```bash
curl http://localhost:6900/features/user_42
```

Response appears:
```json
{
  "clicks_1h": 14,
  "cart_adds_24h": 3,
  "_latency_ms": 2
}
```

Then show a watch command:
```bash
watch -n 1 'curl -s http://localhost:6900/features/user_42'
```

Show the numbers updating in real-time as events keep flowing. `clicks_1h` climbs: 14 → 17 → 21 → 25.

**Voiceover:**
> Query features for any entity over HTTP. 2 millisecond response time. Features update in real-time — no sync step, no cache staleness window. Point your model serving code at this URL. That's the integration.

---

### Scene 6 — What's in the box vs what's not (2:45–3:10)

**On screen:** Simple split diagram. Clean, no clutter.

```
Traditional real-time feature stack:       Beava:

  ┌──────────┐                              ┌──────────┐
  │  Kafka   │                              │          │
  └────┬─────┘                              │          │
       │                                    │  Beava   │
  ┌────▼─────┐                              │          │
  │  Flink   │                              │ (one     │
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

**Voiceover:**
> Traditional real-time feature serving requires four systems and a team to run them. Beava does all of that in one process. Already have Kafka? Beava reads from it. Don't have Kafka? You don't need it.

---

### Scene 7 — Close (3:10–3:30)

**On screen:** Simple text card, no animations:

```
Beava
Real-time features. HTTP in, HTTP out.

• 10,000 events/sec per binary (verified)
• Python-defined features
• Single process, under 500 MB RAM
• Works with your existing Kafka / Redis / Postgres
  — or without

beava.dev
hoang@beava.dev
```

**Voiceover:**
> If this looks like it could fit alongside what you already run, I'd love to do a 30-minute call. Happy to run a proof of concept on a sample of your own traffic.

**End card:** *Thanks for watching. — Hoang*

---

## Production notes

**Time budget:** half a day to record, half a day to edit.

- **Recording:** QuickTime screen recorder or OBS. Don't overthink it.
- **Voiceover:** Record separately. Slightly slower pace than conversational — the viewer may watch with captions on. Clarity over charisma.
- **Captions:** Auto-generate then clean up. Head of IT at a Vietnamese company will appreciate CC even with good English.
- **Cursor:** Hide cursor in terminal recordings, or use cursor highlight so eyes can follow.
- **Terminal:** Dark background, monospace 18pt+. He'll watch at 1x on a laptop.
- **Music:** None. Voice + terminal sounds is cleaner. Music dates the video fast.
- **Resolution:** 1080p minimum. 4K is overkill.
- **Length target:** hit 3:30 exactly. Over 4 minutes loses ~30% of viewers per minute.

**What to NOT show:**
- Fork mechanism — save for a separate demo.
- Competitor benchmarks or name-calling.
- Business pitch — no pricing, no "revolutionary," no adjectives. Let the video be the video.
- Any feature that might misbehave on stage. Rehearse 3 times before recording.

---

## Pre-recording verification checklist

Verify all of these actually hold on your recording machine. Do NOT record numbers that aren't true.

- [ ] `docker run` starts Beava cleanly in under 10 seconds
- [ ] Memory footprint under 500 MB at steady state
- [ ] `pipeline.py` reload works without restart
- [ ] 10,000 events/sec sustained ingest for 60+ seconds
- [ ] p99 ingest latency < 10ms under that load
- [ ] Query latency < 5ms p99 under that load
- [ ] `watch -n 1` query shows features updating live
- [ ] Sample OTTO events file ready at hand

If any number is worse than claimed, edit the script to show the TRUE number before recording. Lower honest numbers beat higher false ones.

**Recommended sample data:** OTTO RecSys Challenge 2022 dataset. ~200M events, sessions with clicks/carts/orders. Structurally matches Sendo's C2C marketplace flow. Source: https://github.com/otto-de/recsys-dataset

---

## Email to send with the video

Keep it short. Head of IT is busy.

**Subject:** Beava — 3-minute walkthrough as requested

> Hi [Head of IT name],
>
> Thanks for your interest. Here's a 3-minute walkthrough of Beava running on real e-commerce event data at 10,000 events per second on a single binary: [video link]
>
> The short pitch: one process replaces Kafka + stream processor + feature store + Redis for real-time feature serving. If you already have Kafka running for other things, Beava reads from it. If you don't, you don't need it.
>
> If anything looks like it could fit somewhere in Sendo's rec/ranking/personalization stack, happy to jump on a 30-min call to dig into specifics. I can also run a small proof-of-concept on a sample of your own event data if that's useful.
>
> — Hoang

That's it. No deck, no pricing, no "overview." He asked for something quick; send something quick.

---

## Sanity check before sending

Watch the video back once as the Head of IT would. Questions he'll ask himself:

1. **Does this work?** — the 10K EPS demo answers yes.
2. **Can my team operate it?** — single binary + Docker command answers yes.
3. **Does it replace things or sit alongside?** — Scene 6 diagram answers.
4. **What's the catch?** — if the video doesn't overclaim, he won't look for one.

If any of those four questions don't have a clean answer by the end, re-record. If they do, ship it.

---

## Context for fresh Claude session

If resuming work on this in a new session, the key facts to know:

- Demo is for Sendo Head of IT (Vietnamese marketplace, 10M+ buyers, FPT subsidiary)
- He's casually interested — wants to see if Beava fits. Not asking for a meeting yet.
- Beava's verified throughput is 10K EPS on a single binary (not the 315K EPS sometimes mentioned — that's TCP-specific and the demo should stick with the conservative honest number)
- Target length 3:30, bias toward 3:00 if anything feels slow
- Email the video, don't request a meeting unless he replies
- Sendo already has Redis / MongoDB / MySQL / ElasticSearch / gRPC + Pub/Sub — don't pitch replacing any of them
- Beava positioning is "HTTP events in, HTTP features out, no Kafka required (but will integrate if you have one)"
- If Beava isn't yet production-ready for this demo, fall back to private-beta / design-partner framing instead

If Beava hasn't launched publicly yet, adjust the email's video description from "running on real e-commerce event data" to "running on OTTO public dataset at 10,000 events/second" — honest and sufficient.
