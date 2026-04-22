# Sendo Demo — Recording Runbook

Everything needed to record the 3:30 Sendo walkthrough defined in
`SENDO-DEMO.md` (English source) / `SENDO-DEMO-VI.md` (Vietnamese voiceover +
captions + email).

## What this demo shows

A Sendo Farm-style fresh-food marketplace pipeline: one stream of buyer
events (view / add_to_cart / order), three feature tables keyed by
buyer, product, and origin province, eleven features total covering
short-window trending (5 min — the perishability signal), basket value,
unique-viewer reach, and provincial demand. Close enough to a real
Sendo Farm feature set that the prospect can picture it in their stack;
far enough from OTTO fashion data that the signals actually translate.

## Prerequisites (install these yourself)

- **Docker Desktop** — https://www.docker.com/products/docker-desktop/
- **Python 3.10+** — `brew install python@3.12` or system package
- **jq** — `brew install jq` / `apt-get install jq`
- **Screen recorder** — QuickTime (macOS) or OBS Studio
- **Terminal font** — Iosevka, JetBrains Mono, or similar monospace at ≥18 pt
- **Cursor highlight** (optional) — any cursor highlighter tool

Disk: ≥ 8 GB free. RAM: ≥ 4 GB free.

## One-shot setup

From the repo root:

```bash
bash scripts/demo-sendo/setup.sh
```

| # | Step | What it does |
|---|---|---|
| 1 | Preflight | Checks docker, python ≥3.10, disk, RAM, jq |
| 2 | Image | `docker build -t beavadb/beava:latest .` |
| 3 | Venv | Creates `.venv`, installs `httpx`, `-e python/` (Beava SDK) |
| 4 | Data | Runs `generate-events.sh` — synthesizes ~2 M Sendo-Farm-flavored events |
| 5 | Smoke | Starts the container, hits `/health`, stops it |

Re-running after success takes ~30 s (image cached, dataset cached, venv reused).

### Why synthesized events, not OTTO

We considered replaying the public OTTO RecSys dataset. Two reasons we
don't:

1. **OTTO is Kaggle-only** — no direct HTTP URL. Setup would need a
   Kaggle account and `~/.kaggle/kaggle.json` per developer.
2. **OTTO is German fashion retail.** Sendo Farm is Vietnamese fresh
   food. Morning meal planning, OCOP categories, Vietnamese provinces,
   perishability-driven 5-minute trending — none of that exists in OTTO.
   Faking those on top of OTTO would be less honest than generating
   them directly.

The generator ships with plausible Vietnamese province→category weights
(Da Lat / Lam Dong heavy on leafy greens; Bac Giang / Tien Giang heavy
on fruit; Can Tho on rice), a Zipfian popularity curve, and a
time-of-day burst pattern. See `generate-events.py` for knobs.

## Pre-recording verification

```bash
bash scripts/demo-sendo/verify.sh
```

Runs the 8-point checklist from `SENDO-DEMO.md` and prints a claimed-vs-measured
table:

| Check | Claimed | Measured | Verdict |
|---|---|---|---|
| Startup < 10 s | 10s | 4s | PASS |
| Idle memory < 500 MB | 500 | 380 | PASS |
| Ingest p99 < 10 ms | 10 | 4.2 | PASS |
| Query p99 < 5 ms | 5 | 1.8 | PASS |
| ... | | | |

**If any row is FAIL, edit `SENDO-DEMO-VI.md` to the true measured number
before recording.** The honest lower number still sells the product. The
false higher number ends the conversation.

## Recording day

Three terminals, in order:

```bash
# Terminal A — Scene 2: start the server
docker run -p 6900:6900 -p 6400:6400 beavadb/beava:latest

# Terminal B — Scene 3: register the pipeline
source scripts/demo-sendo/.venv/bin/activate
python scripts/demo-sendo/pipeline.py
# prints: "3 tables · 11 features active"

# Terminal C — Scene 4: the load test
cat scripts/demo-sendo/events.jsonl | \
  python scripts/demo-sendo/beava-bench.py \
    --rate 10000 \
    --to http://localhost:6900/push-batch/events \
    --duration 60

# Terminal C (after load settles) — Scene 5: query features
# pick a high-traffic user_id that actually appears in events.jsonl:
UID=$(head -10000 scripts/demo-sendo/events.jsonl | \
      jq -r '.user_id' | sort | uniq -c | sort -rn | head -1 | awk '{print $2}')
curl http://localhost:6900/features/${UID} | jq

# watch them update live:
watch -n 1 "curl -s http://localhost:6900/features/${UID}"
```

## Recording settings

- **Resolution**: 1080p (1920×1080). Don't go above 1440p for terminal content.
- **Framerate**: 30 fps.
- **Cursor**: hide, or use a subtle highlighter.
- **Terminal theme**: dark background, high-contrast, no transparency.
- **Close everything else**: Slack, browser tabs, notifications. DND on.
- **Voiceover**: record separately in a quiet room. VN voiceover in `SENDO-DEMO-VI.md`.

## Troubleshooting

| Problem | Fix |
|---|---|
| `port 6900 already allocated` | `docker ps`, then `docker rm -f <id>` |
| `image not found: beavadb/beava:latest` | Re-run `setup.sh` — step 2 builds it |
| `generate-events.py` takes forever | Lower `TARGET_EVENTS` in the script (2M is overkill for a 60-second demo; 1M is plenty) |
| `pip install` fails on `-e python/` | `python3 -m pip install --upgrade pip`, retry |
| `verify.sh` prints FAIL rows | Numbers hold on beefier hardware; on a laptop you may need to lower `--rate` in the bench. Edit `SENDO-DEMO-VI.md` to match — do NOT record bigger numbers than you can hit. |
| Pipeline registration hangs | Check `docker logs <container>` for panics; ensure TCP port 6400 is mapped |

## What lives where

```
scripts/demo-sendo/
├── README.md              ← this file
├── setup.sh               ← one-shot prep
├── generate-events.sh     ← thin wrapper over the python generator
├── generate-events.py     ← synthesize ~2M Sendo-Farm-style events
├── pipeline.py            ← on-screen pipeline: 3 tables, 11 features
├── beava-bench.py         ← 10K EPS load generator
├── verify.sh              ← pre-record checklist runner
├── .venv/                 ← Python venv (gitignored)
└── events.jsonl           ← synthesized events, ~2M rows, ~220 MB (gitignored)

/SENDO-DEMO.md             ← English source script
/SENDO-DEMO-VI.md          ← Vietnamese voiceover + captions + email
```

## What this prep does NOT do

- Publish `beavadb/beava:latest` to Docker Hub.
- Add a `beava reload` CLI subcommand (use `pipeline.py` instead).
- Add a startup banner to the binary (show `docker logs` output instead).
- Record, edit, or send the video. That part is on you.
