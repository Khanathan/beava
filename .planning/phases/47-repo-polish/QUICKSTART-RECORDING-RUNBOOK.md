# SHIP-05: Quickstart GIF Recording Runbook

**Decision lock:** D-38 — The recording is a human-run step. This plan delivers the
runbook; the maintainer executes it on a clean VM (or local machine), commits the
resulting `docs/assets/quickstart.cast` and `docs/assets/quickstart.gif`, and embeds
the GIF in `README.md`.

**Requirement:** SHIP-05 — Record a ~60-second asciinema cast of the `docker run →
curl POST → curl GET` quickstart, convert to GIF, commit assets, link from README.

**Target files:**
- `docs/assets/quickstart.cast` — raw asciinema recording
- `docs/assets/quickstart.gif` — converted GIF (<3 MB)
- `README.md` — `<img>` tag added after title banner

---

## Prerequisites

```bash
# macOS
brew install asciinema
brew install agg   # or: cargo install agg

# Ubuntu / Debian
sudo apt-get install -y asciinema
cargo install agg  # or: pip3 install agg

# Verify
asciinema --version
agg --version   # or agg --help
```

Terminal must be at least **100×30** characters. Set it:

```bash
printf '\e[8;30;100t'   # resize terminal to 100 cols × 30 rows
# Or resize manually in your terminal emulator preferences.
```

---

## Procedure

### Step 1 — Clean environment

Ensure no stale containers or leftover state:

```bash
docker stop beava 2>/dev/null || true
docker rm beava 2>/dev/null || true
docker pull beavadb/beava:latest   # ensure freshest image
```

### Step 2 — Create assets directory

```bash
mkdir -p docs/assets
```

### Step 3 — Start the recording

```bash
asciinema rec docs/assets/quickstart.cast \
  --title "Beava 60-second quickstart" \
  --idle-time-limit 2
```

You are now recording. Type the commands EXACTLY as shown. Pause naturally; agg will
respect the `--idle-time-limit 2` cap.

**Commands to type (copy-paste one at a time):**

```
docker run -d --rm -p 6900:6900 --name beava beavadb/beava:latest
```

*(wait ~3 seconds for startup)*

```
sleep 3
```

```
curl http://localhost:6900/health
```

*(expected: `{"status":"ok"}`)*

```
curl -X POST http://localhost:6900/push/clicks \
  -H 'Content-Type: application/json' \
  -d '{"user":"alice","page":"/home"}'
```

*(expected: `{"ok":true}`)*

```
curl http://localhost:6900/features/alice | jq .
```

*(expected: JSON with feature data keyed by "alice")*

```
docker stop beava
```

Press **Ctrl-D** to stop recording.

**Target session length: 45–60 seconds.** If the recording exceeds 70 seconds, re-record
and type faster or reduce pauses. The idle-time-limit cap compresses long pauses to 2 s
automatically.

### Step 4 — Verify the cast

```bash
asciinema play docs/assets/quickstart.cast
```

Review that:
- All commands appear and complete successfully
- No error output visible
- Total playback time is 45–60 seconds

If the cast has visible errors, delete it and re-record from Step 3.

### Step 5 — Convert to GIF

```bash
agg docs/assets/quickstart.cast docs/assets/quickstart.gif \
  --cols 100 \
  --rows 30 \
  --fps-cap 30 \
  --font-size 14
```

Check file size:

```bash
ls -lh docs/assets/quickstart.gif
```

**Target: <3 MB.** If the GIF exceeds 3 MB:

```bash
# Option A — lower fps-cap
agg docs/assets/quickstart.cast docs/assets/quickstart.gif \
  --cols 100 --rows 30 --fps-cap 15 --font-size 14

# Option B — re-record at 80×24 (smaller terminal = smaller GIF)
printf '\e[8;24;80t'
# ... re-record with --cols 80 --rows 24 in both asciinema and agg
```

### Step 6 — Link from README.md

Open `README.md` and insert the following `<img>` tag **after the CI/license badge block
and before the `## 60-second quickstart` heading**:

```markdown
<img src="docs/assets/quickstart.gif" alt="Beava 60-second quickstart: docker run, push event, read feature" width="720">
```

Verify README line count stays within the <60-line target (CONTENT-01 / D-19):

```bash
wc -l README.md
```

If the count exceeds 60, compress elsewhere (e.g., collapse a blank line).

### Step 7 — Commit all three artifacts

```bash
git add docs/assets/quickstart.cast
git add docs/assets/quickstart.gif
git add README.md
git commit -m "assets(47-10): 60-second quickstart GIF + cast (SHIP-05, D-38)"
```

---

## Verification Checklist

Before committing, confirm all items pass:

- [ ] `test -f docs/assets/quickstart.cast` — cast file present
- [ ] `test -f docs/assets/quickstart.gif` — GIF file present
- [ ] `[ $(stat -c%s docs/assets/quickstart.gif) -lt 3145728 ]` — GIF <3 MB
- [ ] `asciinema play docs/assets/quickstart.cast` plays without errors
- [ ] GIF playback shows: `docker run` → `curl /health` → `curl /push/clicks` → `curl /features/alice` → `docker stop`
- [ ] `grep -q 'quickstart.gif' README.md` — README links to GIF
- [ ] `wc -l README.md` shows ≤60 lines

---

## Re-recording Note

If `README.md` commands change (port rename, endpoint path change), the recording must
be redone. The cast references the exact commands from README.md's "60-second quickstart"
block — any drift makes the recording misleading. The cast + gif are intentionally
committed as binary assets, not generated on CI.

---

## Placeholder Asset Paths (pre-recording)

Until the recording is made, these paths are documented here so that future scripts or
CI can reference them:

- `docs/assets/quickstart.cast`
- `docs/assets/quickstart.gif`

The `README.md` embed is added when the assets are committed. Do NOT add a broken
`<img>` tag before the files exist.
