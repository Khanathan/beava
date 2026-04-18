# SHIP-02: Fresh-VM Smoke Runbook

**Decision lock:** D-35 — Actual execution on a cloud VM is a human-run step. This plan
delivers the runbook; the maintainer executes it once at launch day and records results in
`SHIP-02-RESULTS.md` (template at the end of this file).

**Requirement:** SHIP-02 — end-to-end smoke on a fresh machine: install from public source →
run one example → push events via HTTP → read features → kill process → recover → confirm
data survived. Time-to-first-success recorded; target <60 seconds.

**Dependencies:** Docker Hub push must have been completed first (see
`docs/docker-publish-runbook.md`). If `beavadb/beava:latest` is not yet published, Step 3
will fail with "image not found" — complete the publish runbook before running this one.

---

## Prerequisites

- AWS, Fly.io, or Hetzner account with SSH access
- SSH keypair already configured
- `asciinema` installed locally if you want to record the session (shared with SHIP-05)

---

## Procedure

### Step 1 — Provision a fresh VM (est. 5 min)

Choose one provider. The smoke test has been validated on both.

**Option A — AWS t3.small (us-east-1):**

```bash
# Requires AWS CLI configured
aws ec2 run-instances \
  --image-id ami-0c02fb55956c7d316 \
  --instance-type t3.small \
  --key-name YOUR_KEY_NAME \
  --security-group-ids YOUR_SG_ID \
  --region us-east-1 \
  --query 'Instances[0].PublicIpAddress' \
  --output text

# Wait ~30s, then SSH
ssh -i ~/.ssh/YOUR_KEY.pem ubuntu@<IP>
```

**Option B — Fly.io shared-cpu-1x (iad):**

```bash
# Requires flyctl installed and authenticated
flyctl machine run ubuntu:22.04 \
  --region iad \
  --vm-size shared-cpu-1x \
  --shell

# You are now in the VM shell
```

**Option C — Hetzner CX21 (Falkenstein):**

```bash
hcloud server create \
  --name beava-smoke \
  --type cx21 \
  --image ubuntu-22.04 \
  --ssh-key YOUR_KEY_NAME

hcloud server ip beava-smoke
ssh root@<IP>
```

Record: VM provider, region, instance type, IP.

---

### Step 2 — Install Docker (est. 2 min)

```bash
# Standard Docker install for Ubuntu 22.04
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER

# Re-login so group membership takes effect
exec newgrp docker

# Verify
docker --version
# → Docker version 24.x.y, build ...
```

---

### Step 3 — Pull Beava and run the 60-second quickstart (LOAD-BEARING STEP)

**Start stopwatch now.**

```bash
# Pull and run Beava
docker run -d --rm -p 6900:6900 --name beava beavadb/beava:latest

# Wait for the server to initialize (3 seconds)
sleep 3

# Verify it is alive
curl http://localhost:6900/health
# → {"status":"ok"}

# Push an event
curl -X POST http://localhost:6900/push/clicks \
  -H 'Content-Type: application/json' \
  -d "{\"user\":\"alice\",\"page\":\"/home\",\"_event_time\":$(date +%s)000}"
# → {"ok":true}

# Read features back
curl http://localhost:6900/features/alice?table=clicks
# → {"ok":true,"data":{"key":"alice","tables":{"clicks":{...}}}}
```

**Stop stopwatch here.** Record wall-clock elapsed time. Target: <60 seconds from the
`docker run` command to the successful `/features/alice` response.

If the stopwatch exceeds 60 seconds, investigate the bottleneck (usually network pull time
on first run) and note it in SHIP-02-RESULTS.md.

---

### Step 4 — Run one example (est. 2 min)

Clone the repo and run the session-features example (simpler than fraud-scoring, good for
first-contact validation):

```bash
# Install git if not present
sudo apt-get install -y git python3 python3-pip

# Clone
git clone --depth 1 https://github.com/petrpan26/beava.git
cd beava

# Install Python dependencies
pip3 install -e python/

# Run session-features example
cd examples/session-features
bash run.sh
```

Expected: the script pushes synthetic click events and reads back aggregated features.
Record whether the example ran without manual edits: Y / N.

If N, note what required editing (this surfaces env / path / SDK packaging bugs).

---

### Step 5 — Kill-and-recover durability test (est. 3 min)

This tests that Beava's write-ahead log and snapshots survive a hard kill.

```bash
# Stop the --rm container from Step 3 (already gone on kill)
# Start a new container WITH a persistent volume
docker run -d -p 6900:6900 --name beava-persist \
  -v /tmp/beava-data:/data \
  beavadb/beava:latest

sleep 3

# Push 100 events to build some state
for i in $(seq 1 100); do
  curl -s -X POST http://localhost:6900/push/smoke \
    -H 'Content-Type: application/json' \
    -d "{\"user\":\"u42\",\"val\":$i,\"_event_time\":$(date +%s)000}" > /dev/null
done

# Read pre-kill feature value
echo "=== PRE-KILL VALUE ==="
curl http://localhost:6900/features/u42?table=smoke
echo ""

# Hard kill (simulates a crash or OOM kill)
docker kill beava-persist

# Wait 2s, then restart with the SAME volume
sleep 2
docker run -d -p 6900:6900 --name beava-recover \
  -v /tmp/beava-data:/data \
  beavadb/beava:latest

# Wait for recovery to complete
sleep 10

# Verify /health before reading
curl http://localhost:6900/health
# → {"status":"ok"}

# Read post-recovery feature value
echo "=== POST-RECOVERY VALUE ==="
curl http://localhost:6900/features/u42?table=smoke
echo ""
```

Expected: the `count` or equivalent aggregate field in the post-recovery response
matches the pre-kill value. Any mismatch is a P0 regression — file an issue immediately
against `src/state/event_log.rs` and check whether the Phase 46 Plan 08 ship-gate test
regressed (run `cargo test ship_gate`).

---

### Step 6 — Teardown (est. 1 min)

```bash
docker stop beava-recover 2>/dev/null || true
docker rm beava-recover 2>/dev/null || true
rm -rf /tmp/beava-data

# If on AWS: terminate the instance
# aws ec2 terminate-instances --instance-ids <ID> --region us-east-1

# If on Fly.io: destroy the machine
# flyctl machine destroy <ID>

# If on Hetzner:
# hcloud server delete beava-smoke
```

---

## Success Criteria Checklist

Complete this checklist and transcribe results to `SHIP-02-RESULTS.md`.

- [ ] **SC-1:** Step 3 completed in <60 seconds stopwatch-verified from `docker run` to
  successful `/features/alice` response.
- [ ] **SC-2:** Step 4 example (`session-features/run.sh`) ran without manual edits.
- [ ] **SC-3:** Step 5 post-recovery `/features/u42` value matched pre-kill value.

All three must be checked for SHIP-02 to be marked CLOSED.

---

## Troubleshooting Guide

| Symptom | Most Likely Cause | Fix |
|---------|------------------|-----|
| Step 3: `docker pull` fails with "not found" | Image not yet published to Docker Hub | Run `docs/docker-publish-runbook.md` first |
| Step 3: `/health` returns nothing after 10s | Container failed to start | `docker logs beava` — look for panic or port conflict |
| Step 3: `/features/alice` returns `{"tables":{}}` | Event not indexed yet | Check `docker logs beava` for ingest errors |
| Step 4: `bash run.sh` fails with Python import error | SDK not installed | `pip3 install -e python/` from repo root |
| Step 5: Post-recovery value differs from pre-kill | Potential data-loss regression | File P0 issue; run `cargo test --test ship_gate` |
| Step 5: Server never returns `{"status":"ok"}` after restart | Snapshot corrupt | Check volume contents with `ls /tmp/beava-data/` |

---

## Recording SHIP-02 Execution

After completing this runbook, produce
`.planning/phases/47-repo-polish/SHIP-02-RESULTS.md` using the template below.

---

## SHIP-02-RESULTS.md Template

```markdown
# SHIP-02 Execution Results

**Date:** YYYY-MM-DD
**Executor:** [name / GitHub handle]
**Git SHA:** [git rev-parse --short HEAD]
**VM provider / spec:** [e.g., AWS t3.small, us-east-1, Ubuntu 22.04]
**Docker image SHA:** [docker inspect beavadb/beava:latest --format '{{.Id}}']

## Step 3 — 60-second quickstart stopwatch

- Start: HH:MM:SS
- docker pull complete: HH:MM:SS (elapsed: XX s)
- /health returned ok: HH:MM:SS (elapsed: XX s)
- /features/alice returned features: HH:MM:SS (elapsed: XX s)
- **Total elapsed: XX seconds**
- **SC-1:** [ ] PASS (< 60 s)  [ ] FAIL (XX s — note reason)

## Step 4 — session-features example

- Ran without edits: [ ] Y  [ ] N
- If N, edits required: [describe]
- **SC-2:** [ ] PASS  [ ] FAIL

## Step 5 — kill-and-recover

- Pre-kill feature value:
  ```json
  [paste curl output]
  ```
- Post-recovery feature value:
  ```json
  [paste curl output]
  ```
- Values match: [ ] Y  [ ] N
- **SC-3:** [ ] PASS  [ ] FAIL

## Overall SHIP-02 Status

[ ] CLOSED (all 3 SC checked PASS)
[ ] BLOCKED — see issues: [link]

## Deviations from Runbook

[Any step that required adjustment and why]
```
