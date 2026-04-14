#!/usr/bin/env bash
# =============================================================================
# Tally Demo Recording Script
# =============================================================================
#
# This is a GUIDE for the human recording an asciinema/vhs terminal GIF.
# It is NOT meant to be run as an automated script.
#
# Recording tools:
#   asciinema: asciinema rec demo.cast
#   vhs:       vhs demo.tape  (define steps in demo.tape, auto-records)
#
# Tips:
#   - Use a clean terminal with a dark background
#   - Set font size large enough to read in a GIF (16-18pt)
#   - Terminal width: 80-100 columns, height: 30-40 rows
#   - Pause briefly after each command so viewers can read the output
#   - Type at a natural pace (or use vhs's Type command for consistency)
#
# Prerequisite: Build tally first
#   cargo build --release
#   cd python && pip install -e . && cd ..
#
# =============================================================================

# --- STEP 1: Start the Tally server ----------------------------------------
# Show that it's a single binary. Start it in the background.
# Wait a moment for it to be ready.

./target/release/tally &
sleep 1

# You should see output like:
#   [INFO] Tally server starting
#   [INFO] TCP listening on 0.0.0.0:6400
#   [INFO] HTTP listening on 0.0.0.0:6401

# --- STEP 2: Verify the server is running ----------------------------------
# Quick health check via curl to show the HTTP management API.

curl -s http://localhost:6401/health | python3 -m json.tool

# Expected output: {"status": "ok", ...}

# --- STEP 3: Define a fraud detection pipeline (5 features) -----------------
# Create a small Python script inline. Type this out (or have it ready).
# The point is to show how few lines it takes.

cat > /tmp/tally_demo.py << 'PYEOF'
import tally as tl

# Define a stream -- raw payment events
@tl.stream
class RawTransactions:
    user_id: str
    amount: float
    merchant_id: str

# Define features -- 5 real-time fraud signals
@tl.table(key="user_id")
def UserFeatures(txs: RawTransactions) -> tl.Table:
    return txs.group_by("user_id").agg(
        tx_count_1h=tl.count(window="1h"),
        tx_sum_1h=tl.sum("amount", window="1h"),
        avg_amount=tl.avg("amount", window="1h"),
        max_amount=tl.max("amount", window="1h"),
        unique_merchants=tl.count_distinct("merchant_id", window="1h"),
    )

# Connect and register
app = tl.App("localhost:6400")
app.register(RawTransactions, UserFeatures)
print("Pipeline registered.")

# Push 10 events and show features updating
import random
for i in range(1, 11):
    amount = round(random.uniform(10, 500), 2)
    merchant = f"merchant_{random.randint(1, 5)}"
    features = app.push_sync(RawTransactions, {
        "user_id": "user_123",
        "amount": amount,
        "merchant_id": merchant,
    })
    print(f"Event {i:2d}: amount=${amount:7.2f}  merchant={merchant}")
    print(f"         tx_count_1h={features.tx_count_1h}  "
          f"tx_sum_1h={features.tx_sum_1h:.2f}  "
          f"avg={features.avg_amount:.2f}  "
          f"max={features.max_amount:.2f}  "
          f"unique_merchants={features.unique_merchants}")
    print()

app.close()
PYEOF

# --- STEP 4: Run the script ------------------------------------------------
# This is the key moment. Show events going in, features coming back.

python3 /tmp/tally_demo.py

# Pause here so viewers can read the output. Each event should show:
#   - The input (amount, merchant)
#   - The updated features (count incrementing, sum growing, etc.)

# --- STEP 5: Show memory usage via the management API ----------------------
# Demonstrate that Tally tracks memory per stream and per entity.

curl -s http://localhost:6401/debug/memory | python3 -m json.tool

# Expected output shows:
#   - entity_count: number of unique entities
#   - estimated_bytes: total memory used
#   - per_stream: breakdown by stream

# --- STEP 6: Inspect a specific entity's features --------------------------
# Show all features for user_123 via the debug API.

curl -s http://localhost:6401/debug/key/user_123 | python3 -m json.tool

# --- STEP 7: Clean up ------------------------------------------------------
# Stop the server.

kill %1

# =============================================================================
# END OF RECORDING
#
# Post-processing:
#   asciinema: asciinema upload demo.cast  (or convert to GIF)
#   Convert to GIF: agg demo.cast demo.gif
#   Or use vhs to render directly: vhs demo.tape
#
# The GIF should be ~30-45 seconds. Trim dead time in post.
# Embed in README as: ![Tally Demo](assets/demo.gif)
# =============================================================================
