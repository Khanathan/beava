/**
 * Fraud demo: high-cardinality velocity + sketch + geo aggregations
 * (transaction velocity, unique merchants, geo velocity for impossible-travel).
 *
 * Mirrors crates/beava-bench/configs/fraud-team.json shape -- 5 event types,
 * 5 group-by axes. Uses Polars-renamed op names per ADR-002 (mean / nUnique
 * / quantile).
 *
 * Phase 13.0 mock supports count/sum/mean/min/max. Sketches (nUnique,
 * quantile, topK), decays, and geo ops are no-ops in the mock --
 * demonstrated below for shape but assertions only check the
 * mock-supported ops. Phase 13.6 re-verifies with real engines.
 */
import { BeavaApp, event, table } from "./_mock.ts";

async function main(): Promise<number> {
  const app = new BeavaApp();
  try {
    // 5 event types (mirrors fraud-team.json)
    const Txn = event("Txn");
    const Login = event("Login");
    const Signup = event("Signup");
    const CardAdd = event("CardAdd");
    const Refund = event("Refund");

    // User-axis aggregation table.
    // n_unique / quantile / geo_velocity are real-engine ops shown for
    // shape; the mock no-ops them. Assertions check count/sum/mean/min/max.
    const UserFraudStats = table({
      name: "UserFraudStats",
      source: "Txn",
      key: ["user_id"],
      ops: {
        tx_count_1h: ["count", null],
        tx_sum_1h: ["sum", "amount"],
        tx_mean_1h: ["mean", "amount"],
        tx_min_1h: ["min", "amount"],
        tx_max_1h: ["max", "amount"],
        tx_unique_merchants_1h: ["n_unique", "merchant_id"], // mock no-op
        tx_p99_1h: ["quantile", "amount"], // mock no-op
        tx_geo_velocity_1h: ["geo_velocity", null], // mock no-op
      },
    });
    await app.register([Txn, Login, Signup, CardAdd, Refund, UserFraudStats]);

    const txns: Array<[number, string]> = [
      [12.5, "amazon"],
      [150.0, "amazon"],
      [89.99, "ebay"],
      [1500.0, "fancy_store"],
      [5.0, "starbucks"],
      [35.0, "amazon"],
      [220.0, "ebay"],
      [10.0, "starbucks"],
      [45.0, "amazon"],
      [12.0, "starbucks"],
    ];
    for (const [amount, merchant] of txns) {
      await app.push("Txn", {
        user_id: "alice",
        card_fp: "card_001",
        amount,
        merchant_id: merchant,
        ip_address: "203.0.113.42",
        device_id: "phone_xyz",
        lat: 37.7749,
        lon: -122.4194,
      });
    }

    const result = await app.get("UserFraudStats", "alice");
    console.log(`alice fraud stats: ${JSON.stringify(result)}`);

    // Assertions on mock-supported ops (computed values).
    if (result.tx_count_1h !== 10) throw new Error("tx_count");
    const expectedSum = txns.reduce((acc, [a]) => acc + a, 0);
    if (Math.abs((result.tx_sum_1h as number) - expectedSum) > 1e-3) {
      throw new Error("tx_sum");
    }
    if (Math.abs((result.tx_mean_1h as number) - expectedSum / 10) > 1e-3) {
      throw new Error("tx_mean");
    }
    if (Math.abs((result.tx_min_1h as number) - 5.0) > 1e-6) {
      throw new Error("tx_min");
    }
    if (Math.abs((result.tx_max_1h as number) - 1500.0) > 1e-6) {
      throw new Error("tx_max");
    }
    // nUnique + quantile + geo_velocity are no-ops in mock; not asserted.
  } finally {
    await app.close();
  }

  console.log("OK -- fraud.ts");
  return 0;
}

main().then(
  (code) => process.exit(code),
  (err) => {
    console.error(err);
    process.exit(1);
  },
);
