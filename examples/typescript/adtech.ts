/**
 * Adtech demo: impression event type; campaign aggregations
 * (impressions per campaign, sum of bid amounts, mean bid).
 *
 * Runs against the in-process mock (`_mock.ts`); swap the import to
 * `@beava/sdk` for the real BeavaApp.
 */
import { BeavaApp, event, table } from "./_mock.ts";

async function main(): Promise<number> {
  const app = new BeavaApp();
  try {
    const Impression = event("Impression");
    const CampaignStats = table({
      name: "CampaignStats",
      source: "Impression",
      key: ["campaign_id"],
      ops: {
        impressions_1h: ["count", null],
        bid_sum_1h: ["sum", "bid"],
        bid_mean_1h: ["mean", "bid"],
        bid_min_1h: ["min", "bid"],
        bid_max_1h: ["max", "bid"],
      },
    });
    const result = await app.register([Impression, CampaignStats]);
    if (result.status !== "ok") {
      throw new Error(`register failed: ${JSON.stringify(result)}`);
    }
    console.log(`Registered ${result.registryVersion} descriptors`);

    const events: Array<[string, string, number]> = [
      ["c1", "cr1", 0.5],
      ["c1", "cr1", 0.75],
      ["c1", "cr2", 1.0],
      ["c2", "cr3", 0.25],
      ["c2", "cr3", 0.4],
    ];
    for (const [campId, creativeId, bid] of events) {
      await app.push("Impression", {
        campaign_id: campId,
        creative_id: creativeId,
        bid,
      });
    }

    const c1 = await app.get("CampaignStats", "c1");
    const c2 = await app.get("CampaignStats", "c2");
    console.log(`Campaign c1: ${JSON.stringify(c1)}`);
    console.log(`Campaign c2: ${JSON.stringify(c2)}`);

    if (c1.impressions_1h !== 3) throw new Error("c1 impressions");
    if (Math.abs((c1.bid_sum_1h as number) - 2.25) > 1e-6) {
      throw new Error("c1 bid_sum");
    }
    if (Math.abs((c1.bid_mean_1h as number) - 0.75) > 1e-6) {
      throw new Error("c1 bid_mean");
    }
    if (Math.abs((c1.bid_min_1h as number) - 0.5) > 1e-6) {
      throw new Error("c1 bid_min");
    }
    if (Math.abs((c1.bid_max_1h as number) - 1.0) > 1e-6) {
      throw new Error("c1 bid_max");
    }
    if (c2.impressions_1h !== 2) throw new Error("c2 impressions");
    if (Math.abs((c2.bid_sum_1h as number) - 0.65) > 1e-6) {
      throw new Error("c2 bid_sum");
    }
  } finally {
    await app.close();
  }

  console.log("OK -- adtech.ts");
  return 0;
}

main().then(
  (code) => process.exit(code),
  (err) => {
    console.error(err);
    process.exit(1);
  },
);
