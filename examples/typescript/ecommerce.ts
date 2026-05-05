/**
 * E-commerce demo: purchase event type; basket aggregations
 * (items per user, mean basket size, total spend).
 *
 * Runs against the in-process mock (`_mock.ts`); swap the import to
 * `@beava/sdk` for the real BeavaApp.
 */
import { BeavaApp, event, table } from "./_mock.ts";

async function main(): Promise<number> {
  const app = new BeavaApp();
  try {
    const Purchase = event("Purchase");
    const UserBasket = table({
      name: "UserBasket",
      source: "Purchase",
      key: ["user_id"],
      ops: {
        items_purchased_1h: ["sum", "qty"],
        purchase_count_1h: ["count", null],
        mean_purchase_value_1h: ["mean", "price"],
        total_spend_1h: ["sum", "price"],
        min_price_1h: ["min", "price"],
        max_price_1h: ["max", "price"],
      },
    });
    await app.register([Purchase, UserBasket]);

    const purchases: Array<[string, number, number]> = [
      ["sku_a", 1, 10.0],
      ["sku_b", 2, 5.0],
      ["sku_a", 1, 10.0],
      ["sku_c", 3, 7.5],
    ];
    for (const [sku, qty, price] of purchases) {
      await app.push("Purchase", {
        user_id: "bob",
        sku,
        qty,
        price,
      });
    }

    const bob = await app.get("UserBasket", "bob");
    console.log(`bob basket: ${JSON.stringify(bob)}`);

    if (bob.purchase_count_1h !== 4) throw new Error("purchase_count");
    if (bob.items_purchased_1h !== 7) throw new Error("items_purchased");
    const expectedTotal = 10.0 + 5.0 + 10.0 + 7.5;
    if (Math.abs((bob.total_spend_1h as number) - expectedTotal) > 1e-6) {
      throw new Error("total_spend");
    }
    if (
      Math.abs((bob.mean_purchase_value_1h as number) - expectedTotal / 4) >
      1e-6
    ) {
      throw new Error("mean_purchase_value");
    }
    if (Math.abs((bob.min_price_1h as number) - 5.0) > 1e-6) {
      throw new Error("min_price");
    }
    if (Math.abs((bob.max_price_1h as number) - 10.0) > 1e-6) {
      throw new Error("max_price");
    }
  } finally {
    await app.close();
  }

  console.log("OK -- ecommerce.ts");
  return 0;
}

main().then(
  (code) => process.exit(code),
  (err) => {
    console.error(err);
    process.exit(1);
  },
);
