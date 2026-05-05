"""
E-commerce demo: purchase event type; basket aggregations
(items per user, mean basket size, total spend).

Runs against the in-process mock (`_mock.py`); swap the import to
`from beava import App, event, table` for the real SDK.
"""
from _mock import App, event, table


def main() -> int:
    with App() as app:
        Purchase = event("Purchase")
        UserBasket = table(
            name="UserBasket",
            source="Purchase",
            key=["user_id"],
            ops={
                "items_purchased_1h": ("sum", "qty"),
                "purchase_count_1h": ("count", None),
                "mean_purchase_value_1h": ("mean", "price"),
                "total_spend_1h": ("sum", "price"),
                "min_price_1h": ("min", "price"),
                "max_price_1h": ("max", "price"),
            },
        )
        app.register(Purchase, UserBasket)

        for sku, qty, price in [
            ("sku_a", 1, 10.0),
            ("sku_b", 2, 5.0),
            ("sku_a", 1, 10.0),
            ("sku_c", 3, 7.5),
        ]:
            app.push(
                "Purchase",
                {"user_id": "bob", "sku": sku, "qty": qty, "price": price},
            )

        bob = app.get("UserBasket", "bob")
        print(f"bob basket: {bob}")

        assert bob["purchase_count_1h"] == 4
        assert bob["items_purchased_1h"] == 7  # 1 + 2 + 1 + 3
        expected_total = 10.0 + 5.0 + 10.0 + 7.5
        assert abs(bob["total_spend_1h"] - expected_total) < 1e-6
        assert abs(bob["mean_purchase_value_1h"] - expected_total / 4) < 1e-6
        assert abs(bob["min_price_1h"] - 5.0) < 1e-6
        assert abs(bob["max_price_1h"] - 10.0) < 1e-6

    print("OK -- ecommerce.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
