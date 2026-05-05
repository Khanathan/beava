"""
Fraud demo: high-cardinality velocity + sketch + geo aggregations
(transaction velocity, unique merchants, geo velocity for impossible-travel).

Mirrors `crates/beava-bench/configs/fraud-team.json` shape -- 5 event types,
5 group-by axes -- using Polars-renamed op names per ADR-002 (mean / n_unique
/ quantile / var / std).

The mock supports count/sum/mean/min/max. Sketches (n_unique, quantile,
top_k), decays, and geo ops are no-ops here; they're shown for shape so
the registered surface mirrors the real benchmark.
"""
from _mock import App, event, table


def main() -> int:
    with App() as app:
        # 5 event types mirror fraud-team.json.
        Txn = event("Txn")
        Login = event("Login")
        Signup = event("Signup")
        CardAdd = event("CardAdd")
        Refund = event("Refund")

        # The user-axis aggregation table is the busiest in fraud detection.
        # n_unique / quantile / geo_velocity are real-engine ops; included
        # for shape but the mock no-ops them, so assertions only cover
        # count/sum/mean/min/max.
        UserFraudStats = table(
            name="UserFraudStats",
            source="Txn",
            key=["user_id"],
            ops={
                "tx_count_1h": ("count", None),
                "tx_sum_1h": ("sum", "amount"),
                "tx_mean_1h": ("mean", "amount"),
                "tx_min_1h": ("min", "amount"),
                "tx_max_1h": ("max", "amount"),
                "tx_unique_merchants_1h": ("n_unique", "merchant_id"),  # mock no-op
                "tx_p99_1h": ("quantile", "amount"),  # mock no-op
                "tx_geo_velocity_1h": ("geo_velocity", None),  # mock no-op
            },
        )
        app.register(Txn, Login, Signup, CardAdd, Refund, UserFraudStats)

        # 10 transactions for 'alice' (mirrors fraud-team Txn shape).
        for amount, merchant in [
            (12.50, "amazon"),
            (150.00, "amazon"),
            (89.99, "ebay"),
            (1500.00, "fancy_store"),
            (5.00, "starbucks"),
            (35.00, "amazon"),
            (220.00, "ebay"),
            (10.00, "starbucks"),
            (45.00, "amazon"),
            (12.00, "starbucks"),
        ]:
            app.push(
                "Txn",
                {
                    "user_id": "alice",
                    "card_fp": "card_001",
                    "amount": amount,
                    "merchant_id": merchant,
                    "ip_address": "203.0.113.42",
                    "device_id": "phone_xyz",
                    "lat": 37.7749,
                    "lon": -122.4194,
                },
            )

        result = app.get("UserFraudStats", "alice")
        print(f"alice fraud stats: {result}")

        assert result["tx_count_1h"] == 10
        expected_sum = sum(
            [
                12.50,
                150.00,
                89.99,
                1500.00,
                5.00,
                35.00,
                220.00,
                10.00,
                45.00,
                12.00,
            ]
        )
        assert abs(result["tx_sum_1h"] - expected_sum) < 1e-3
        assert abs(result["tx_mean_1h"] - expected_sum / 10) < 1e-3
        assert abs(result["tx_min_1h"] - 5.00) < 1e-6
        assert abs(result["tx_max_1h"] - 1500.00) < 1e-6
        # n_unique / quantile / geo_velocity: not asserted (mock no-ops).
        # Real engine values:
        #   tx_unique_merchants_1h ~ 4 (amazon, ebay, fancy_store, starbucks)
        #   tx_p99_1h ~ 1500
        #   tx_geo_velocity_1h ~ 0 (single location)

    print("OK -- fraud.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
