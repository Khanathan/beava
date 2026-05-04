"""
Fraud demo: high-cardinality velocity + sketch + geo aggregations
(transaction velocity, unique merchants, geo velocity for impossible-travel).

Mirrors crates/beava-bench/configs/fraud-team.json shape -- 5 event types,
5 group-by axes. Uses Polars-renamed op names per ADR-002 (mean / n_unique
/ quantile / var / std).

Phase 13.0 mock supports count/sum/mean/min/max. Sketches (n_unique,
quantile, top_k), decays, and geo ops are no-ops in the mock --
demonstrated below for shape but assertions only check the
mock-supported ops. Phase 13.5 + 13.6 re-verify with real engines.
"""
from _mock import App, event, table


def main() -> int:
    with App() as app:
        # 5 event types (mirrors fraud-team.json)
        Txn = event("Txn")
        Login = event("Login")
        Signup = event("Signup")
        CardAdd = event("CardAdd")
        Refund = event("Refund")

        # User-axis aggregation table (the most active in fraud detection).
        # NOTE: n_unique / quantile / geo_velocity are real-engine ops; they're
        # shown here for shape but the mock no-ops them. Assertions check
        # count/sum/mean/min/max (mock-supported).
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
                # The following are real-engine ops; mock no-ops them.
                # Phase 13.5 will compute them against the real engine.
                "tx_unique_merchants_1h": ("n_unique", "merchant_id"),  # mock no-op
                "tx_p99_1h": ("quantile", "amount"),  # mock no-op
                "tx_geo_velocity_1h": ("geo_velocity", None),  # mock no-op
            },
        )
        app.register(Txn, Login, Signup, CardAdd, Refund, UserFraudStats)

        # Push 10 transactions for user 'alice' (mirrors fraud-team Txn shape).
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

        # Assertions on mock-supported ops (computed values).
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
        # n_unique + quantile + geo_velocity are no-ops in mock; not asserted here.
        # Real engine in Phase 13.5 will compute:
        #   tx_unique_merchants_1h ~ 4 (amazon, ebay, fancy_store, starbucks)
        #   tx_p99_1h ~ 1500
        #   tx_geo_velocity_1h ~ 0 (single location)

    print("OK -- fraud.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
