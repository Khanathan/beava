"""
Adtech demo: impression event type; campaign aggregations
(impressions per campaign, sum of bid amounts, mean bid).

Phase 13.0 runs against examples/python/_mock.py (computes via push).
Phase 13.5 swaps the import to `from beava import App, event, table` (real bv.App).
"""
from _mock import App, event, table


def main() -> int:
    with App() as app:
        # Register an event source + a table aggregation.
        Impression = event("Impression")
        CampaignStats = table(
            name="CampaignStats",
            source="Impression",
            key=["campaign_id"],
            ops={
                "impressions_1h": ("count", None),
                "bid_sum_1h": ("sum", "bid"),
                "bid_mean_1h": ("mean", "bid"),
                "bid_min_1h": ("min", "bid"),
                "bid_max_1h": ("max", "bid"),
            },
        )
        result = app.register(Impression, CampaignStats)
        assert result["status"] == "ok"
        print(f"Registered {result['registry_version']} descriptors")

        # Push events. Each push goes through MockApp's update logic.
        events = [
            ("c1", "cr1", 0.50),
            ("c1", "cr1", 0.75),
            ("c1", "cr2", 1.00),
            ("c2", "cr3", 0.25),
            ("c2", "cr3", 0.40),
        ]
        for camp_id, creative_id, bid in events:
            app.push(
                "Impression",
                {"campaign_id": camp_id, "creative_id": creative_id, "bid": bid},
            )

        # Query -- computed values, not pre-seeded.
        c1 = app.get("CampaignStats", "c1")
        c2 = app.get("CampaignStats", "c2")

        print(f"Campaign c1: {c1}")
        print(f"Campaign c2: {c2}")

        # Assertions check the COMPUTED values.
        assert c1["impressions_1h"] == 3
        assert abs(c1["bid_sum_1h"] - 2.25) < 1e-6
        assert abs(c1["bid_mean_1h"] - 0.75) < 1e-6
        assert abs(c1["bid_min_1h"] - 0.50) < 1e-6
        assert abs(c1["bid_max_1h"] - 1.00) < 1e-6
        assert c2["impressions_1h"] == 2
        assert abs(c2["bid_sum_1h"] - 0.65) < 1e-6

    print("OK -- adtech.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
