"""
pipeline.py — Sendo Farm-flavored real-time feature pipeline.

Camera points here in Scene 3. Three tables, eleven features, all driven
by a single event stream that mirrors a fresh-food marketplace:
session-level clicks / cart-adds / orders, tagged with product category
and origin province so the pipeline can answer questions a fresh-food
operator actually asks — who's shopping, what's trending in the next 5
minutes, which provinces are supplying the most demand right now.

Uses the shipping v0 API (@bv.stream + @bv.table + group_by/agg) —
what a viewer would find in the repo.

Run:
    python scripts/demo-sendo/pipeline.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))

import beava as bv


# ─── Events ────────────────────────────────────────────────────────────
# A single stream of buyer actions. OCOP-flavored categories and
# province origin fields let downstream tables slice by region and
# category — the things that matter for perishable supply planning.

@bv.stream
class Event:
    user_id: str
    product_id: str
    category: str       # rau_la | trai_cay | thit_ca | sua_trung | gao_kho
    origin: str         # Hai_Duong | Lam_Dong | Bac_Giang | ...
    type: str           # view | add_to_cart | order
    price: float        # VND
    ts: float


# ─── Per-buyer: engagement, taste breadth, basket value ────────────────

@bv.table(key="user_id")
def BuyerFeatures(ev: Event) -> bv.Table:
    views  = ev.filter(bv.col("type") == "view")
    carts  = ev.filter(bv.col("type") == "add_to_cart")
    orders = ev.filter(bv.col("type") == "order")
    return (
        views.group_by("user_id").agg(
            views_1h        = bv.count(window="1h"),
            categories_24h  = bv.count_distinct("category", window="24h"),
        )
        .join(
            carts.group_by("user_id").agg(
                cart_adds_24h    = bv.count(window="24h"),
                basket_value_24h = bv.sum("price", window="24h"),
            ),
            on="user_id", type="left")
        .join(
            orders.group_by("user_id").agg(
                orders_24h = bv.count(window="24h"),
            ),
            on="user_id", type="left")
    )


# ─── Per-product: trending short+long, reach, purchase intent ──────────

@bv.table(key="product_id")
def ProductFeatures(ev: Event) -> bv.Table:
    views = ev.filter(bv.col("type") == "view")
    carts = ev.filter(bv.col("type") == "add_to_cart")
    return (
        views.group_by("product_id").agg(
            trending_5m        = bv.count(window="5m"),
            trending_1h        = bv.count(window="1h"),
            unique_viewers_1h  = bv.count_distinct("user_id", window="1h"),
        )
        .join(
            carts.group_by("product_id").agg(
                cart_adds_1h = bv.count(window="1h"),
            ),
            on="product_id", type="left")
    )


# ─── Per-origin: regional demand for harvest + supply planning ─────────

@bv.table(key="origin")
def OriginFeatures(ev: Event) -> bv.Table:
    orders = ev.filter(bv.col("type") == "order")
    return orders.group_by("origin").agg(
        orders_1h   = bv.count(window="1h"),
        gmv_24h     = bv.sum("price", window="24h"),
        buyers_24h  = bv.count_distinct("user_id", window="24h"),
    )


if __name__ == "__main__":
    app = bv.App("localhost:6400")
    app.register(Event, BuyerFeatures, ProductFeatures, OriginFeatures)
    print("3 tables · 11 features active")
