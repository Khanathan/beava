//! Per-segment timing for `push_with_cascade_on_shard`. Counts + nanoseconds
//! accumulated into Relaxed atomics; snapshot via `/debug/hotpath`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub(crate) static CASCADE_CALLS:       AtomicU64 = AtomicU64::new(0);
pub(crate) static CASCADE_PRIMARY_NS:  AtomicU64 = AtomicU64::new(0);
pub(crate) static CASCADE_WALK_NS:     AtomicU64 = AtomicU64::new(0);
pub(crate) static CASCADE_TOTAL_NS:    AtomicU64 = AtomicU64::new(0);

pub(crate) static DOWNSTREAM_CALLS:    AtomicU64 = AtomicU64::new(0);
pub(crate) static DOWNSTREAM_TOTAL_NS: AtomicU64 = AtomicU64::new(0);

pub(crate) static WEM_CALLS:           AtomicU64 = AtomicU64::new(0);
pub(crate) static WEM_LOOKUP_NS:       AtomicU64 = AtomicU64::new(0);
pub(crate) static WEM_CLOSURE_NS:      AtomicU64 = AtomicU64::new(0);

pub(crate) static OPS_CALLS:           AtomicU64 = AtomicU64::new(0);
pub(crate) static OPS_FIND_NS:         AtomicU64 = AtomicU64::new(0);
pub(crate) static OPS_PUSH_NS:         AtomicU64 = AtomicU64::new(0);

#[inline]
pub(crate) fn record_cascade(primary: Duration, walk: Duration, total: Duration) {
    CASCADE_CALLS.fetch_add(1, Ordering::Relaxed);
    CASCADE_PRIMARY_NS.fetch_add(primary.as_nanos() as u64, Ordering::Relaxed);
    CASCADE_WALK_NS.fetch_add(walk.as_nanos() as u64, Ordering::Relaxed);
    CASCADE_TOTAL_NS.fetch_add(total.as_nanos() as u64, Ordering::Relaxed);
}

#[inline]
pub(crate) fn record_downstream(d: Duration) {
    DOWNSTREAM_CALLS.fetch_add(1, Ordering::Relaxed);
    DOWNSTREAM_TOTAL_NS.fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
}

#[inline]
pub(crate) fn record_wem(lookup: Duration, closure: Duration) {
    WEM_CALLS.fetch_add(1, Ordering::Relaxed);
    WEM_LOOKUP_NS.fetch_add(lookup.as_nanos() as u64, Ordering::Relaxed);
    WEM_CLOSURE_NS.fetch_add(closure.as_nanos() as u64, Ordering::Relaxed);
}

#[inline]
pub(crate) fn record_ops(find: Duration, push: Duration) {
    OPS_CALLS.fetch_add(1, Ordering::Relaxed);
    OPS_FIND_NS.fetch_add(find.as_nanos() as u64, Ordering::Relaxed);
    OPS_PUSH_NS.fetch_add(push.as_nanos() as u64, Ordering::Relaxed);
}

pub fn snapshot_and_reset() -> serde_json::Value {
    let avg = |t: u64, c: u64| if c == 0 { 0.0 } else { t as f64 / c as f64 };
    let cc = CASCADE_CALLS.swap(0, Ordering::Relaxed);
    let cp = CASCADE_PRIMARY_NS.swap(0, Ordering::Relaxed);
    let cw = CASCADE_WALK_NS.swap(0, Ordering::Relaxed);
    let ct = CASCADE_TOTAL_NS.swap(0, Ordering::Relaxed);
    let dc = DOWNSTREAM_CALLS.swap(0, Ordering::Relaxed);
    let dt = DOWNSTREAM_TOTAL_NS.swap(0, Ordering::Relaxed);
    let wc = WEM_CALLS.swap(0, Ordering::Relaxed);
    let wl = WEM_LOOKUP_NS.swap(0, Ordering::Relaxed);
    let wx = WEM_CLOSURE_NS.swap(0, Ordering::Relaxed);
    let oc = OPS_CALLS.swap(0, Ordering::Relaxed);
    let of = OPS_FIND_NS.swap(0, Ordering::Relaxed);
    let op = OPS_PUSH_NS.swap(0, Ordering::Relaxed);
    serde_json::json!({
        "cascade": {
            "calls": cc,
            "avg_ns": {
                "primary_push": avg(cp, cc),
                "downstream_walk": avg(cw, cc),
                "total": avg(ct, cc),
            },
        },
        "per_downstream": {
            "calls": dc,
            "avg_ns_each": avg(dt, dc),
            "calls_per_cascade": if cc == 0 { 0.0 } else { dc as f64 / cc as f64 },
        },
        "with_entity_mut": {
            "calls": wc,
            "avg_ns": { "lookup": avg(wl, wc), "closure": avg(wx, wc) },
        },
        "op_iteration_inside_closure": {
            "calls": oc,
            "avg_ns": {
                "find_loop": avg(of, oc),
                "push_aggregate": avg(op, oc),
            },
        },
    })
}
