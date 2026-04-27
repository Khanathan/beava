//! Phase 11 bounded-buffer + geo operator benches.
//!
//! Bench groups (D-10 in 11-CONTEXT.md):
//!   buffer/{histogram,hour_of_day_histogram,seasonal_deviation,most_recent_n,reservoir_sample}/update
//!   geo/{geo_velocity,geo_distance,distance_from_home}/update
//!
//! Plan 19.2-06 (D-05): unique_cells + geo_entropy benches removed (ops removed from catalogue).
//! Recipe replacements (count_distinct(quadkey) + entropy(quadkey)) are covered by Plan 19.2-08
//! apply_path_bench.rs on the fraud-team pipeline shape.
//!
//! Per-bench rows captured to .planning/phases/11-bounded-buffer-geo-operators/11-perf-row.md
//! after running `cargo bench -p beava-core --bench phase11_buffer_geo`.

use beava_core::agg_buffer::{
    DowHourHistogramState, EventTypeMixState, HistogramState, HourOfDayHistogramState,
    MostRecentNState, ReservoirSampleState, SeasonalDeviationState,
};
use beava_core::agg_geo::{DistanceFromHomeState, GeoDistanceState, GeoVelocityState};
use beava_core::row::{Row, Value};
use criterion::{criterion_group, criterion_main, Criterion};

fn row_amount(v: f64) -> Row {
    Row::new().with_field("amount", Value::F64(v))
}

fn row_geo(lat: f64, lon: f64) -> Row {
    Row::new()
        .with_field("lat", Value::F64(lat))
        .with_field("lon", Value::F64(lon))
}

fn row_str(field: &str, v: &str) -> Row {
    Row::new().with_field(field, Value::Str(v.into()))
}

// ─── buffer/histogram/update ─────────────────────────────────────────────────

fn bench_histogram(c: &mut Criterion) {
    let mut h = HistogramState::new(vec![10.0, 20.0, 50.0, 100.0]);
    let r = row_amount(35.0);
    c.bench_function("buffer/histogram/update", |b| {
        b.iter(|| {
            h.update(std::hint::black_box(&r), Some("amount"), true);
        });
    });
}

fn bench_hour_of_day(c: &mut Criterion) {
    let mut h = HourOfDayHistogramState::default();
    c.bench_function("buffer/hour_of_day_histogram/update", |b| {
        b.iter(|| {
            h.update(std::hint::black_box(10_800_000), true);
        });
    });
}

fn bench_dow_hour(c: &mut Criterion) {
    let mut h = DowHourHistogramState::default();
    c.bench_function("buffer/dow_hour_histogram/update", |b| {
        b.iter(|| {
            h.update(std::hint::black_box(10_800_000), true);
        });
    });
}

fn bench_seasonal_deviation(c: &mut Criterion) {
    let mut s = SeasonalDeviationState::default();
    let r = row_amount(100.0);
    c.bench_function("buffer/seasonal_deviation/update", |b| {
        b.iter(|| {
            s.update(std::hint::black_box(&r), 10_800_000, Some("amount"), true);
        });
    });
}

fn bench_event_type_mix(c: &mut Criterion) {
    let mut s = EventTypeMixState::new(8, None);
    let r = row_str("type", "click");
    c.bench_function("buffer/event_type_mix/update", |b| {
        b.iter(|| {
            s.update(std::hint::black_box(&r), Some("type"), true);
        });
    });
}

fn bench_most_recent_n(c: &mut Criterion) {
    let mut s = MostRecentNState::new(16);
    let r = row_amount(7.0);
    c.bench_function("buffer/most_recent_n/update", |b| {
        b.iter(|| {
            s.update(std::hint::black_box(&r), Some("amount"), true);
        });
    });
}

fn bench_reservoir_sample(c: &mut Criterion) {
    let mut s = ReservoirSampleState::new(16);
    let r = row_amount(7.0);
    c.bench_function("buffer/reservoir_sample/update", |b| {
        b.iter(|| {
            s.update(std::hint::black_box(&r), Some("amount"), true);
        });
    });
}

// ─── geo benches ─────────────────────────────────────────────────────────────

fn bench_geo_velocity(c: &mut Criterion) {
    let mut s = GeoVelocityState::with_fields("lat".into(), "lon".into());
    s.update(&row_geo(40.0, -74.0), 0, true);
    let r = row_geo(40.5, -74.0);
    let mut t: i64 = 1_000_000;
    c.bench_function("geo/geo_velocity/update", |b| {
        b.iter(|| {
            s.update(std::hint::black_box(&r), t, true);
            t += 1_000;
        });
    });
}

fn bench_geo_distance(c: &mut Criterion) {
    let mut s = GeoDistanceState::with_fields("lat".into(), "lon".into());
    s.update(&row_geo(40.0, -74.0), true);
    let r = row_geo(40.001, -74.001);
    c.bench_function("geo/geo_distance/update", |b| {
        b.iter(|| {
            s.update(std::hint::black_box(&r), true);
        });
    });
}

fn bench_distance_from_home(c: &mut Criterion) {
    let mut s = DistanceFromHomeState::with_fields("lat".into(), "lon".into(), 8);
    for i in 0..8 {
        s.update(&row_geo(40.0 + i as f64 * 0.001, -74.0), true);
    }
    let r = row_geo(40.7128, -74.0060);
    c.bench_function("geo/distance_from_home/update", |b| {
        b.iter(|| {
            s.update(std::hint::black_box(&r), true);
        });
    });
}

criterion_group!(
    buffer_geo,
    bench_histogram,
    bench_hour_of_day,
    bench_dow_hour,
    bench_seasonal_deviation,
    bench_event_type_mix,
    bench_most_recent_n,
    bench_reservoir_sample,
    bench_geo_velocity,
    bench_geo_distance,
    bench_distance_from_home,
);
criterion_main!(buffer_geo);
