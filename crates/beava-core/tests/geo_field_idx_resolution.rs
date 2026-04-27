//! Plan 19.2-06 Task 3 — D-01: migrate 4 surviving geo ops to consume
//! `extracted[lat_idx]` + `extracted[lon_idx]` via `update_at`, removing
//! the `read_lat_lon(row, &lat_field, &lon_field)` row-scan in the hot path.
//!
//! RED commit: tests fail before Task 3.b adds:
//!   - `lat_idx: u8` + `lon_idx: u8` fields to `AggExtParams`
//!   - `update_at(extracted, lat_idx, lon_idx, ...)` methods on each geo state
//!   - `update_with_extracted` dispatch switches from row-based to index-based
//!     when `lat_idx != FIELD_IDX_NONE`
//!
//! GREEN commit: Task 3.b makes all tests pass.

use beava_core::agg_geo::{
    DistanceFromHomeState, GeoDistanceState, GeoSpreadState, GeoVelocityState,
};
use beava_core::agg_op::{AggExtParams, ExtractedFields, FIELD_IDX_NONE};
use beava_core::row::{Row, Value};
use smallvec::smallvec;

// ─── helpers ────────────────────────────────────────────────────────────────

fn row_geo(lat: f64, lon: f64) -> Row {
    Row::new()
        .with_field("lat", Value::F64(lat))
        .with_field("lon", Value::F64(lon))
}

fn extracted_geo(lat: f64, lon: f64) -> ExtractedFields<'static> {
    // We use a leaked Box to get a 'static reference for the SmallVec — test
    // only; never do this in production code.
    let lat_box: &'static Value = Box::leak(Box::new(Value::F64(lat)));
    let lon_box: &'static Value = Box::leak(Box::new(Value::F64(lon)));
    smallvec![Some(lat_box), Some(lon_box)]
}

// ── Test 1: AggExtParams exposes lat_idx + lon_idx ───────────────────────────

/// `AggExtParams` must have `lat_idx: u8` and `lon_idx: u8` fields defaulting
/// to `FIELD_IDX_NONE`. RED: fields do not exist today.
#[test]
fn agg_ext_params_has_lat_lon_idx() {
    let ext = AggExtParams::default();
    assert_eq!(
        ext.lat_idx, FIELD_IDX_NONE,
        "AggExtParams::lat_idx must default to FIELD_IDX_NONE"
    );
    assert_eq!(
        ext.lon_idx, FIELD_IDX_NONE,
        "AggExtParams::lon_idx must default to FIELD_IDX_NONE"
    );
}

// ── Test 2: GeoVelocityState::update_at resolves coords from extracted array ─

/// `GeoVelocityState::update_at(extracted, lat_idx, lon_idx, t, where_matched)`
/// must read lat/lon from `extracted[lat_idx]` / `extracted[lon_idx]` and
/// produce the same result as `update(row, t, where_matched)`.
///
/// RED: `update_at` does not exist on GeoVelocityState.
#[test]
fn geo_velocity_update_at_matches_update() {
    // Row-based reference.
    let mut s_row = GeoVelocityState::with_fields("lat".into(), "lon".into());
    s_row.update(&row_geo(40.0, -74.0), 0, true);
    s_row.update(&row_geo(40.5, -74.0), 3_600_000, true);
    let ref_val = s_row.query();

    // extracted-based fast path: lat at index 0, lon at index 1.
    let mut s_ext = GeoVelocityState::with_fields("lat".into(), "lon".into());
    let ext0 = extracted_geo(40.0, -74.0);
    s_ext.update_at(&ext0, 0, 1, 0, true);
    let ext1 = extracted_geo(40.5, -74.0);
    s_ext.update_at(&ext1, 0, 1, 3_600_000, true);
    let ext_val = s_ext.query();

    assert_eq!(
        ref_val, ext_val,
        "GeoVelocityState::update_at must produce same result as update(); \
         row={ref_val:?} vs ext={ext_val:?}"
    );
}

// ── Test 3: GeoDistanceState::update_at resolves from extracted array ────────

/// RED: `update_at` does not exist on GeoDistanceState.
#[test]
fn geo_distance_update_at_matches_update() {
    let mut s_row = GeoDistanceState::with_fields("lat".into(), "lon".into());
    s_row.update(&row_geo(40.0, -74.0), true);
    s_row.update(&row_geo(40.5, -74.0), true);
    let ref_val = s_row.query();

    let mut s_ext = GeoDistanceState::with_fields("lat".into(), "lon".into());
    let ext0 = extracted_geo(40.0, -74.0);
    s_ext.update_at(&ext0, 0, 1, true);
    let ext1 = extracted_geo(40.5, -74.0);
    s_ext.update_at(&ext1, 0, 1, true);
    let ext_val = s_ext.query();

    assert_eq!(
        ref_val, ext_val,
        "GeoDistanceState::update_at must produce same result as update(); \
         row={ref_val:?} vs ext={ext_val:?}"
    );
}

// ── Test 4: GeoSpreadState::update_at resolves from extracted array ──────────

/// RED: `update_at` does not exist on GeoSpreadState.
#[test]
fn geo_spread_update_at_matches_update() {
    let mut s_row = GeoSpreadState::with_fields("lat".into(), "lon".into());
    s_row.update(&row_geo(40.0, -74.0), true);
    s_row.update(&row_geo(41.0, -74.0), true);
    s_row.update(&row_geo(40.5, -73.5), true);
    let ref_val = s_row.query();

    let mut s_ext = GeoSpreadState::with_fields("lat".into(), "lon".into());
    let e0 = extracted_geo(40.0, -74.0);
    s_ext.update_at(&e0, 0, 1, true);
    let e1 = extracted_geo(41.0, -74.0);
    s_ext.update_at(&e1, 0, 1, true);
    let e2 = extracted_geo(40.5, -73.5);
    s_ext.update_at(&e2, 0, 1, true);
    let ext_val = s_ext.query();

    assert_eq!(
        ref_val, ext_val,
        "GeoSpreadState::update_at must produce same result as update(); \
         row={ref_val:?} vs ext={ext_val:?}"
    );
}

// ── Test 5: DistanceFromHomeState::update_at resolves from extracted array ───

/// RED: `update_at` does not exist on DistanceFromHomeState.
#[test]
fn distance_from_home_update_at_matches_update() {
    let samples = 3;
    let mut s_row = DistanceFromHomeState::with_fields("lat".into(), "lon".into(), samples);
    s_row.update(&row_geo(40.0, -74.0), true);
    s_row.update(&row_geo(40.1, -74.0), true);
    s_row.update(&row_geo(40.2, -74.0), true);
    s_row.update(&row_geo(41.0, -74.0), true);
    let ref_val = s_row.query();

    let mut s_ext = DistanceFromHomeState::with_fields("lat".into(), "lon".into(), samples);
    let e0 = extracted_geo(40.0, -74.0);
    s_ext.update_at(&e0, 0, 1, true);
    let e1 = extracted_geo(40.1, -74.0);
    s_ext.update_at(&e1, 0, 1, true);
    let e2 = extracted_geo(40.2, -74.0);
    s_ext.update_at(&e2, 0, 1, true);
    let e3 = extracted_geo(41.0, -74.0);
    s_ext.update_at(&e3, 0, 1, true);
    let ext_val = s_ext.query();

    assert_eq!(
        ref_val, ext_val,
        "DistanceFromHomeState::update_at must produce same result as update(); \
         row={ref_val:?} vs ext={ext_val:?}"
    );
}
