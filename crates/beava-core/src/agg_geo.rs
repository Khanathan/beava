//! Phase 11 geo aggregation operators (AGG-GEO-01..06).
//!
//! Distance computations use the `haversine` crate (great-circle / spherical
//! Earth, mean radius 6371 km). Cell encoding uses an equirectangular grid
//! `(floor(lat*precision), floor(lon*precision))` per CONTEXT D-02 — keeps
//! the dep surface small for v0; can swap to `h3o` in v0.1.
//!
//! D-06 invariants: no wall-clock reads, no rand. All state transitions are a
//! pure function of `(row, event_time_ms, prior state)`.
//! D-08 (Phase 11 CONTEXT): all operators are lifetime / windowless in v0.
//!
//! Each geo state owns its `lat_field` / `lon_field` name (captured at register
//! time) so the apply loop does not need to thread the descriptor params
//! through every `update` call.

use crate::row::{Row, Value};
use haversine::{distance, Location, Units};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ─── helpers ─────────────────────────────────────────────────────────────────

fn read_lat_lon(row: &Row, lat_field: &str, lon_field: &str) -> Option<(f64, f64)> {
    let lat = match row.get(lat_field)? {
        Value::F64(v) => *v,
        Value::I64(v) => *v as f64,
        _ => return None,
    };
    let lon = match row.get(lon_field)? {
        Value::F64(v) => *v,
        Value::I64(v) => *v as f64,
        _ => return None,
    };
    Some((lat, lon))
}

/// Great-circle distance in km between two `(lat, lon)` pairs.
pub fn haversine_km(p1: (f64, f64), p2: (f64, f64)) -> f64 {
    distance(
        Location {
            latitude: p1.0,
            longitude: p1.1,
        },
        Location {
            latitude: p2.0,
            longitude: p2.1,
        },
        Units::Kilometers,
    )
}

// ─── GeoVelocityState (AGG-GEO-01) ───────────────────────────────────────────

/// Maximum implied speed (km/h) between consecutive events for an entity.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GeoVelocityState {
    pub lat_field: String,
    pub lon_field: String,
    pub prev: Option<(f64, f64, i64)>,
    pub max_kmh: f64,
}

impl GeoVelocityState {
    pub fn with_fields(lat_field: String, lon_field: String) -> Self {
        Self {
            lat_field,
            lon_field,
            prev: None,
            max_kmh: 0.0,
        }
    }

    pub fn update(&mut self, row: &Row, event_time_ms: i64, where_matched: bool) {
        if !where_matched {
            return;
        }
        let Some((lat, lon)) = read_lat_lon(row, &self.lat_field, &self.lon_field) else {
            return;
        };
        if let Some((plat, plon, pt)) = self.prev {
            let dt_ms = event_time_ms - pt;
            if dt_ms > 0 {
                let km = haversine_km((plat, plon), (lat, lon));
                let kmh = km / (dt_ms as f64 / 3_600_000.0);
                if kmh > self.max_kmh {
                    self.max_kmh = kmh;
                }
            }
        }
        self.prev = Some((lat, lon, event_time_ms));
    }

    pub fn query(&self) -> Value {
        if self.prev.is_none() {
            Value::Null
        } else {
            Value::F64(self.max_kmh)
        }
    }
}

// ─── GeoDistanceState (AGG-GEO-02) ───────────────────────────────────────────

/// Total path length (km) traversed by an entity across consecutive events.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GeoDistanceState {
    pub lat_field: String,
    pub lon_field: String,
    pub prev: Option<(f64, f64)>,
    pub total_km: f64,
}

impl GeoDistanceState {
    pub fn with_fields(lat_field: String, lon_field: String) -> Self {
        Self {
            lat_field,
            lon_field,
            prev: None,
            total_km: 0.0,
        }
    }

    pub fn update(&mut self, row: &Row, where_matched: bool) {
        if !where_matched {
            return;
        }
        let Some((lat, lon)) = read_lat_lon(row, &self.lat_field, &self.lon_field) else {
            return;
        };
        if let Some(prev) = self.prev {
            self.total_km += haversine_km(prev, (lat, lon));
        }
        self.prev = Some((lat, lon));
    }

    pub fn query(&self) -> Value {
        Value::F64(self.total_km)
    }
}

// ─── GeoSpreadState (AGG-GEO-03) ─────────────────────────────────────────────

/// Maximum distance (km) of any observed event from the running mean centre.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GeoSpreadState {
    pub lat_field: String,
    pub lon_field: String,
    pub n: u64,
    pub mean_lat: f64,
    pub mean_lon: f64,
    pub max_km: f64,
    /// Keep all observed points so max recomputes correctly when the mean moves.
    /// Bounded scaling: 16 bytes/sample → 16MB at 1M samples per entity (acceptable
    /// for v0 capacity envelope; downsample sketch deferred to v0.1).
    pub samples: Vec<(f64, f64)>,
}

impl GeoSpreadState {
    pub fn with_fields(lat_field: String, lon_field: String) -> Self {
        Self {
            lat_field,
            lon_field,
            ..Default::default()
        }
    }

    pub fn update(&mut self, row: &Row, where_matched: bool) {
        if !where_matched {
            return;
        }
        let Some((lat, lon)) = read_lat_lon(row, &self.lat_field, &self.lon_field) else {
            return;
        };
        self.n += 1;
        let inv_n = 1.0 / self.n as f64;
        self.mean_lat += (lat - self.mean_lat) * inv_n;
        self.mean_lon += (lon - self.mean_lon) * inv_n;
        self.samples.push((lat, lon));
        let mean = (self.mean_lat, self.mean_lon);
        let mut new_max = 0.0_f64;
        for &p in &self.samples {
            let d = haversine_km(p, mean);
            if d > new_max {
                new_max = d;
            }
        }
        self.max_km = new_max;
    }

    pub fn query(&self) -> Value {
        if self.n == 0 {
            Value::Null
        } else {
            Value::F64(self.max_km)
        }
    }
}

// ─── UniqueCellsState (AGG-GEO-04) ───────────────────────────────────────────

/// Distinct grid cells visited by an entity. Equirectangular cell encoding:
/// `(floor(lat * precision), floor(lon * precision))` (i32 pairs).
///
/// precision examples (degrees per cell):
/// - precision = 1   → 1° cell ≈ 111 km
/// - precision = 10  → 0.1° cell ≈ 11 km
/// - precision = 100 → 0.01° cell ≈ 1.1 km
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueCellsState {
    pub lat_field: String,
    pub lon_field: String,
    pub precision: u32,
    pub cells: BTreeMap<(i32, i32), u64>,
}

impl UniqueCellsState {
    pub fn new(precision: u32) -> Self {
        Self {
            lat_field: String::new(),
            lon_field: String::new(),
            precision: precision.max(1),
            cells: BTreeMap::new(),
        }
    }

    pub fn with_fields(lat_field: String, lon_field: String, precision: u32) -> Self {
        Self {
            lat_field,
            lon_field,
            precision: precision.max(1),
            cells: BTreeMap::new(),
        }
    }

    pub(crate) fn cell_id(precision: u32, lat: f64, lon: f64) -> (i32, i32) {
        let p = precision as f64;
        ((lat * p).floor() as i32, (lon * p).floor() as i32)
    }

    pub fn update(&mut self, row: &Row, where_matched: bool) {
        if !where_matched {
            return;
        }
        let Some((lat, lon)) = read_lat_lon(row, &self.lat_field, &self.lon_field) else {
            return;
        };
        let cell = Self::cell_id(self.precision, lat, lon);
        *self.cells.entry(cell).or_insert(0) += 1;
    }

    pub fn query(&self) -> Value {
        Value::I64(self.cells.len() as i64)
    }
}

// ─── GeoEntropyState (AGG-GEO-05) ────────────────────────────────────────────

/// Shannon entropy (bits) over the distribution of grid-cell visits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoEntropyState {
    pub lat_field: String,
    pub lon_field: String,
    pub precision: u32,
    pub cells: BTreeMap<(i32, i32), u64>,
    pub total: u64,
}

impl GeoEntropyState {
    pub fn new(precision: u32) -> Self {
        Self {
            lat_field: String::new(),
            lon_field: String::new(),
            precision: precision.max(1),
            cells: BTreeMap::new(),
            total: 0,
        }
    }

    pub fn with_fields(lat_field: String, lon_field: String, precision: u32) -> Self {
        Self {
            lat_field,
            lon_field,
            precision: precision.max(1),
            cells: BTreeMap::new(),
            total: 0,
        }
    }

    pub fn update(&mut self, row: &Row, where_matched: bool) {
        if !where_matched {
            return;
        }
        let Some((lat, lon)) = read_lat_lon(row, &self.lat_field, &self.lon_field) else {
            return;
        };
        let cell = UniqueCellsState::cell_id(self.precision, lat, lon);
        *self.cells.entry(cell).or_insert(0) += 1;
        self.total += 1;
    }

    pub fn query(&self) -> Value {
        if self.total == 0 {
            return Value::Null;
        }
        let denom = self.total as f64;
        let mut h = 0.0_f64;
        for &c in self.cells.values() {
            if c == 0 {
                continue;
            }
            let p = c as f64 / denom;
            h -= p * p.log2();
        }
        Value::F64(h)
    }
}

// ─── DistanceFromHomeState (AGG-GEO-06) ──────────────────────────────────────

/// Distance (km) of the *current* event from the running centroid of the last
/// `samples` events for this entity.
///
/// Per Phase 11 CONTEXT D-03 (top_k Phase-10 fallback): centroid is the
/// arithmetic mean of the last-N (lat, lon) circular buffer. Once Phase 10's
/// `top_k` lands, swap to top-K most-frequent-cell centroid.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistanceFromHomeState {
    pub lat_field: String,
    pub lon_field: String,
    pub samples: usize,
    pub buf: Vec<(f64, f64)>,
    pub head: usize,
    pub filled: bool,
    pub last: Option<(f64, f64)>,
}

impl DistanceFromHomeState {
    pub fn new(samples: usize) -> Self {
        Self {
            lat_field: String::new(),
            lon_field: String::new(),
            samples: samples.max(1),
            buf: Vec::with_capacity(samples.max(1)),
            head: 0,
            filled: false,
            last: None,
        }
    }

    pub fn with_fields(lat_field: String, lon_field: String, samples: usize) -> Self {
        Self {
            lat_field,
            lon_field,
            samples: samples.max(1),
            buf: Vec::with_capacity(samples.max(1)),
            head: 0,
            filled: false,
            last: None,
        }
    }

    pub fn update(&mut self, row: &Row, where_matched: bool) {
        if !where_matched {
            return;
        }
        let Some((lat, lon)) = read_lat_lon(row, &self.lat_field, &self.lon_field) else {
            return;
        };
        if !self.filled {
            self.buf.push((lat, lon));
            if self.buf.len() == self.samples {
                self.filled = true;
                self.head = 0;
            }
        } else {
            self.buf[self.head] = (lat, lon);
            self.head = (self.head + 1) % self.samples;
        }
        self.last = Some((lat, lon));
    }

    pub fn query(&self) -> Value {
        let Some(last) = self.last else {
            return Value::Null;
        };
        if self.buf.is_empty() {
            return Value::Null;
        }
        let n = self.buf.len() as f64;
        let mean_lat: f64 = self.buf.iter().map(|p| p.0).sum::<f64>() / n;
        let mean_lon: f64 = self.buf.iter().map(|p| p.1).sum::<f64>() / n;
        Value::F64(haversine_km(last, (mean_lat, mean_lon)))
    }
}

// ─── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn row_geo(lat: f64, lon: f64) -> Row {
        Row::new()
            .with_field("lat", Value::F64(lat))
            .with_field("lon", Value::F64(lon))
    }

    /// Cite SC2 — verify against published Haversine distance values.
    /// Distance NYC (40.7128, -74.0060) → London (51.5074, -0.1278) ≈ 5570 km.
    #[test]
    fn haversine_nyc_to_london_matches_published() {
        let nyc = (40.7128, -74.0060);
        let lon = (51.5074, -0.1278);
        let d = haversine_km(nyc, lon);
        // Published: 5570 km. Allow ±20 km tolerance.
        assert!(
            (d - 5570.0).abs() < 20.0,
            "expected ~5570 km, got {d} (haversine crate)"
        );
    }

    // ── GeoVelocityState ─────────────────────────────────────────────────────

    #[test]
    fn geo_velocity_records_max_kmh_between_events() {
        let mut s = GeoVelocityState::with_fields("lat".into(), "lon".into());
        // event 1 @ t=0 NYC
        s.update(&row_geo(40.7128, -74.0060), 0, true);
        // event 2 @ t=3_600_000 (1h later) 1° north → ~111 km/h
        s.update(&row_geo(41.7128, -74.0060), 3_600_000, true);
        let v = s.query();
        if let Value::F64(kmh) = v {
            assert!((kmh - 111.0).abs() < 2.0, "expected ~111 km/h, got {kmh}");
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn geo_velocity_returns_null_with_no_events() {
        let s = GeoVelocityState::with_fields("lat".into(), "lon".into());
        assert_eq!(s.query(), Value::Null);
    }

    // ── GeoDistanceState ─────────────────────────────────────────────────────

    #[test]
    fn geo_distance_sums_path_segments() {
        let mut s = GeoDistanceState::with_fields("lat".into(), "lon".into());
        s.update(&row_geo(0.0, 0.0), true);
        s.update(&row_geo(0.0, 1.0), true); // ~111 km east
        s.update(&row_geo(0.0, 2.0), true); // ~111 km east
        let d = match s.query() {
            Value::F64(x) => x,
            _ => panic!(),
        };
        assert!((d - 222.0).abs() < 5.0, "expected ~222 km path, got {d}");
    }

    // ── GeoSpreadState ───────────────────────────────────────────────────────

    // Phase 19.1.2-01: GeoSpread now returns RMS dispersion (km), not max distance.
    // The 4 corners of a unit square at lat 0 happen to coincide between the two
    // semantics (~78.7 km), so this test is preserved as a smoke baseline that
    // exercises both old & new code paths to the same number.
    #[test]
    fn geo_spread_unit_square_at_origin_smoke() {
        let mut s = GeoSpreadState::with_fields("lat".into(), "lon".into());
        s.update(&row_geo(0.5, 0.5), true);
        s.update(&row_geo(0.5, -0.5), true);
        s.update(&row_geo(-0.5, 0.5), true);
        s.update(&row_geo(-0.5, -0.5), true);
        let d = match s.query() {
            Value::F64(x) => x,
            _ => panic!(),
        };
        // For 4 corners at (±0.5, ±0.5) with mean (0,0):
        // - old max-from-mean: each corner is ~78.6 km away → 78.6
        // - new RMS: sqrt(var_lat_km² + var_lon_km²) = sqrt(0.25 * 111.32² * 2) ≈ 78.7
        assert!((d - 78.7).abs() < 1.0, "expected ~78.7 km, got {d}");
    }

    // Phase 19.1.2-01 RED tests — Welford RMS semantics.

    /// New contract: variance is undefined for n<2; query returns Null.
    /// Old contract returned F64(0.0) for n=1 (max_km init = 0). RED.
    #[test]
    fn geo_spread_returns_null_for_zero_or_one_event() {
        let mut s = GeoSpreadState::with_fields("lat".into(), "lon".into());
        assert!(matches!(s.query(), Value::Null), "n=0 must be Null");

        s.update(&row_geo(40.7128, -74.0060), true);
        assert!(
            matches!(s.query(), Value::Null),
            "n=1 must be Null (variance undefined); got {:?}",
            s.query()
        );
    }

    /// 4 points at (0,0), (1,0), (0,1), (1,1). Mean = (0.5, 0.5).
    /// var_lat_deg² = var_lon_deg² = 1.0 / 4 = 0.25.
    /// At lat 0.5°, cos ≈ 1.0, so km/deg lon ≈ km/deg lat = 111.32.
    /// var_lat_km² = var_lon_km² ≈ 0.25 * 111.32² ≈ 3098.
    /// RMS = sqrt(2 * 3098) ≈ 78.7 km.
    #[test]
    fn geo_spread_returns_rms_km_for_unit_square() {
        let mut s = GeoSpreadState::with_fields("lat".into(), "lon".into());
        s.update(&row_geo(0.0, 0.0), true);
        s.update(&row_geo(1.0, 0.0), true);
        s.update(&row_geo(0.0, 1.0), true);
        s.update(&row_geo(1.0, 1.0), true);
        let result = match s.query() {
            Value::F64(v) => v,
            other => panic!("expected F64, got {:?}", other),
        };
        assert!(
            (result - 78.7).abs() < 1.0,
            "expected ~78.7 km RMS, got {result}"
        );
    }

    /// Welford RMS is permutation-invariant by construction (combinable across orders).
    /// Old max-from-mean was permutation-invariant too because it re-walked all samples;
    /// this test still locks in the contract for the new impl.
    #[test]
    fn geo_spread_permutation_invariant() {
        // Simple deterministic point set; reverse order should give identical RMS.
        let points: Vec<(f64, f64)> = (0..20)
            .map(|i| {
                let f = i as f64;
                (f * 0.13 - 1.3, f * 0.07 - 0.7)
            })
            .collect();

        let mut s1 = GeoSpreadState::with_fields("lat".into(), "lon".into());
        for &(lat, lon) in &points {
            s1.update(&row_geo(lat, lon), true);
        }
        let r1 = match s1.query() {
            Value::F64(v) => v,
            _ => panic!("forward order returned non-F64"),
        };

        let mut shuffled = points.clone();
        shuffled.reverse();
        let mut s2 = GeoSpreadState::with_fields("lat".into(), "lon".into());
        for &(lat, lon) in &shuffled {
            s2.update(&row_geo(lat, lon), true);
        }
        let r2 = match s2.query() {
            Value::F64(v) => v,
            _ => panic!("reversed order returned non-F64"),
        };

        // Welford has well-known FP rounding under different orders; tolerance 1e-6.
        assert!(
            (r1 - r2).abs() < 1e-6,
            "permutation invariance broken: forward={r1} reversed={r2}"
        );
    }

    /// Scaling all displacements by k must scale the RMS by k.
    /// Welford variance is exactly homogeneous of degree 2; sqrt → degree 1 in scatter.
    /// Old max-from-mean was approximately linear in small scatter (haversine ≈
    /// linear for small angles), so this test is a soft RED for old code.
    #[test]
    fn geo_spread_monotone_in_scatter() {
        let mut s_small = GeoSpreadState::with_fields("lat".into(), "lon".into());
        let mut s_large = GeoSpreadState::with_fields("lat".into(), "lon".into());

        for i in 0..10 {
            let lat = (i as f64) * 0.01; // small spread
            let lon = (i as f64) * 0.01;
            s_small.update(&row_geo(lat, lon), true);
            s_large.update(&row_geo(lat * 10.0, lon * 10.0), true); // 10× scatter
        }

        let small = match s_small.query() {
            Value::F64(v) => v,
            _ => panic!("small returned non-F64"),
        };
        let large = match s_large.query() {
            Value::F64(v) => v,
            _ => panic!("large returned non-F64"),
        };

        let ratio = large / small;
        assert!(
            ratio > 9.5 && ratio < 10.5,
            "expected ~10× scatter ratio; small={small} large={large} ratio={ratio}"
        );
    }

    /// 10 points at NYC + 1 outlier at LA. Old code returns max_km ≈ 3700 km
    /// (LA-to-mean is huge); new RMS code returns ≈ 1100 km (variance over coords).
    /// This is a STRONG RED test — the two semantics give very different numbers.
    #[test]
    fn geo_spread_asymmetric_cluster_has_lower_rms_than_max() {
        let mut s = GeoSpreadState::with_fields("lat".into(), "lon".into());
        // 10 NYC events.
        for _ in 0..10 {
            s.update(&row_geo(40.7128, -74.0060), true);
        }
        // 1 LA outlier.
        s.update(&row_geo(34.0522, -118.2437), true);

        let v = match s.query() {
            Value::F64(v) => v,
            _ => panic!("expected F64"),
        };
        // RMS expected: variance of 10 zeros and 1 large deviation, divided by 11,
        // sqrt'd → small fraction of the LA distance. For these points, RMS ≈ 1100 km.
        // Old max-from-mean would be ≈ 3580 km (LA-to-running-mean).
        assert!(
            v < 1500.0,
            "RMS for 10×NYC + 1×LA must be < 1500 km (saw {v}); old max-from-mean returns ~3580"
        );
        assert!(
            v > 500.0,
            "RMS for 10×NYC + 1×LA must be > 500 km (saw {v}); collapsing to 0 means math is broken"
        );
    }

    // ── UniqueCellsState ─────────────────────────────────────────────────────

    #[test]
    fn unique_cells_counts_distinct_cells() {
        let mut s = UniqueCellsState::with_fields("lat".into(), "lon".into(), 10);
        s.update(&row_geo(0.05, 0.05), true);
        s.update(&row_geo(0.07, 0.05), true); // same cell as first
        s.update(&row_geo(1.0, 0.0), true); // different cell
        s.update(&row_geo(2.0, 2.0), true); // different cell
        assert_eq!(s.query(), Value::I64(3));
    }

    // ── GeoEntropyState ──────────────────────────────────────────────────────

    #[test]
    fn geo_entropy_uniform_distribution_high_entropy() {
        let mut s = GeoEntropyState::with_fields("lat".into(), "lon".into(), 10);
        for &(lat, lon) in &[(0.05, 0.05), (1.0, 0.0), (2.0, 2.0), (3.0, 3.0)] {
            s.update(&row_geo(lat, lon), true);
        }
        match s.query() {
            Value::F64(h) => assert!(
                (h - 2.0).abs() < 1e-9,
                "expected H=2.0 bits for uniform 4-cell distribution, got {h}"
            ),
            _ => panic!(),
        }
    }

    #[test]
    fn geo_entropy_single_cell_zero_entropy() {
        let mut s = GeoEntropyState::with_fields("lat".into(), "lon".into(), 10);
        for _ in 0..5 {
            s.update(&row_geo(0.05, 0.05), true);
        }
        match s.query() {
            Value::F64(h) => assert!(h.abs() < 1e-9, "expected H=0 for single cell, got {h}"),
            _ => panic!(),
        }
    }

    // ── DistanceFromHomeState ────────────────────────────────────────────────

    #[test]
    fn distance_from_home_uses_centroid_of_last_n() {
        let mut s = DistanceFromHomeState::with_fields("lat".into(), "lon".into(), 3);
        s.update(&row_geo(0.0, 0.0), true);
        s.update(&row_geo(0.0, 0.1), true);
        s.update(&row_geo(0.1, 0.0), true);
        s.update(&row_geo(1.0, 1.0), true);
        let d = match s.query() {
            Value::F64(x) => x,
            _ => panic!(),
        };
        assert!(d > 50.0 && d < 200.0, "expected 50-200 km, got {d}");
    }

    #[test]
    fn distance_from_home_null_with_no_events() {
        let s = DistanceFromHomeState::with_fields("lat".into(), "lon".into(), 5);
        assert_eq!(s.query(), Value::Null);
    }

    // ── Determinism guard ────────────────────────────────────────────────────

    #[test]
    fn no_systemtime_now_in_geo_module() {
        let forbidden_clock = ["SystemTime", "::", "now"].concat();
        let forbidden_rand = ["rand", "::"].concat();
        let src = include_str!("agg_geo.rs");
        assert!(
            !src.contains(forbidden_clock.as_str()),
            "agg_geo.rs must not use wall-clock reads (D-06)"
        );
        assert!(
            !src.contains(forbidden_rand.as_str()),
            "agg_geo.rs must not use rand crate (D-06)"
        );
    }
}
