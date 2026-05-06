//! Pool=N pre-encoded TCP-frame builder for the standalone bench binaries.
//!
//! 1. **Why Pool=N (not a sampler):** pre-encoding ALL N frames at sender
//!    startup eliminates per-iteration RNG cost AND per-iteration encode
//!    cost from the bench hot loop. The bench-side floor becomes "as fast
//!    as TCP `write_all` can drain" — the server-side ceiling is the only
//!    number we're measuring. Pool memory is ~500 MB-1 GB for N = 1 M;
//!    callers are responsible for sizing against host RAM.
//! 2. **Setup time excluded from `wall_clock_ms`:** [`build_pool_timed`]
//!    returns `(Vec<Bytes>, Duration)` so the caller can subtract pool
//!    setup from any saturation measurement.
//! 3. **All four shapes share this abstraction:** fixed / uniform / zipfian
//!    / mixed are emitted in identical TCP-frame envelopes (`CT_JSON` or
//!    `CT_MSGPACK`) so the ledger rows are directly comparable.
//! 4. **Determinism:** every shape uses `StdRng::seed_from_u64(seed)` and
//!    Zipfian's hybrid sampler is seeded the same way; same-seed runs
//!    produce byte-identical pool output.
//!
//! The module reuses `beava_core::wire::encode_frame` verbatim — zero new
//! wire formats.

use beava_core::wire::{encode_frame, Frame, CT_JSON, CT_MSGPACK, OP_PUSH};
use bytes::{Bytes, BytesMut};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use serde_json::Value;
use std::time::{Duration, Instant};

/// The per-pipeline configuration consumed by the pool builder.
///
/// Field layout matches the binary harness (`src/bin/beava-bench-v18.rs`) so
/// the JSON files under `crates/beava-bench/configs/` deserialize directly
/// into either type without conversion.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct PipelineConfig {
    pub name: String,
    // reason: deserialized from configs/*.json; retained for serde-shape
    // parity with the binary harness even if the blast_shape path doesn't
    // read this field.
    #[allow(dead_code)]
    pub description: String,
    pub register: Value,
    pub event_name: String,
    pub features: Vec<String>,
    pub key_field: String,
    pub extra_fields: serde_json::Map<String, Value>,
}

/// Wire format for the encoded pool. Matches `bin/beava-bench-v18.rs`.
///
/// JSON is the curl/HTTP-default; MessagePack is the production fast-path on
/// TCP for blast benches.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WireFormat {
    Json,
    Msgpack,
}

/// One of the four blast shapes — controls how the pool's per-frame
/// `key_field` and `event_name` are sampled.
#[derive(Copy, Clone, Debug)]
pub enum BlastShape {
    /// One pre-encoded frame, reused N times. Cache-warm marketing peak.
    Fixed,
    /// `key_field` rolls evenly over `cardinality` keys. Cache-pessimistic
    /// floor.
    Uniform { cardinality: u64 },
    /// Zipfian distribution over `cardinality` keys with `alpha` skew.
    /// Realistic fraud workload.
    Zipfian { alpha: f64, cardinality: u64 },
    /// Pool spans `event_count` registered events sampled per push.
    /// Multi-stream realism.
    Mixed { event_count: usize },
}

/// Static configuration passed alongside `BlastShape` into `build_pool`.
pub struct BlastShapeConfig<'a> {
    pub pipeline: &'a PipelineConfig,
    /// For `BlastShape::Mixed` only. Must contain at least `event_count`
    /// names. For other shapes, ignored.
    pub event_names_for_mixed: &'a [&'a str],
    pub wire_format: WireFormat,
    pub seed: u64,
}

/// Errors returned by `build_pool`. None of these are recoverable — the
/// caller is misconfigured.
#[derive(Debug, thiserror::Error)]
pub enum BlastShapeError {
    #[error("BlastShape::Mixed requires at least event_count distinct event names in event_names_for_mixed")]
    MixedRequiresMultipleEvents,
    #[error("Zipfian alpha must be > 0.0")]
    InvalidAlpha,
    #[error("cardinality must be > 0 (Uniform / Zipfian)")]
    InvalidCardinality,
}

/// Deterministic, seedable Zipfian rank sampler.
///
/// Implements the hybrid algorithm from Gray et al. ("Quickly Generating
/// Billion-Record Synthetic Databases", SIGMOD 1994). For `alpha = 1.0` the
/// limiting form `(self.eta * v - self.eta + 1.0).powf(1.0 / (1.0 - alpha))`
/// degenerates (division by zero); we route the alpha=1 case through the
/// log-uniform inverse-CDF instead.
///
/// `sample()` returns ranks in `[0, k)`. Rank 0 is the most frequent.
///
/// Setup is O(k) (it sums `ζ(k, α) = Σ_{i=1..=k} 1/i^α`). For `k = 1 M` this
/// is ~10 ms — fine for the Pool=N use case where the pool builder runs once
/// at sender startup and setup time is captured in [`build_pool_timed`]'s
/// returned `Duration` (excluded from `wall_clock_ms`).
pub struct ZipfianSampler {
    rng: StdRng,
    alpha: f64,
    k: u64,
    /// `ζ(k, α)` — normalising constant for the truncated zeta distribution.
    zetan: f64,
    /// Quasi-uniform cutoff used by the inverse-CDF branch (alpha != 1).
    eta: f64,
    /// Pre-computed `0.5_f64.powf(alpha)` to avoid recomputing inside sample().
    half_alpha: f64,
}

impl ZipfianSampler {
    /// Construct a new sampler over ranks `[0, k)` with skew `alpha` and
    /// PRNG seed `seed`. Panics if `alpha <= 0.0` or `k == 0` — callers are
    /// expected to validate via `BlastShape::*` enums.
    pub fn new(alpha: f64, k: u64, seed: u64) -> Self {
        assert!(alpha > 0.0, "alpha must be > 0");
        assert!(k > 0, "k must be > 0");
        let zetan = zeta(k, alpha);
        let zeta2 = zeta(2, alpha);
        // For alpha == 1, the inverse-CDF formula divides by zero. We still
        // initialise eta to a finite value so debug builds don't trip a
        // NaN-flag; sample() detects alpha == 1 separately and uses a log-
        // uniform inverse instead.
        let eta = if (alpha - 1.0).abs() < f64::EPSILON {
            1.0
        } else {
            (1.0 - (2.0_f64 / k as f64).powf(1.0 - alpha)) / (1.0 - zeta2 / zetan)
        };
        let half_alpha = (0.5_f64).powf(alpha);
        Self {
            rng: StdRng::seed_from_u64(seed),
            alpha,
            k,
            zetan,
            eta,
            half_alpha,
        }
    }

    /// Sample one rank in `[0, k)`. Rank 0 is the most frequent.
    pub fn sample(&mut self) -> u64 {
        let u: f64 = self.rng.gen();
        let uz = u * self.zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + self.half_alpha {
            return 1;
        }
        // For alpha != 1 use Gray et al.'s inverse-CDF approximation; for
        // alpha == 1 (the most common fraud-shape default) use a log-uniform
        // inverse, which is exact for the harmonic-series limit.
        let v: f64 = self.rng.gen();
        let rank_f = if (self.alpha - 1.0).abs() < f64::EPSILON {
            // alpha == 1: cumulative ∝ ln(r); inverse is r = 2 * exp(v *
            // (ln(k) - ln(2))). Anchor at r=2 because we already special-
            // cased r=0 and r=1 above.
            let ln_k = (self.k as f64).ln();
            let ln_2 = 2.0_f64.ln();
            (2.0 * (v * (ln_k - ln_2)).exp()).floor()
        } else {
            (self.k as f64) * (self.eta * v - self.eta + 1.0).powf(1.0 / (1.0 - self.alpha))
        };
        let rank = rank_f as u64;
        rank.min(self.k - 1)
    }
}

/// Truncated zeta function `ζ(n, α) = Σ_{i=1..=n} 1 / i^α`.
fn zeta(n: u64, alpha: f64) -> f64 {
    let mut s = 0.0_f64;
    for i in 1..=n {
        s += 1.0 / (i as f64).powf(alpha);
    }
    s
}

/// Build N pre-encoded TCP frames matching the requested `BlastShape`.
///
/// Each entry is a complete `[u32 length][u16 op][u8 ct][payload]` frame —
/// the same envelope produced by `beava_core::wire::encode_frame` for
/// `OP_PUSH`. Callers can stream the pool out a TCP connection with
/// `write_all(&pool[i])` without touching the encoder again on the hot path.
pub fn build_pool(
    shape: BlastShape,
    cfg: &BlastShapeConfig,
    n: u64,
) -> Result<Vec<Bytes>, BlastShapeError> {
    match shape {
        BlastShape::Mixed { event_count } => {
            if cfg.event_names_for_mixed.len() < event_count {
                return Err(BlastShapeError::MixedRequiresMultipleEvents);
            }
        }
        BlastShape::Zipfian { alpha, cardinality } => {
            if alpha <= 0.0 {
                return Err(BlastShapeError::InvalidAlpha);
            }
            if cardinality == 0 {
                return Err(BlastShapeError::InvalidCardinality);
            }
        }
        BlastShape::Uniform { cardinality } => {
            if cardinality == 0 {
                return Err(BlastShapeError::InvalidCardinality);
            }
        }
        BlastShape::Fixed => {}
    }

    let mut rng = StdRng::seed_from_u64(cfg.seed);
    let mut zipf = if let BlastShape::Zipfian { alpha, cardinality } = shape {
        Some(ZipfianSampler::new(
            alpha,
            cardinality,
            cfg.seed.wrapping_add(0xDEAD),
        ))
    } else {
        None
    };

    let mut pool: Vec<Bytes> = Vec::with_capacity(n as usize);

    // Single reusable encode buffer to amortise allocation across the loop.
    let mut buf = BytesMut::with_capacity(4 * 1024);

    let ct = match cfg.wire_format {
        WireFormat::Json => CT_JSON,
        WireFormat::Msgpack => CT_MSGPACK,
    };

    // Fixed shape: encode the single frame once and clone its Bytes N times.
    // Both a perf-win (no per-frame encode) AND a contract guaranteed to the
    // tests (every entry byte-identical to entry 0).
    if let BlastShape::Fixed = shape {
        let body = build_event_body(
            cfg.pipeline,
            /* key_idx */ 0,
            /* seq */ 0,
            &mut rng,
        );
        let payload = encode_envelope(&cfg.pipeline.event_name, &body, cfg.wire_format);
        let frame = Frame {
            op: OP_PUSH,
            content_type: ct,
            payload: Bytes::from(payload),
        };
        buf.clear();
        encode_frame(&frame, &mut buf);
        let frozen: Bytes = buf.split().freeze();
        for _ in 0..n {
            pool.push(frozen.clone()); // Bytes::clone is a refcount bump
        }
        return Ok(pool);
    }

    for seq in 0..n {
        let key_idx: u64 = match shape {
            BlastShape::Fixed => unreachable!("handled above"),
            BlastShape::Uniform { cardinality } => rng.gen_range(0..cardinality),
            BlastShape::Zipfian { .. } => zipf.as_mut().unwrap().sample(),
            // Mixed varies the EVENT NAME, but key cardinality is the
            // pipeline's default (1 M); no shared key tracking needed.
            BlastShape::Mixed { .. } => rng.gen_range(0..1_000_000_u64),
        };
        let event_name: &str = match shape {
            BlastShape::Mixed { event_count } => {
                let idx = rng.gen_range(0..event_count);
                cfg.event_names_for_mixed[idx]
            }
            _ => &cfg.pipeline.event_name,
        };

        let body = build_event_body(cfg.pipeline, key_idx, seq, &mut rng);
        let payload = encode_envelope(event_name, &body, cfg.wire_format);

        let frame = Frame {
            op: OP_PUSH,
            content_type: ct,
            payload: Bytes::from(payload),
        };
        buf.clear();
        encode_frame(&frame, &mut buf);
        // `split` returns an owned BytesMut we can freeze without copying;
        // the remaining capacity stays on `buf` for the next iteration. No
        // extra allocation per frame.
        pool.push(buf.split().freeze());
    }

    Ok(pool)
}

/// Same as [`build_pool`], but additionally returns the wall-clock time spent
/// building the pool. The caller can subtract this from any `wall_clock_ms`
/// measurement (setup is excluded from saturation reads).
pub fn build_pool_timed(
    shape: BlastShape,
    cfg: &BlastShapeConfig,
    n: u64,
) -> Result<(Vec<Bytes>, Duration), BlastShapeError> {
    let t0 = Instant::now();
    let pool = build_pool(shape, cfg, n)?;
    Ok((pool, t0.elapsed()))
}

/// Build the per-event JSON `body` object that goes inside the envelope.
/// Field set matches `make_event_payload` in `beava-bench-v18.rs`:
/// `{key_field: "k%08u", event_time: i64, ...extra_fields}`.
fn build_event_body(pipeline: &PipelineConfig, key_idx: u64, seq: u64, rng: &mut StdRng) -> Value {
    let mut obj = serde_json::Map::with_capacity(2 + pipeline.extra_fields.len());
    obj.insert(
        pipeline.key_field.clone(),
        Value::String(format!("k{key_idx:08}")),
    );
    obj.insert(
        "event_time".to_string(),
        Value::Number((1_000_000 + seq as i64).into()),
    );
    for (field, ty) in &pipeline.extra_fields {
        let v = match ty.as_str().unwrap_or("f64") {
            "f64" => serde_json::json!(rng.gen_range(0.0..1000.0)),
            "i64" => serde_json::json!(rng.gen_range(0_i64..1_000_000)),
            "str" => serde_json::json!(format!("s{}", rng.gen_range(0..1000))),
            _ => serde_json::json!(0),
        };
        obj.insert(field.clone(), v);
    }
    Value::Object(obj)
}

/// Encode `{event, body}` into either JSON bytes or MessagePack bytes.
fn encode_envelope(event_name: &str, body: &Value, wire_format: WireFormat) -> Vec<u8> {
    match wire_format {
        WireFormat::Json => {
            let envelope = serde_json::json!({ "event": event_name, "body": body });
            serde_json::to_vec(&envelope).expect("json envelope encode")
        }
        WireFormat::Msgpack => {
            #[derive(Serialize)]
            struct Envelope<'a> {
                event: &'a str,
                body: &'a Value,
            }
            let env = Envelope {
                event: event_name,
                body,
            };
            rmp_serde::to_vec_named(&env).expect("msgpack envelope encode")
        }
    }
}
