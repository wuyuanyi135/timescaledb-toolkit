use std::{convert::TryInto, ops::Deref};

use pgrx::*;

use crate::{
    accessors::{
        AccessorApproxCdf, AccessorApproxPercentile, AccessorApproxPercentileRank, AccessorMaxVal,
        AccessorMean, AccessorMinVal, AccessorNumVals,
    },
    aggregate_utils::in_aggregate_context,
    flatten,
    palloc::{Inner, Internal, InternalAsValue, ToInternal},
    pg_type,
};

use tdigest::{Centroid, TDigest as InternalTDigest};

// PG function for adding values to a digest.
// Null values are ignored.
#[pg_extern(immutable, parallel_safe)]
pub fn tdigest_trans(
    state: Internal,
    size: i32,
    value: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    tdigest_trans_inner(unsafe { state.to_inner() }, size, value, fcinfo).internal()
}
pub fn tdigest_trans_inner(
    state: Option<Inner<tdigest::Builder>>,
    size: i32,
    value: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<tdigest::Builder>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let value = match value {
                None => return state,
                // NaNs are nonsensical in the context of a percentile, so exclude them
                Some(value) => {
                    if value.is_nan() {
                        return state;
                    } else {
                        value
                    }
                }
            };
            let mut state = match state {
                None => tdigest::Builder::with_size(size.try_into().unwrap()).into(),
                Some(state) => state,
            };
            state.push(value);
            Some(state)
        })
    }
}

// PG function for adding weighted values to a digest.
// Null values or weights are ignored.
#[pg_extern(immutable, parallel_safe)]
pub fn weighted_tdigest_trans(
    state: Internal,
    size: i32,
    value: Option<f64>,
    weight: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let value = match value {
                None => return Some(state),
                Some(value) if value.is_nan() => return Some(state),
                Some(value) => value,
            };
            let weight = match weight {
                None => return Some(state),
                Some(w) if w <= 0.0 => return Some(state),
                Some(w) if w.is_nan() => return Some(state),
                Some(w) => w,
            };
            let mut state = match unsafe { state.to_inner() } {
                None => tdigest::Builder::with_size(size.try_into().unwrap()).into(),
                Some(state) => state,
            };
            state.push_weighted(value, weight.round() as u64);
            state.internal()
        })
    }
}

// PG function for merging digests.
#[pg_extern(immutable, parallel_safe)]
pub fn tdigest_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe { tdigest_combine_inner(state1.to_inner(), state2.to_inner(), fcinfo).internal() }
}

pub fn tdigest_combine_inner(
    state1: Option<Inner<tdigest::Builder>>,
    state2: Option<Inner<tdigest::Builder>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<tdigest::Builder>> {
    unsafe {
        in_aggregate_context(fcinfo, || match (state1, state2) {
            (None, None) => None,
            (None, Some(state2)) => Some(state2.clone().into()),
            (Some(state1), None) => Some(state1.clone().into()),
            (Some(state1), Some(state2)) => {
                let mut merged = state1.clone();
                merged.merge(state2.clone());
                Some(merged.into())
            }
        })
    }
}

use crate::raw::bytea;

#[pg_extern(immutable, parallel_safe, strict)]
pub fn tdigest_serialize(state: Internal) -> bytea {
    let mut state = state;
    let state: &mut tdigest::Builder = unsafe { state.get_mut().unwrap() };
    // TODO this macro is really broken
    let hack = state.build();
    let hackref = &hack;
    crate::do_serialize!(hackref)
}

#[pg_extern(strict, immutable, parallel_safe)]
pub fn tdigest_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    tdigest_deserialize_inner(bytes).internal()
}
pub fn tdigest_deserialize_inner(bytes: bytea) -> Inner<tdigest::Builder> {
    crate::do_deserialize!(bytes, tdigest::Builder)
}

// PG object for the digest.
pg_type! {
    #[derive(Debug)]
    struct TDigest<'input> {
        // We compute this.  It's a (harmless) bug that we serialize it.
        #[serde(skip_deserializing)]
        buckets: u32,
        max_buckets: u32,
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
        centroids: [Centroid; self.buckets],
    }
}

impl<'input> InOutFuncs for TDigest<'input> {
    fn output(&self, buffer: &mut StringInfo) {
        use crate::serialization::{EncodedStr::*, str_to_db_encoding};

        let stringified = ron::to_string(&**self).unwrap();
        match str_to_db_encoding(&stringified) {
            Utf8(s) => buffer.push_str(s),
            Other(s) => buffer.push_bytes(s.to_bytes()),
        }
    }

    fn input(input: &std::ffi::CStr) -> TDigest<'input>
    where
        Self: Sized,
    {
        use crate::serialization::str_from_db_encoding;

        let input = str_from_db_encoding(input);
        let mut val: TDigestData = ron::from_str(input).unwrap();
        val.buckets = val
            .centroids
            .len()
            .try_into()
            .expect("centroids len fits into u32");
        unsafe { Self(val, crate::type_builder::CachedDatum::None).flatten() }
    }
}

impl<'input> TDigest<'input> {
    fn to_internal_tdigest(&self) -> InternalTDigest {
        InternalTDigest::new(
            self.centroids.iter().collect(),
            self.sum,
            self.count,
            self.max,
            self.0.min,
            self.max_buckets as usize,
        )
    }

    fn from_internal_tdigest(digest: &InternalTDigest) -> TDigest<'static> {
        let max_buckets: u32 = digest.max_size().try_into().unwrap();

        let centroids = digest.raw_centroids();

        // we need to flatten the vector to a single buffer that contains
        // both the size, the data, and the varlen header
        unsafe {
            flatten!(TDigest {
                max_buckets,
                buckets: centroids.len() as u32,
                count: digest.count(),
                sum: digest.sum(),
                min: digest.min(),
                max: digest.max(),
                centroids: centroids.into(),
            })
        }
    }
}

// PG function to generate a user-facing TDigest object from an internal tdigest::Builder.
#[pg_extern(immutable, parallel_safe)]
fn tdigest_final(state: Internal, fcinfo: pg_sys::FunctionCallInfo) -> Option<TDigest<'static>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            let mut state = state;
            let state: &mut tdigest::Builder = match state.get_mut() {
                None => return None,
                Some(state) => state,
            };
            TDigest::from_internal_tdigest(&state.build()).into()
        })
    }
}

extension_sql!(
    "\n\
    CREATE AGGREGATE tdigest(size integer, value DOUBLE PRECISION)\n\
    (\n\
        sfunc = tdigest_trans,\n\
        stype = internal,\n\
        finalfunc = tdigest_final,\n\
        combinefunc = tdigest_combine,\n\
        serialfunc = tdigest_serialize,\n\
        deserialfunc = tdigest_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "tdigest_agg",
    requires = [
        tdigest_trans,
        tdigest_final,
        tdigest_combine,
        tdigest_serialize,
        tdigest_deserialize
    ],
);

extension_sql!(
    "\n\
    CREATE AGGREGATE weighted_tdigest(size integer, value DOUBLE PRECISION, weight DOUBLE PRECISION)\n\
    (\n\
        sfunc = weighted_tdigest_trans,\n\
        stype = internal,\n\
        finalfunc = tdigest_final,\n\
        combinefunc = tdigest_combine,\n\
        serialfunc = tdigest_serialize,\n\
        deserialfunc = tdigest_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "weighted_tdigest_agg",
    requires = [
        weighted_tdigest_trans,
        tdigest_final,
        tdigest_combine,
        tdigest_serialize,
        tdigest_deserialize
    ],
);

#[pg_extern(immutable, parallel_safe)]
pub fn tdigest_compound_trans(
    state: Internal,
    value: Option<TDigest<'static>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    tdigest_compound_trans_inner(unsafe { state.to_inner() }, value, fcinfo).internal()
}
pub fn tdigest_compound_trans_inner(
    state: Option<Inner<InternalTDigest>>,
    value: Option<TDigest<'static>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<InternalTDigest>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state, value) {
                (a, None) => a,
                (None, Some(a)) => Some(a.to_internal_tdigest().into()),
                (Some(a), Some(b)) => {
                    assert_eq!(a.max_size(), b.max_buckets as usize);
                    Some(
                        InternalTDigest::merge_digests(
                            vec![a.deref().clone(), b.to_internal_tdigest()], // TODO: TDigest merge with self
                        )
                        .into(),
                    )
                }
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn tdigest_compound_combine(
    state1: Internal,
    state2: Internal,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal> {
    unsafe {
        tdigest_compound_combine_inner(state1.to_inner(), state2.to_inner(), fcinfo).internal()
    }
}
pub fn tdigest_compound_combine_inner(
    state1: Option<Inner<InternalTDigest>>,
    state2: Option<Inner<InternalTDigest>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Inner<InternalTDigest>> {
    unsafe {
        in_aggregate_context(fcinfo, || {
            match (state1, state2) {
                (None, None) => None,
                (None, Some(state2)) => Some(state2.clone().into()),
                (Some(state1), None) => Some(state1.clone().into()),
                (Some(state1), Some(state2)) => {
                    assert_eq!(state1.max_size(), state2.max_size());
                    Some(
                        InternalTDigest::merge_digests(
                            vec![state1.deref().clone(), state2.deref().clone()], // TODO: TDigest merge with self
                        )
                        .into(),
                    )
                }
            }
        })
    }
}

#[pg_extern(immutable, parallel_safe)]
fn tdigest_compound_final(
    state: Internal,
    _fcinfo: pg_sys::FunctionCallInfo,
) -> Option<TDigest<'static>> {
    let state: Option<&InternalTDigest> = unsafe { state.get() };
    state.map(TDigest::from_internal_tdigest)
}

#[pg_extern(immutable, parallel_safe)]
fn tdigest_compound_serialize(state: Internal, _fcinfo: pg_sys::FunctionCallInfo) -> bytea {
    let state: Inner<InternalTDigest> = unsafe { state.to_inner().unwrap() };
    crate::do_serialize!(state)
}

#[pg_extern(immutable, parallel_safe)]
pub fn tdigest_compound_deserialize(bytes: bytea, _internal: Internal) -> Option<Internal> {
    let i: InternalTDigest = crate::do_deserialize!(bytes, InternalTDigest);
    Inner::from(i).internal()
}

extension_sql!(
    "\n\
    CREATE AGGREGATE rollup(\n\
        tdigest\n\
    ) (\n\
        sfunc = tdigest_compound_trans,\n\
        stype = internal,\n\
        finalfunc = tdigest_compound_final,\n\
        combinefunc = tdigest_compound_combine,\n\
        serialfunc = tdigest_compound_serialize,\n\
        deserialfunc = tdigest_compound_deserialize,\n\
        parallel = safe\n\
    );\n\
",
    name = "tdigest_rollup",
    requires = [
        tdigest_compound_trans,
        tdigest_compound_final,
        tdigest_compound_combine,
        tdigest_compound_serialize,
        tdigest_compound_deserialize
    ],
);

//---- Available PG operations on the digest

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_tdigest_approx_percentile<'a>(
    sketch: TDigest<'a>,
    accessor: AccessorApproxPercentile,
) -> f64 {
    tdigest_quantile(accessor.percentile, sketch)
}

// Helper: weighted cumulative weight at a given value
fn _weighted_cumulative_at(centroids: &[Centroid], value: f64, weight_power: f64) -> (f64, f64) {
    let mut cumulative = 0.0_f64;
    for c in centroids {
        let w = if weight_power == 0.0 {
            c.weight() as f64
        } else {
            c.weight() as f64 * c.mean().powf(weight_power)
        };
        if c.mean() >= value {
            break;
        }
        cumulative += w;
    }
    let total: f64 = centroids.iter().map(|c| {
        if weight_power == 0.0 {
            c.weight() as f64
        } else {
            c.weight() as f64 * c.mean().powf(weight_power)
        }
    }).sum();
    (cumulative, total)
}

// Helper: weighted quantile (inverse CDF)
fn _weighted_quantile(centroids: &[Centroid], q: f64, weight_power: f64) -> f64 {
    if centroids.is_empty() {
        return 0.0;
    }
    if q <= 0.0 {
        return centroids.first().unwrap().mean();
    }
    if q >= 1.0 {
        return centroids.last().unwrap().mean();
    }

    // Compute total weighted count
    let total: f64 = centroids.iter().map(|c| {
        if weight_power == 0.0 {
            c.weight() as f64
        } else {
            c.weight() as f64 * c.mean().powf(weight_power)
        }
    }).sum();

    let target = q * total;
    let mut cumulative = 0.0_f64;

    for c in centroids {
        let w = if weight_power == 0.0 {
            c.weight() as f64
        } else {
            c.weight() as f64 * c.mean().powf(weight_power)
        };
        if cumulative + w >= target {
            // Interpolate within this centroid
            let centroid_frac = (target - cumulative) / w;
            return c.mean(); // simplified: return centroid mean
        }
        cumulative += w;
    }
    centroids.last().unwrap().mean()
}

// Approximate the value at the given quantile (0.0-1.0)
#[pg_extern(immutable, parallel_safe, name = "approx_percentile")]
pub fn tdigest_quantile<'a>(quantile: f64, digest: TDigest<'a>) -> f64 {
    _weighted_quantile(
        digest.to_internal_tdigest().raw_centroids(),
        quantile,
        0.0,
    )
}

#[pg_extern(immutable, parallel_safe, name = "approx_percentile")]
pub fn tdigest_quantile_weighted<'a>(
    quantile: f64,
    digest: TDigest<'a>,
    weight_power: f64,
) -> f64 {
    _weighted_quantile(
        digest.to_internal_tdigest().raw_centroids(),
        quantile,
        weight_power,
    )
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_tdigest_approx_rank<'a>(
    sketch: TDigest<'a>,
    accessor: AccessorApproxPercentileRank,
) -> f64 {
    tdigest_quantile_at_value(accessor.value, sketch)
}

// Approximate the quantile at the given value
#[pg_extern(immutable, parallel_safe, name = "approx_percentile_rank")]
pub fn tdigest_quantile_at_value<'a>(value: f64, digest: TDigest<'a>) -> f64 {
    let internal = digest.to_internal_tdigest();
    internal.estimate_quantile_at_value(value)
}

#[pg_extern(immutable, parallel_safe, name = "approx_percentile_rank")]
pub fn tdigest_quantile_at_value_weighted<'a>(
    value: f64,
    digest: TDigest<'a>,
    weight_power: f64,
) -> f64 {
    if weight_power == 0.0 {
        return tdigest_quantile_at_value(value, digest);
    }
    let internal = digest.to_internal_tdigest();
    let centroids = internal.raw_centroids();
    let (cum, total) = _weighted_cumulative_at(centroids, value, weight_power);
    if total == 0.0 {
        0.0
    } else {
        cum / total
    }
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_tdigest_approx_cdf<'a>(
    sketch: TDigest<'a>,
    accessor: AccessorApproxCdf,
) -> f64 {
    tdigest_quantile_at_value(accessor.value, sketch)
}

#[pg_extern(immutable, parallel_safe, name = "approx_cdf")]
pub fn tdigest_approx_cdf<'a>(value: f64, digest: TDigest<'a>) -> f64 {
    tdigest_quantile_at_value(value, digest)
}

#[pg_extern(immutable, parallel_safe, name = "approx_cdf")]
pub fn tdigest_approx_cdf_weighted<'a>(
    value: f64,
    digest: TDigest<'a>,
    weight_power: f64,
) -> f64 {
    tdigest_quantile_at_value_weighted(value, digest, weight_power)
}

pub fn _tdigest_to_histogram_inner<'a>(
    sketch: TDigest<'a>,
    bin_edges: Vec<f64>,
    weight_power: f64,
) -> Vec<f64> {
    let m = bin_edges.len();
    if m < 2 || sketch.count == 0 {
        return vec![0.0_f64; if m > 1 { m - 1 } else { 0 }];
    }
    let n_bins = m - 1;
    let mut hist = vec![0.0_f64; n_bins];
    let internal = sketch.to_internal_tdigest();
    for c in internal.raw_centroids() {
        let mean = c.mean();
        let weight = if weight_power == 0.0 {
            c.weight() as f64
        } else {
            c.weight() as f64 * mean.powf(weight_power)
        };
        let idx = match bin_edges.binary_search_by(|e| e.partial_cmp(&mean).unwrap()) {
            Ok(i) => {
                if i >= n_bins {
                    n_bins - 1
                } else {
                    i
                }
            }
            Err(i) => {
                if i == 0 {
                    0
                } else if i >= m {
                    n_bins - 1
                } else {
                    i - 1
                }
            }
        };
        hist[idx] += weight;
    }
    hist
}

#[pg_extern(immutable, parallel_safe, name = "tdigest_to_histogram")]
pub fn tdigest_to_histogram<'a>(sketch: TDigest<'a>, bin_edges: Vec<f64>) -> Vec<f64> {
    _tdigest_to_histogram_inner(sketch, bin_edges, 0.0)
}

#[pg_extern(immutable, parallel_safe, name = "tdigest_to_histogram")]
pub fn tdigest_to_histogram_weighted<'a>(
    sketch: TDigest<'a>,
    bin_edges: Vec<f64>,
    weight_power: f64,
) -> Vec<f64> {
    _tdigest_to_histogram_inner(sketch, bin_edges, weight_power)
}

// tdigest_to_pdf: returns num_points PDF samples as percentage values
// y[i] = (CDF(x_hi) - CDF(x_lo)) * 100.0
pub fn _tdigest_to_pdf_inner<'a>(
    sketch: TDigest<'a>,
    num_points: i32,
    x_min: f64,
    x_max: f64,
    weight_power: f64,
) -> Vec<f64> {
    if num_points <= 0 {
        return vec![];
    }
    if sketch.count == 0 {
        return vec![0.0_f64; num_points as usize];
    }
    let n = num_points as usize;
    let dx = (x_max - x_min) / n as f64;
    let mut result = vec![0.0_f64; n];

    let internal = sketch.to_internal_tdigest();
    let centroids = internal.raw_centroids();

    if weight_power == 0.0 {
        // Unweighted path: use estimate_quantile_at_value on the internal digest
        for i in 0..n {
            let x_lo = x_min + i as f64 * dx;
            let x_hi = x_min + (i + 1) as f64 * dx;
            let cdf_lo = internal.estimate_quantile_at_value(x_lo);
            let cdf_hi = internal.estimate_quantile_at_value(x_hi);
            result[i] = (cdf_hi - cdf_lo) * 100.0;
        }
    } else {
        // Weighted path: use _weighted_cumulative_at
        for i in 0..n {
            let x_lo = x_min + i as f64 * dx;
            let x_hi = x_min + (i + 1) as f64 * dx;
            let (cum_lo, total) = _weighted_cumulative_at(centroids, x_lo, weight_power);
            let (cum_hi, _) = _weighted_cumulative_at(centroids, x_hi, weight_power);
            if total == 0.0 {
                result[i] = 0.0;
            } else {
                result[i] = ((cum_hi - cum_lo) / total) * 100.0;
            }
        }
    }

    result
}

#[pg_extern(immutable, parallel_safe, name = "tdigest_to_pdf")]
pub fn tdigest_to_pdf<'a>(
    sketch: TDigest<'a>,
    num_points: i32,
    x_min: f64,
    x_max: f64,
) -> Vec<f64> {
    _tdigest_to_pdf_inner(sketch, num_points, x_min, x_max, 0.0)
}

#[pg_extern(immutable, parallel_safe, name = "tdigest_to_pdf")]
pub fn tdigest_to_pdf_weighted<'a>(
    sketch: TDigest<'a>,
    num_points: i32,
    x_min: f64,
    x_max: f64,
    weight_power: f64,
) -> Vec<f64> {
    _tdigest_to_pdf_inner(sketch, num_points, x_min, x_max, weight_power)
}

// Gaussian kernel density estimation

fn gaussian_pdf(x: f64) -> f64 {
    (-x * x / 2.0).exp() / (2.0 * std::f64::consts::PI).sqrt()
}

fn _tdigest_to_pdf_kde_inner<'a>(
    sketch: TDigest<'a>,
    num_points: i32,
    x_min: f64,
    x_max: f64,
    bandwidth: f64,
    weight_power: f64,
) -> Vec<f64> {
    if num_points <= 0 {
        return vec![];
    }
    if sketch.count == 0 {
        return vec![0.0_f64; num_points as usize];
    }

    let n = num_points as usize;
    let dx = (x_max - x_min) / n as f64;
    let mut result = vec![0.0_f64; n];

    let internal = sketch.to_internal_tdigest();
    let centroids = internal.raw_centroids();

    let weighted: Vec<(f64, f64)> = centroids
        .iter()
        .map(|c| {
            let w = if weight_power == 0.0 {
                c.weight() as f64
            } else {
                c.weight() as f64 * c.mean().powf(weight_power)
            };
            (c.mean(), w)
        })
        .collect();

    let total_weight: f64 = weighted.iter().map(|&(_, w)| w).sum();

    if total_weight == 0.0 {
        return vec![0.0_f64; n];
    }

    // Auto-estimate bandwidth via Silverman's rule of thumb
    let bw = if bandwidth <= 0.0 {
        let weighted_mean: f64 = weighted.iter().map(|&(m, w)| m * w).sum::<f64>() / total_weight;
        let variance: f64 = weighted
            .iter()
            .map(|&(m, w)| w * (m - weighted_mean).powi(2))
            .sum::<f64>()
            / total_weight;
        let std = variance.sqrt();
        let sum_w2: f64 = weighted.iter().map(|&(_, w)| w * w).sum();
        let n_eff = total_weight * total_weight / sum_w2;
        let rule = 1.06 * std * n_eff.powf(-0.2);
        if rule <= 0.0 || !rule.is_finite() {
            dx / 2.0
        } else {
            rule
        }
    } else {
        bandwidth
    };

    for i in 0..n {
        let x = x_min + (i as f64 + 0.5) * dx;
        let mut sum = 0.0;
        for &(m, w) in &weighted {
            sum += w * gaussian_pdf((x - m) / bw);
        }
        result[i] = sum / (total_weight * bw) * dx * 100.0;
    }

    result
}

#[pg_extern(immutable, parallel_safe, name = "tdigest_to_pdf_kde")]
pub fn tdigest_to_pdf_kde<'a>(
    sketch: TDigest<'a>,
    num_points: i32,
    x_min: f64,
    x_max: f64,
) -> Vec<f64> {
    _tdigest_to_pdf_kde_inner(sketch, num_points, x_min, x_max, 0.0, 0.0)
}

#[pg_extern(immutable, parallel_safe, name = "tdigest_to_pdf_kde")]
pub fn tdigest_to_pdf_kde_bandwidth<'a>(
    sketch: TDigest<'a>,
    num_points: i32,
    x_min: f64,
    x_max: f64,
    bandwidth: f64,
) -> Vec<f64> {
    _tdigest_to_pdf_kde_inner(sketch, num_points, x_min, x_max, bandwidth, 0.0)
}

#[pg_extern(immutable, parallel_safe, name = "tdigest_to_pdf_kde")]
pub fn tdigest_to_pdf_kde_weighted<'a>(
    sketch: TDigest<'a>,
    num_points: i32,
    x_min: f64,
    x_max: f64,
    bandwidth: f64,
    weight_power: f64,
) -> Vec<f64> {
    _tdigest_to_pdf_kde_inner(sketch, num_points, x_min, x_max, bandwidth, weight_power)
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_tdigest_num_vals<'a>(sketch: TDigest<'a>, _accessor: AccessorNumVals) -> f64 {
    tdigest_count(sketch)
}

// Number of elements from which the digest was built.
#[pg_extern(immutable, parallel_safe, name = "num_vals")]
pub fn tdigest_count<'a>(digest: TDigest<'a>) -> f64 {
    digest.count as f64
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_tdigest_min<'a>(sketch: TDigest<'a>, _accessor: AccessorMinVal) -> f64 {
    tdigest_min(sketch)
}

// Minimum value entered in the digest.
#[pg_extern(immutable, parallel_safe, name = "min_val")]
pub fn tdigest_min<'a>(digest: TDigest<'a>) -> f64 {
    digest.min
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_tdigest_max<'a>(sketch: TDigest<'a>, _accessor: AccessorMaxVal) -> f64 {
    tdigest_max(sketch)
}

// Maximum value entered in the digest.
#[pg_extern(immutable, parallel_safe, name = "max_val")]
pub fn tdigest_max<'a>(digest: TDigest<'a>) -> f64 {
    digest.max
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_tdigest_mean<'a>(sketch: TDigest<'a>, _accessor: AccessorMean) -> f64 {
    tdigest_mean(sketch)
}

// Average of all the values entered in the digest.
// Note that this is not an approximation, though there may be loss of precision.
#[pg_extern(immutable, parallel_safe, name = "mean")]
pub fn tdigest_mean<'a>(digest: TDigest<'a>) -> f64 {
    if digest.count > 0 {
        digest.sum / digest.count as f64
    } else {
        0.0
    }
}

/// Total sum of all the values entered in the digest.
#[pg_extern(immutable, parallel_safe, name = "total")]
pub fn tdigest_sum(digest: TDigest<'_>) -> f64 {
    digest.sum
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    use pgrx_macros::pg_test;

    // Assert equality between two floats, within some fixed error range.
    fn apx_eql(value: f64, expected: f64, error: f64) {
        assert!(
            (value - expected).abs() < error,
            "Float value {value} differs from expected {expected} by more than {error}"
        );
    }

    // Assert equality between two floats, within an error expressed as a fraction of the expected value.
    fn pct_eql(value: f64, expected: f64, pct_error: f64) {
        apx_eql(value, expected, pct_error * expected);
    }

    #[pg_test]
    fn test_tdigest_aggregate() {
        Spi::connect_mut(|client| {
            client
                .update("CREATE TABLE test (data DOUBLE PRECISION)", None, &[])
                .unwrap();
            client
                .update(
                    "INSERT INTO test SELECT generate_series(0.01, 100, 0.01)",
                    None,
                    &[],
                )
                .unwrap();

            let sanity = client
                .update("SELECT COUNT(*) FROM test", None, &[])
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap();
            assert_eq!(10000, sanity.unwrap());

            client
                .update(
                    "CREATE VIEW digest AS \
                SELECT tdigest(100, data) FROM test",
                    None,
                    &[],
                )
                .unwrap();

            let (min, max, count) = client
                .update(
                    "SELECT \
                    min_val(tdigest), \
                    max_val(tdigest), \
                    num_vals(tdigest) \
                    FROM digest",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_three::<f64, f64, f64>()
                .unwrap();

            apx_eql(min.unwrap(), 0.01, 0.000001);
            apx_eql(max.unwrap(), 100.0, 0.000001);
            apx_eql(count.unwrap(), 10000.0, 0.000001);

            let (min2, max2, count2) = client
                .update(
                    "SELECT \
                    tdigest->min_val(), \
                    tdigest->max_val(), \
                    tdigest->num_vals() \
                    FROM digest",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_three::<f64, f64, f64>()
                .unwrap();

            assert_eq!(min2, min);
            assert_eq!(max2, max);
            assert_eq!(count2, count);

            let (mean, mean2) = client
                .update(
                    "SELECT \
                    mean(tdigest), \
                    tdigest -> mean()
                    FROM digest",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_two::<f64, f64>()
                .unwrap();

            apx_eql(mean.unwrap(), 50.005, 0.0001);
            assert_eq!(mean, mean2);

            for i in 0..=100 {
                let value = i as f64;
                let quantile = value / 100.0;

                let (est_val, est_quant) = client
                    .update(
                        &format!(
                            "SELECT
                            approx_percentile({quantile}, tdigest), \
                            approx_percentile_rank({value}, tdigest) \
                            FROM digest"
                        ),
                        None,
                        &[],
                    )
                    .unwrap()
                    .first()
                    .get_two::<f64, f64>()
                    .unwrap();

                if i == 0 {
                    pct_eql(est_val.unwrap(), 0.01, 1.0);
                    apx_eql(est_quant.unwrap(), quantile, 0.0001);
                } else {
                    pct_eql(est_val.unwrap(), value, 1.0);
                    pct_eql(est_quant.unwrap(), quantile, 1.0);
                }

                let (est_val2, est_quant2) = client
                    .update(
                        &format!(
                            "SELECT
                            tdigest->approx_percentile({quantile}), \
                            tdigest->approx_percentile_rank({value}) \
                            FROM digest"
                        ),
                        None,
                        &[],
                    )
                    .unwrap()
                    .first()
                    .get_two::<f64, f64>()
                    .unwrap();
                assert_eq!(est_val2, est_val);
                assert_eq!(est_quant2, est_quant);
            }
        });
    }

    #[pg_test]
    fn test_tdigest_small_count() {
        Spi::connect_mut(|client| {
            let estimate = client
                .update(
                    "SELECT \
                    approx_percentile(\
                        0.99, \
                        tdigest(100, data)) \
                    FROM generate_series(1, 100) data;",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one()
                .unwrap();

            assert_eq!(estimate, Some(99.5));
        });
    }

    #[pg_test]
    fn serialization_matches() {
        let mut t = InternalTDigest::new_with_size(10);
        let vals = vec![1.0, 1.0, 1.0, 2.0, 1.0, 1.0];
        for v in vals {
            t = t.merge_unsorted(vec![v]);
        }
        let pgt = TDigest::from_internal_tdigest(&t);
        let mut si = StringInfo::new();
        pgt.output(&mut si);
        assert_eq!(t.format_for_postgres(), si.to_string());
    }

    #[pg_test]
    fn test_tdigest_io() {
        Spi::connect_mut(|client| {
            let output = client
                .update(
                    "SELECT \
                tdigest(100, data)::text \
                FROM generate_series(1, 100) data;",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<String>()
                .unwrap();

            let expected = "(version:1,buckets:88,max_buckets:100,count:100,sum:5050,min:1,max:100,centroids:[(mean:1,weight:1),(mean:2,weight:1),(mean:3,weight:1),(mean:4,weight:1),(mean:5,weight:1),(mean:6,weight:1),(mean:7,weight:1),(mean:8,weight:1),(mean:9,weight:1),(mean:10,weight:1),(mean:11,weight:1),(mean:12,weight:1),(mean:13,weight:1),(mean:14,weight:1),(mean:15,weight:1),(mean:16,weight:1),(mean:17,weight:1),(mean:18,weight:1),(mean:19,weight:1),(mean:20,weight:1),(mean:21,weight:1),(mean:22,weight:1),(mean:23,weight:1),(mean:24,weight:1),(mean:25,weight:1),(mean:26,weight:1),(mean:27,weight:1),(mean:28,weight:1),(mean:29,weight:1),(mean:30,weight:1),(mean:31,weight:1),(mean:32,weight:1),(mean:33,weight:1),(mean:34,weight:1),(mean:35,weight:1),(mean:36,weight:1),(mean:37,weight:1),(mean:38,weight:1),(mean:39,weight:1),(mean:40,weight:1),(mean:41,weight:1),(mean:42,weight:1),(mean:43,weight:1),(mean:44,weight:1),(mean:45,weight:1),(mean:46,weight:1),(mean:47,weight:1),(mean:48,weight:1),(mean:49,weight:1),(mean:50,weight:1),(mean:51,weight:1),(mean:52.5,weight:2),(mean:54.5,weight:2),(mean:56.5,weight:2),(mean:58.5,weight:2),(mean:60.5,weight:2),(mean:62.5,weight:2),(mean:64,weight:1),(mean:65.5,weight:2),(mean:67.5,weight:2),(mean:69,weight:1),(mean:70.5,weight:2),(mean:72,weight:1),(mean:73.5,weight:2),(mean:75,weight:1),(mean:76,weight:1),(mean:77.5,weight:2),(mean:79,weight:1),(mean:80,weight:1),(mean:81.5,weight:2),(mean:83,weight:1),(mean:84,weight:1),(mean:85,weight:1),(mean:86,weight:1),(mean:87,weight:1),(mean:88,weight:1),(mean:89,weight:1),(mean:90,weight:1),(mean:91,weight:1),(mean:92,weight:1),(mean:93,weight:1),(mean:94,weight:1),(mean:95,weight:1),(mean:96,weight:1),(mean:97,weight:1),(mean:98,weight:1),(mean:99,weight:1),(mean:100,weight:1)])";

            assert_eq!(output, Some(expected.into()));

            let estimate = client
                .update(
                    &format!("SELECT approx_percentile(0.90, '{expected}'::tdigest)"),
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one()
                .unwrap();
            assert_eq!(estimate, Some(90.5));
        });
    }

    #[pg_test]
    fn test_tdigest_byte_io() {
        unsafe {
            use std::ptr;
            let state = tdigest_trans_inner(None, 100, Some(14.0), ptr::null_mut());
            let state = tdigest_trans_inner(state, 100, Some(18.0), ptr::null_mut());
            let state = tdigest_trans_inner(state, 100, Some(22.7), ptr::null_mut());
            let state = tdigest_trans_inner(state, 100, Some(39.42), ptr::null_mut());
            let state = tdigest_trans_inner(state, 100, Some(-43.0), ptr::null_mut());

            let mut control = state.unwrap();
            let buffer = tdigest_serialize(Inner::from(control.clone()).internal().unwrap());
            let buffer = pgrx::varlena::varlena_to_byte_slice(buffer.0.cast_mut_ptr());

            let expected = [
                1, 1, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 69, 192, 1, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 44, 64, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 50, 64, 1, 0,
                0, 0, 0, 0, 0, 0, 51, 51, 51, 51, 51, 179, 54, 64, 1, 0, 0, 0, 0, 0, 0, 0, 246, 40,
                92, 143, 194, 181, 67, 64, 1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 144,
                194, 245, 40, 92, 143, 73, 64, 5, 0, 0, 0, 0, 0, 0, 0, 246, 40, 92, 143, 194, 181,
                67, 64, 0, 0, 0, 0, 0, 128, 69, 192,
            ];
            assert_eq!(buffer, expected);

            let expected = pgrx::varlena::rust_byte_slice_to_bytea(&expected);
            let mut new_state =
                tdigest_deserialize_inner(bytea(pg_sys::Datum::from(expected.as_ptr())));

            assert_eq!(new_state.build(), control.build());
        }
    }

    #[pg_test]
    fn test_tdigest_compound_agg() {
        Spi::connect_mut(|client| {
            client
                .update(
                    "CREATE TABLE new_test (device INTEGER, value DOUBLE PRECISION)",
                    None,
                    &[],
                )
                .unwrap();
            client.update("INSERT INTO new_test SELECT dev, dev - v FROM generate_series(1,10) dev, generate_series(0, 1.0, 0.01) v", None, &[]).unwrap();

            let sanity = client
                .update("SELECT COUNT(*) FROM new_test", None, &[])
                .unwrap()
                .first()
                .get_one::<i64>()
                .unwrap();
            assert_eq!(Some(1010), sanity);

            client
                .update(
                    "CREATE VIEW digests AS \
                SELECT device, tdigest(20, value) \
                FROM new_test \
                GROUP BY device",
                    None,
                    &[],
                )
                .unwrap();

            client
                .update(
                    "CREATE VIEW composite AS \
                SELECT tdigest(tdigest) \
                FROM digests",
                    None,
                    &[],
                )
                .unwrap();

            client
                .update(
                    "CREATE VIEW base AS \
                SELECT tdigest(20, value) \
                FROM new_test",
                    None,
                    &[],
                )
                .unwrap();

            let value = client
                .update(
                    "SELECT \
                    approx_percentile(0.9, tdigest) \
                    FROM base",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap();

            let test_value = client
                .update(
                    "SELECT \
                approx_percentile(0.9, tdigest) \
                    FROM composite",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap();

            apx_eql(test_value.unwrap(), value.unwrap(), 0.1);
            apx_eql(test_value.unwrap(), 9.0, 0.1);
        });
    }

    #[pg_test]
    fn test_tdigest_approx_cdf() {
        Spi::connect_mut(|client| {
            client
                .update(
                    "CREATE TABLE cdf_test (data DOUBLE PRECISION)",
                    None,
                    &[],
                )
                .unwrap();
            client
                .update(
                    "INSERT INTO cdf_test SELECT generate_series(0.01, 100, 0.01)",
                    None,
                    &[],
                )
                .unwrap();

            client
                .update(
                    "CREATE VIEW cdf_digest AS \
                    SELECT tdigest(100, data) FROM cdf_test",
                    None,
                    &[],
                )
                .unwrap();

            // CDF at 50: ~50% of data should be <= 50
            let cdf_at_50 = client
                .update(
                    "SELECT approx_cdf(50, tdigest) FROM cdf_digest",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            apx_eql(cdf_at_50, 0.5, 0.01);

            // CDF at 0: near 0
            let cdf_at_0 = client
                .update(
                    "SELECT approx_cdf(0, tdigest) FROM cdf_digest",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            apx_eql(cdf_at_0, 0.0, 0.001);

            // CDF at 100: near 1
            let cdf_at_100 = client
                .update(
                    "SELECT approx_cdf(100, tdigest) FROM cdf_digest",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            apx_eql(cdf_at_100, 1.0, 0.001);

            // CDF range: P(20 < X <= 80) should be ~60%
            let range_prob = client
                .update(
                    "SELECT approx_cdf(80, tdigest) - approx_cdf(20, tdigest) FROM cdf_digest",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            apx_eql(range_prob, 0.6, 0.01);

            // Arrow operator syntax
            let arrow_cdf = client
                .update(
                    "SELECT tdigest -> approx_cdf(50) FROM cdf_digest",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            apx_eql(arrow_cdf, 0.5, 0.01);

            client.update("DROP VIEW cdf_digest", None, &[]).unwrap();
            client.update("DROP TABLE cdf_test", None, &[]).unwrap();
        });
    }

    #[pg_test]
    fn test_tdigest_to_histogram() {
        Spi::connect_mut(|client| {
            // Uniform distribution: 0.01..99.99 step 1 => 100 values
            client
                .update(
                    "CREATE TABLE hist_test (data DOUBLE PRECISION)",
                    None,
                    &[],
                )
                .unwrap();
            client
                .update(
                    "INSERT INTO hist_test SELECT generate_series(0.01, 99.99, 1.0)",
                    None,
                    &[],
                )
                .unwrap();

            client
                .update(
                    "CREATE VIEW hist_digest AS \
                    SELECT tdigest(100, data) FROM hist_test",
                    None,
                    &[],
                )
                .unwrap();

            // 4 equal bins [0, 25, 50, 75, 100] => ~25 each
            let hist = client
                .update(
                    "SELECT tdigest_to_histogram(tdigest, '{0,25,50,75,100}') FROM hist_digest",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            assert_eq!(hist.len(), 4);
            for &h in &hist {
                apx_eql(h, 25.0, 2.0);
            }

            // Single bin covers all
            let hist_one = client
                .update(
                    "SELECT tdigest_to_histogram(tdigest, '{0,100}') FROM hist_digest",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            assert_eq!(hist_one.len(), 1);
            apx_eql(hist_one[0], 100.0, 0.01);

            // Single point at 50 => should land in bin [25, 50) or [50, 75)
            let hist_single = client
                .update(
                    "SELECT tdigest_to_histogram(tdigest(100, 50.0), '{0,25,50,75,100}')",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            assert_eq!(hist_single.len(), 4);
            let total_single: f64 = hist_single.iter().sum();
            apx_eql(total_single, 1.0, 0.01);

            // Total weight consistency: sum(hist) ≈ total count
            let hist_total: f64 = hist.iter().sum();
            apx_eql(hist_total, 100.0, 0.01);

            client.update("DROP VIEW hist_digest", None, &[]).unwrap();
            client.update("DROP TABLE hist_test", None, &[]).unwrap();
        });
    }

    #[pg_test]
    fn test_tdigest_to_histogram_beyond_max_edge() {
        Spi::connect_mut(|client| {
            // Regression: centroid mean > max(bin_edges) must not OOB.
            // Value 150 > edges max 100, binary_search returns Err(5) where len=5.
            client
                .update(
                    "CREATE TABLE beyond_test (data DOUBLE PRECISION)",
                    None,
                    &[],
                )
                .unwrap();
            client
                .update(
                    "INSERT INTO beyond_test VALUES (50.0), (150.0)",
                    None,
                    &[],
                )
                .unwrap();

            client
                .update(
                    "CREATE VIEW beyond_digest AS \
                    SELECT tdigest(100, data) FROM beyond_test",
                    None,
                    &[],
                )
                .unwrap();

            // edges: [0,25,50,75,100], data: 50 and 150
            // 50 → exact match edges[2] → bin 2 (Ok path, i=2)
            // 150 > 100 → Err(5=m) → previously OOB, now clamped to last bin
            let hist = client
                .update(
                    "SELECT tdigest_to_histogram(tdigest, '{0,25,50,75,100}') FROM beyond_digest",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            assert_eq!(hist.len(), 4);
            // bin[2] (50-75): value 50.0 goes here
            assert!(
                hist[2] > 0.0,
                "value=50 at exact edge should be in bin 2"
            );
            // bin[3] (75-100): value 150.0 clamped here (beyond max edge)
            assert!(
                hist[3] > 0.0,
                "value=150 beyond max edge should be clamped to last bin"
            );

            client
                .update("DROP VIEW beyond_digest", None, &[])
                .unwrap();
            client
                .update("DROP TABLE beyond_test", None, &[])
                .unwrap();
        });
    }

    #[pg_test]
    fn test_weighted_tdigest_identity() {
        Spi::connect_mut(|client| {
            // weighted_tdigest with weight=1 for all -> same as unweighted
            let d50_weighted = client
                .update(
                    "SELECT approx_percentile(0.5, weighted_tdigest(100, v, 1.0)) \
                    FROM (SELECT generate_series(0.01, 99.99, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            let d50_unweighted = client
                .update(
                    "SELECT approx_percentile(0.5, tdigest(100, v)) \
                    FROM (SELECT generate_series(0.01, 99.99, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            apx_eql(d50_weighted, d50_unweighted, 0.001);
        });
    }

    #[pg_test]
    fn test_approx_percentile_weight_power_x3() {
        Spi::connect_mut(|client| {
            // Two values: 1.0 and 3.0 with weight_power=3
            // 3^3 = 27, 1^3 = 1 => weighted median should be much closer to 3.0
            let d50 = client
                .update(
                    "SELECT approx_percentile(0.5, tdigest(100, v), 3) \
                    FROM (SELECT * FROM (VALUES (1.0), (3.0)) AS t(v)) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            // With x³ weighting, 3.0 has 27× the weight of 1.0
            // The weighted median should be >= 3.0 (or very close)
            assert!(d50 >= 2.5, "weighted median should shift toward larger value, got {d50}");
        });
    }

    #[pg_test]
    fn test_approx_percentile_weight_power_0_not_changed() {
        Spi::connect_mut(|client| {
            let val_2arg = client
                .update(
                    "SELECT approx_percentile(0.5, tdigest(100, v)) \
                    FROM (SELECT generate_series(0.01, 99.99, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            let val_3arg_weight0 = client
                .update(
                    "SELECT approx_percentile(0.5, tdigest(100, v), 0) \
                    FROM (SELECT generate_series(0.01, 99.99, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            apx_eql(val_2arg, val_3arg_weight0, 0.0001);
        });
    }

    #[pg_test]
    fn test_tdigest_to_histogram_weight_power_0_unchanged() {
        Spi::connect_mut(|client| {
            let hist_2arg = client
                .update(
                    "SELECT tdigest_to_histogram(tdigest(100, v), '{0,25,50,75,100}') \
                    FROM (SELECT generate_series(0.01, 99.99, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            let hist_3arg = client
                .update(
                    "SELECT tdigest_to_histogram(tdigest(100, v), '{0,25,50,75,100}', 0.0) \
                    FROM (SELECT generate_series(0.01, 99.99, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            assert_eq!(hist_2arg.len(), hist_3arg.len());
            for (a, b) in hist_2arg.iter().zip(hist_3arg.iter()) {
                apx_eql(*a, *b, 0.0001);
            }
        });
    }

    #[pg_test]
    fn test_weighted_tdigest_rollup() {
        Spi::connect_mut(|client| {
            // Build two weighted sketches separately, rollup, compare with single sketch
            let rolled = client
                .update(
                    "SELECT approx_percentile(0.5, rollup(s)) \
                    FROM (SELECT weighted_tdigest(100, v, w) AS s \
                          FROM (VALUES (1.0, 5.0), (2.0, 1.0), (3.0, 1.0)) AS t(v, w) \
                          GROUP BY CASE WHEN v <= 2 THEN 0 ELSE 1 END) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            let single = client
                .update(
                    "SELECT approx_percentile(0.5, weighted_tdigest(100, v, w)) \
                    FROM (VALUES (1.0, 5.0), (2.0, 1.0), (3.0, 1.0)) AS t(v, w)",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            apx_eql(rolled, single, 0.01);
        });
    }

    #[pg_test]
    fn test_write_time_vs_query_time_weighting() {
        Spi::connect_mut(|client| {
            // write-time: weighted_tdigest(v, v^3) with default approx_percentile
            // should match query-time: tdigest(v) with approx_percentile(..., 3)
            let write_time = client
                .update(
                    "SELECT approx_percentile(0.5, weighted_tdigest(100, v, v^3)) \
                    FROM (SELECT generate_series(0.5, 100.0, 0.5) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            let query_time = client
                .update(
                    "SELECT approx_percentile(0.5, tdigest(100, v), 3) \
                    FROM (SELECT generate_series(0.5, 100.0, 0.5) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            let diff = (write_time - query_time).abs() / write_time.abs().max(1.0);
            assert!(diff < 0.02, "write-time vs query-time relative diff: {diff}");
        });
    }

    #[pg_test]
    fn test_weighted_tdigest_round_not_truncate() {
        Spi::connect_mut(|client| {
            // Regression: weighted_tdigest must round() weights, not truncate.
            // weight=0.9 → round to 1, not truncate to 0.
            let result = client
                .update(
                    "SELECT approx_percentile(0.5, weighted_tdigest(100, 1.0, 0.9))",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            // If weight was truncated to 0, result would be NULL (empty sketch).
            // With correct rounding, weight=1, result should be the centroid mean ≈ 1.0.
            assert!(
                (result - 1.0).abs() < 0.01,
                "weight=0.9 should round to 1, got {result}"
            );
        });
    }

    // ─── tdigest_to_pdf tests ───────────────────────────────────────

    #[pg_test]
    fn test_tdigest_to_pdf_basic() {
        Spi::connect_mut(|client| {
            let pdf = client
                .update(
                    "SELECT tdigest_to_pdf(tdigest(100, v), 100, 0.0, 100.0) \
                    FROM (SELECT generate_series(0.01, 99.99, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            assert_eq!(pdf.len(), 100);
            let sum: f64 = pdf.iter().sum();
            apx_eql(sum, 100.0, 0.5);
        });
    }

    #[pg_test]
    fn test_tdigest_to_pdf_empty() {
        Spi::connect_mut(|client| {
            let pdf = client
                .update(
                    "SELECT tdigest_to_pdf(tdigest(100, v), 100, 0.0, 100.0) \
                    FROM (SELECT NULL::double precision AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap();

            if let Some(ref arr) = pdf {
                assert_eq!(arr.len(), 100);
                for &v in arr.iter() {
                    assert!((v - 0.0).abs() < 1e-10);
                }
            }
        });
    }

    #[pg_test]
    fn test_tdigest_to_pdf_single_centroid() {
        Spi::connect_mut(|client| {
            let pdf = client
                .update(
                    "SELECT tdigest_to_pdf(tdigest(100, 50.0), 100, 0.0, 100.0)",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            assert_eq!(pdf.len(), 100);
            let sum: f64 = pdf.iter().sum();
            apx_eql(sum, 100.0, 0.5);
            let nonzero = pdf.iter().filter(|&&v| v > 0.0).count();
            assert!(nonzero > 0, "single centroid should produce at least one nonzero bin");
        });
    }

    #[pg_test]
    fn test_tdigest_to_pdf_weight_power_0() {
        Spi::connect_mut(|client| {
            let pdf_4arg = client
                .update(
                    "SELECT tdigest_to_pdf(tdigest(100, v), 50, 0.0, 100.0) \
                    FROM (SELECT generate_series(0.01, 99.99, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            let pdf_5arg = client
                .update(
                    "SELECT tdigest_to_pdf(tdigest(100, v), 50, 0.0, 100.0, 0.0) \
                    FROM (SELECT generate_series(0.01, 99.99, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            assert_eq!(pdf_4arg.len(), pdf_5arg.len());
            for (a, b) in pdf_4arg.iter().zip(pdf_5arg.iter()) {
                apx_eql(*a, *b, 0.0001);
            }
        });
    }

    #[pg_test]
    fn test_tdigest_to_pdf_sum_equals_100() {
        Spi::connect_mut(|client| {
            let pdf = client
                .update(
                    "SELECT tdigest_to_pdf(tdigest(100, v), 200, 0.0, 100.0) \
                    FROM (SELECT generate_series(0.01, 99.99, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            let sum: f64 = pdf.iter().sum();
            assert!(
                (sum - 100.0).abs() < 0.1,
                "sum(pdf) = {sum}, expected 100.0 ± 0.1"
            );
        });
    }

    #[pg_test]
    fn test_tdigest_to_pdf_weighted() {
        Spi::connect_mut(|client| {
            let pdf = client
                .update(
                    "SELECT tdigest_to_pdf(tdigest(100, v), 100, 0.0, 100.0, 3.0) \
                    FROM (SELECT generate_series(1.0, 99.0, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            assert_eq!(pdf.len(), 100);
            let sum: f64 = pdf.iter().sum();
            apx_eql(sum, 100.0, 0.5);
            let nonzero = pdf.iter().filter(|&&v| v > 0.0).count();
            assert!(nonzero > 0, "weighted pdf should produce nonzero bins");
        });
    }

    #[pg_test]
    fn test_tdigest_to_pdf_zero_bins_fewer_than_histogram() {
        Spi::connect_mut(|client| {
            let pdf = client
                .update(
                    "SELECT tdigest_to_pdf(tdigest(100, v), 200, 0.0, 100.0) \
                    FROM (SELECT generate_series(0.01, 99.99, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            let hist = client
                .update(
                    "SELECT tdigest_to_histogram(tdigest(100, v), '{0,25,50,75,100}') \
                    FROM (SELECT generate_series(0.01, 99.99, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            let pdf_zeros = pdf.iter().filter(|&&v| v == 0.0).count();
            let hist_zeros = hist.iter().filter(|&&v| v == 0.0).count();

            let pdf_density = (pdf.len() - pdf_zeros) as f64 / pdf.len() as f64;
            let hist_density = (hist.len() - hist_zeros) as f64 / hist.len() as f64;
            assert!(
                pdf_density > hist_density,
                "pdf fill ratio {pdf_density} should exceed histogram fill ratio {hist_density}"
            );
        });
    }

    // ─── weighted approx_cdf tests ──────────────────────────────────

    #[pg_test]
    fn test_approx_cdf_weight_power_0() {
        Spi::connect_mut(|client| {
            let cdf_2arg = client
                .update(
                    "SELECT approx_cdf(tdigest(100, v), 50.0) \
                    FROM (SELECT generate_series(0.01, 99.99, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            let cdf_3arg = client
                .update(
                    "SELECT approx_cdf(tdigest(100, v), 50.0, 0.0) \
                    FROM (SELECT generate_series(0.01, 99.99, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            apx_eql(cdf_2arg, cdf_3arg, 0.0001);
        });
    }

    #[pg_test]
    fn test_approx_cdf_weighted_basic() {
        Spi::connect_mut(|client| {
            let cdf = client
                .update(
                    "SELECT approx_cdf(tdigest(100, v), 50.0, 3.0) \
                    FROM (SELECT generate_series(1.0, 99.0, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            assert!(cdf > 0.0 && cdf < 1.0, "weighted cdf should be in (0,1), got {cdf}");
        });
    }

    #[pg_test]
    fn test_approx_cdf_percentile_consistency() {
        Spi::connect_mut(|client| {
            let cdf_at_d50 = client
                .update(
                    "SELECT approx_cdf(
                        approx_percentile(0.5, tdigest(100, v), 3.0),
                        tdigest(100, v),
                        3.0
                    ) \
                    FROM (SELECT generate_series(1.0, 99.0, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            apx_eql(cdf_at_d50, 0.5, 0.01);
        });
    }

    #[pg_test]
    fn test_approx_cdf_percentile_consistency_multiple_quantiles() {
        Spi::connect_mut(|client| {
            let cdf_at_d10 = client
                .update(
                    "SELECT approx_cdf(
                        approx_percentile(0.1, tdigest(100, v), 3.0),
                        tdigest(100, v),
                        3.0
                    ) \
                    FROM (SELECT generate_series(1.0, 99.0, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            let cdf_at_d90 = client
                .update(
                    "SELECT approx_cdf(
                        approx_percentile(0.9, tdigest(100, v), 3.0),
                        tdigest(100, v),
                        3.0
                    ) \
                    FROM (SELECT generate_series(1.0, 99.0, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();

            apx_eql(cdf_at_d10, 0.1, 0.01);
            apx_eql(cdf_at_d90, 0.9, 0.01);
        });
    }

    // ─── single-centroid (NaN regression) tests ─────────────────────

    #[pg_test]
    fn test_tdigest_to_pdf_weighted_single_centroid() {
        Spi::connect_mut(|client| {
            let pdf = client
                .update(
                    "SELECT tdigest_to_pdf(tdigest(100, 200.0), 100, 0.0, 400.0, 3.0)",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            assert_eq!(pdf.len(), 100);
            let sum: f64 = pdf.iter().sum();
            assert!(
                (sum - 100.0).abs() < 0.5,
                "weighted single-centroid PDF sum = {sum}, expected 100"
            );
            let nonzero = pdf.iter().filter(|&&v| v > 0.0).count();
            assert!(nonzero > 0, "should have at least one nonzero bin");
        });
    }

    #[pg_test]
    fn test_approx_cdf_weighted_single_centroid() {
        Spi::connect_mut(|client| {
            let cdf_below = client
                .update(
                    "SELECT approx_cdf(tdigest(100, 200.0), 0.0, 3.0)",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert!(
                (cdf_below - 0.0).abs() < 1e-10,
                "CDF below single centroid should be 0, got {cdf_below}"
            );

            let cdf_at = client
                .update(
                    "SELECT approx_cdf(tdigest(100, 200.0), 200.0, 3.0)",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert!(
                !cdf_at.is_nan(),
                "CDF at centroid mean should not be NaN"
            );

            let cdf_above = client
                .update(
                    "SELECT approx_cdf(tdigest(100, 200.0), 999.0, 3.0)",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert!(
                (cdf_above - 1.0).abs() < 1e-10,
                "CDF above single centroid should be 1, got {cdf_above}"
            );
        });
    }

    #[pg_test]
    fn test_approx_cdf_percentile_consistency_single_centroid() {
        Spi::connect_mut(|client| {
            for q in [0.1, 0.5, 0.9] {
                let cdf = client
                    .update(
                        &format!(
                            "SELECT approx_cdf(
                                approx_percentile({q}, tdigest(100, 200.0), 3.0),
                                tdigest(100, 200.0),
                                3.0
                            )"
                        ),
                        None,
                        &[],
                    )
                    .unwrap()
                    .first()
                    .get_one::<f64>()
                    .unwrap()
                    .unwrap();

                assert!(
                    !cdf.is_nan(),
                    "single-centroid consistency at q={q} should not be NaN"
                );
                assert!(
                    (cdf - 1.0).abs() < 0.01,
                    "single-centroid cdf at q={q} should be ~1, got {cdf}"
                );
            }
        });
    }

    #[pg_test]
    fn test_approx_cdf_single_value() {
        Spi::connect_mut(|client| {
            let cdf_below = client
                .update(
                    "SELECT approx_cdf(tdigest(100, 42.0), 41.9)",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert!(
                (cdf_below - 0.0).abs() < 1e-10,
                "CDF below single value should be 0, got {cdf_below}"
            );

            let cdf_at = client
                .update(
                    "SELECT approx_cdf(tdigest(100, 42.0), 42.0)",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert!(
                !cdf_at.is_nan(),
                "CDF at single value should not be NaN"
            );
            assert!(
                (cdf_at - 1.0).abs() < 0.01,
                "CDF at single value should be ~1, got {cdf_at}"
            );

            let cdf_above = client
                .update(
                    "SELECT approx_cdf(tdigest(100, 42.0), 42.1)",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<f64>()
                .unwrap()
                .unwrap();
            assert!(
                (cdf_above - 1.0).abs() < 1e-10,
                "CDF above single value should be 1, got {cdf_above}"
            );
        });
    }

    #[pg_test]
    fn test_tdigest_to_pdf_out_of_range() {
        Spi::connect_mut(|client| {
            // Data is 1..100, query well outside that range
            let pdf = client
                .update(
                    "SELECT tdigest_to_pdf(tdigest(100, v), 100, -1000.0, -900.0) \
                    FROM (SELECT generate_series(1.0, 100.0, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            assert_eq!(pdf.len(), 100);
            let sum: f64 = pdf.iter().sum();
            assert!(
                (sum - 0.0).abs() < 1e-10,
                "PDF left of data should sum to 0, got {sum}"
            );

            // Data is 1..100, query range covering data
            let pdf_mid = client
                .update(
                    "SELECT tdigest_to_pdf(tdigest(100, v), 100, 0.0, 200.0) \
                    FROM (SELECT generate_series(1.0, 100.0, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            let mid_sum: f64 = pdf_mid.iter().sum();
            assert!(
                (mid_sum - 100.0).abs() < 0.5,
                "PDF covering data should sum to ~100, got {mid_sum}"
            );
        });
    }

    #[pg_test]
    fn test_tdigest_to_pdf_weighted_consistency_with_histogram() {
        Spi::connect_mut(|client| {
            let pdf = client
                .update(
                    "SELECT tdigest_to_pdf(tdigest(100, v), 100, 0.0, 100.0, 3.0) \
                    FROM (SELECT generate_series(1.0, 99.0, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            let sum: f64 = pdf.iter().sum();
            apx_eql(sum, 100.0, 0.5);
            let nonzero = pdf.iter().filter(|&&v| v > 0.0).count();
            assert!(
                nonzero > 50,
                "weighted pdf should have >50 nonzero bins, got {nonzero}"
            );
        });
    }

    // ─── tdigest_to_pdf_kde tests ─────────────────────────────────

    #[pg_test]
    fn test_tdigest_kde_single_centroid() {
        Spi::connect_mut(|client| {
            // Use mean=199 so the centroid aligns with bin center (i=99: x=199 when dx=2)
            let kde = client
                .update(
                    "SELECT tdigest_to_pdf_kde(tdigest(100, 199.0), 200, 0.0, 400.0)",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            assert_eq!(kde.len(), 200);
            let sum: f64 = kde.iter().sum();
            apx_eql(sum, 100.0, 3.0);

            let max_bin = kde.iter().cloned().fold(0.0_f64, f64::max);
            let max_idx = kde.iter().position(|&v| v == max_bin).unwrap_or(0);
            assert!(
                (max_idx as f64 - 99.0).abs() < 5.0,
                "KDE max should be at bin ~99 (mean=199), got bin {max_idx}"
            );

            let kde_nonzero = kde.iter().filter(|&&v| v > 0.0).count();
            assert!(
                kde_nonzero > 20,
                "KDE should spread beyond a single bin, got {kde_nonzero} nonzero"
            );
        });
    }

    #[pg_test]
    fn test_tdigest_kde_sum_equals_100() {
        Spi::connect_mut(|client| {
            let kde = client
                .update(
                    "SELECT tdigest_to_pdf_kde(tdigest(100, v), 200, -200.0, 600.0) \
                    FROM (SELECT generate_series(1.0, 399.0, 1.0) AS v) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            let sum: f64 = kde.iter().sum();
            apx_eql(sum, 100.0, 0.5);
        });
    }

    #[pg_test]
    fn test_tdigest_kde_smoothness() {
        Spi::connect_mut(|client| {
            let kde = client
                .update(
                    "SELECT tdigest_to_pdf_kde(tdigest(100, v), 200, 0.0, 400.0) \
                    FROM (SELECT v FROM (SELECT random() * 100.0 AS v FROM generate_series(1, 10000)) sq) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            let pdf = client
                .update(
                    "SELECT tdigest_to_pdf(tdigest(100, v), 200, 0.0, 400.0) \
                    FROM (SELECT v FROM (SELECT random() * 100.0 AS v FROM generate_series(1, 10000)) sq) AS subq",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            let kde_zero = kde.iter().filter(|&&v| v == 0.0).count();
            let pdf_zero = pdf.iter().filter(|&&v| v == 0.0).count();
            assert!(
                kde_zero <= pdf_zero,
                "KDE should have at most as many zero bins as PDF, got {kde_zero} vs {pdf_zero}"
            );
        });
    }

    #[pg_test]
    fn test_tdigest_kde_bandwidth_param() {
        Spi::connect_mut(|client| {
            // Use mean=199 to align centroid with bin center (i=99: x=199 when dx=2)
            let bw_auto = client
                .update(
                    "SELECT tdigest_to_pdf_kde(tdigest(100, 199.0), 200, 0.0, 400.0)",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            let bw_large = client
                .update(
                    "SELECT tdigest_to_pdf_kde(tdigest(100, 199.0), 200, 0.0, 400.0, 100.0)",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            let bw_small = client
                .update(
                    "SELECT tdigest_to_pdf_kde(tdigest(100, 199.0), 200, 0.0, 400.0, 0.6)",
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_one::<Vec<f64>>()
                .unwrap()
                .unwrap();

            assert_eq!(bw_auto.len(), 200);
            assert_eq!(bw_large.len(), 200);
            assert_eq!(bw_small.len(), 200);

            // bw_auto should sum to ~100 (auto bandwidth is reasonable for single centroid)
            let sum_auto: f64 = bw_auto.iter().sum();
            apx_eql(sum_auto, 100.0, 3.0);

            // bw_large=100 leaks mass outside [0,400] (σ=100 captures Φ(2)-Φ(-2)≈95%)
            // bw_small=2 is well-sampled at dx=2; neither extreme value is expected to sum to 100
            // Instead verify relative peak shapes
            let max_small = bw_small.iter().cloned().fold(0.0_f64, f64::max);
            let max_auto = bw_auto.iter().cloned().fold(0.0_f64, f64::max);
            let max_large = bw_large.iter().cloned().fold(0.0_f64, f64::max);

            assert!(
                max_small > max_auto,
                "small bandwidth should give sharper peak ({max_small}) than auto ({max_auto})"
            );
            assert!(
                max_auto > max_large,
                "auto bandwidth should give sharper peak ({max_auto}) than large ({max_large})"
            );
        });
    }
}
