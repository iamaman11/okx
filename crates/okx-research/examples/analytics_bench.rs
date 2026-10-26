//! Run: cargo run -p okx-research --release --example analytics_bench
use okx_analysis::operators::EmaParameters;
use okx_research::strategy_registry::*;
fn dataset(n: usize, ms: u64) -> Vec<Bar> {
    (0..n)
        .map(|i| {
            let p = 100.0 + (i as f64 / 80.0).sin() * 5.0 + i as f64 * 0.0001;
            Bar {
                time: i as u64 * ms,
                open: p,
                high: p + 1.0,
                low: p - 1.0,
                close: p + 0.1,
                volume: 1.0,
                funding_rate: Some(0.0),
            }
        })
        .collect()
}
fn spec() -> ReplaySpec {
    ReplaySpec {
        strategy_id: "ema_cross_v1".into(),
        parameters: EmaParameters { fast: 20, slow: 50 },
        mechanics: Mechanics::Linear {
            contract_multiplier: 0.01,
        },
        costs: Costs {
            maker_fee_bps: 1.0,
            taker_fee_bps: 5.0,
            slippage_bps: 2.0,
            observed_funding: true,
        },
        timeframe_ms: 900000,
        historical_observed: false,
        historical_rules: false,
        historical_fees: false,
        seed: 398,
        stop_fraction: None,
        take_profit_fraction: None,
    }
}
fn main() {
    if std::env::args().any(|arg| arg == "--k9-fixed") {
        k9_fixed_profile();
        return;
    }
    let bars = dataset(8640, 900000);
    let spec = spec();
    for _ in 0..5 {
        replay(&bars, &spec, 50, bars.len(), &|| false).unwrap();
    }
    let mut elapsed = Vec::new();
    let mut result = None;
    for _ in 0..30 {
        let t = std::time::Instant::now();
        result = Some(replay(&bars, &spec, 50, bars.len(), &|| false).unwrap());
        elapsed.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    elapsed.sort_by(f64::total_cmp);
    let baseline = peak_memory();
    let big = dataset(100000, 900000);
    let variants = (0..100)
        .map(|i| EmaParameters {
            fast: 1 + i,
            slow: 101 + i,
        })
        .collect::<Vec<_>>();
    let t = std::time::Instant::now();
    let g = grid(&big, &spec, &variants, &|| false).unwrap();
    let runtime = t.elapsed().as_secs_f64();
    let peak = peak_memory();
    let result = result.unwrap();
    println!(
        "{}",
        serde_json::json!({"platform":std::env::consts::OS,"arch":std::env::consts::ARCH,"cpu":std::env::var("PROCESSOR_IDENTIFIER").unwrap_or_default(),"logical_cpus":std::thread::available_parallelism().unwrap().get(),"build":"release","fixture_hash":okx_research::canonical_sha256(&bars).unwrap(),"ema_90d_15m":{"bars":8640,"warmups":5,"measurements":30,"p50_ms":elapsed[14],"p95_ms":elapsed[28],"max_ms":elapsed[29],"summary_bytes":serde_json::to_vec(&result.summary).unwrap().len(),"result_hash":result.hash},"grid_100variants_100k":{"variants":100,"bars":100000,"runtime_seconds":runtime,"bars_per_second":10000000.0/runtime,"peak_working_set_bytes":peak,"incremental_peak_bytes":peak.saturating_sub(baseline),"result_hash":g.test.hash}})
    );
}
#[cfg(windows)]
fn peak_memory() -> usize {
    #[repr(C)]
    struct Counters {
        cb: u32,
        faults: u32,
        peak: usize,
        working: usize,
        paged_peak: usize,
        paged: usize,
        nonpaged_peak: usize,
        nonpaged: usize,
        pagefile: usize,
        pagefile_peak: usize,
    }
    #[link(name = "psapi")]
    unsafe extern "system" {
        fn GetProcessMemoryInfo(
            process: *mut core::ffi::c_void,
            counters: *mut Counters,
            size: u32,
        ) -> i32;
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut core::ffi::c_void;
    }
    let mut c = Counters {
        cb: std::mem::size_of::<Counters>() as u32,
        faults: 0,
        peak: 0,
        working: 0,
        paged_peak: 0,
        paged: 0,
        nonpaged_peak: 0,
        nonpaged: 0,
        pagefile: 0,
        pagefile_peak: 0,
    };
    unsafe {
        assert_ne!(
            GetProcessMemoryInfo(
                GetCurrentProcess(),
                &mut c,
                std::mem::size_of::<Counters>() as u32
            ),
            0
        )
    };
    c.peak
}
#[cfg(not(windows))]
fn peak_memory() -> usize {
    0
}

/// K9 reference profile, never reaches the exchange. Deliberately opt-in:
/// cargo run --release -p okx-research --example analytics_bench -- --k9-fixed
/// 5 warmups and 30 recorded runs for each requested 1/4/8 CPU profile.
/// Emit raw samples: gate reviewers must NOT extrapolate from a quick smoke.
fn k9_fixed_profile() {
    use sha2::{Digest, Sha256};
    const INSTRUMENTS: usize = 8;
    const BARS_EACH: usize = 125_000;
    const VARIANT_COUNT: usize = 64;
    const WARMUPS: usize = 5;
    const MEASURED: usize = 30;
    let source = (0..INSTRUMENTS)
        .map(|instrument| {
            let mut bars = dataset(BARS_EACH, 60_000);
            for bar in &mut bars {
                let delta = instrument as f64 / 10.0;
                bar.open += delta;
                bar.high += delta;
                bar.low += delta;
                bar.close += delta;
            }
            bars
        })
        .collect::<Vec<_>>();
    let variants = (0..VARIANT_COUNT)
        .map(|i| EmaParameters {
            fast: 2 + i % 16,
            slow: 40 + i,
        })
        .collect::<Vec<_>>();
    let mut mechanics = spec();
    mechanics.timeframe_ms = 60_000;
    let bars_hash = okx_research::canonical_sha256(&source).unwrap();
    let lockfile_sha256 = format!(
        "{:x}",
        Sha256::digest(std::fs::read("Cargo.lock").expect("run from repository root"))
    );
    let mut reference_hash = None;
    let mut readings = Vec::new();
    let peak_before = peak_memory();
    for workers in [1usize, 4, 8] {
        for _ in 0..WARMUPS {
            let observed = run_fixed_batch(&source, &mechanics, &variants, workers);
            if let Some(expected) = &reference_hash {
                assert_eq!(&observed, expected, "profile results must be identical");
            } else {
                reference_hash = Some(observed);
            }
        }
        let mut raw_ms = Vec::with_capacity(MEASURED);
        for _ in 0..MEASURED {
            let began = std::time::Instant::now();
            let observed = run_fixed_batch(&source, &mechanics, &variants, workers);
            let ms = began.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(Some(&observed), reference_hash.as_ref());
            raw_ms.push(ms);
        }
        let mut sorted = raw_ms.clone();
        sorted.sort_by(f64::total_cmp);
        readings.push(serde_json::json!({
            "workers":workers,
            "warmups":WARMUPS,
            "measurements":MEASURED,
            "p50_ms":sorted[14],
            "p95_ms":sorted[28],
            "max_ms":sorted[29],
            "raw_ms":raw_ms,
            "peak_working_set_bytes":peak_memory(),
        }));
    }
    let logical_cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
    let hash = okx_research::canonical_sha256(&reference_hash).unwrap();
    let one_p50 = readings[0]["p50_ms"].as_f64().unwrap();
    let four_p50 = readings[1]["p50_ms"].as_f64().unwrap();
    println!(
        "K9_REFERENCE_PROFILE_JSON:{}",
        serde_json::json!({
            "schema":"okx.k9.reference-benchmark/v1",
            "os":std::env::consts::OS,
            "arch":std::env::consts::ARCH,
            "logical_cpus":logical_cpus,
            "processor":std::env::var("PROCESSOR_IDENTIFIER").unwrap_or_default(),
            "source":"fixed_local_in_memory_no_http",
            "build":"release",
            "lockfile_sha256":lockfile_sha256,
            "fixture_hash":bars_hash,
            "result_hash":hash,
            "bars_total":INSTRUMENTS * BARS_EACH,
            "instruments":INSTRUMENTS,
            "variants":VARIANT_COUNT,
            "worker_profiles":readings,
            "median_speedup_4_vs_1":one_p50 / four_p50,
            "memory_peak_bytes":peak_memory(),
            "memory_incremental_peak_bytes":peak_memory().saturating_sub(peak_before),
            "reference_cpu_eligible":logical_cpus >= 8,
            "acceptance_scope":"pure_replay_kernel_only; real runtime queued/source/memory/RSS/transport not measured",
            "slo_status":"NOT_FULL_K9_ACCEPTANCE",
        })
    );
}

fn run_fixed_batch(
    sources: &[Vec<Bar>],
    replay_spec: &ReplaySpec,
    variants: &[EmaParameters],
    workers: usize,
) -> Vec<String> {
    let mut result_hashes = Vec::new();
    for group in sources.chunks(workers) {
        let complete = std::thread::scope(|scope| {
            let spawned = group
                .iter()
                .map(|bars| {
                    scope.spawn(move || {
                        grid(bars, replay_spec, variants, &|| false)
                            .unwrap()
                            .test
                            .hash
                    })
                })
                .collect::<Vec<_>>();
            spawned
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        result_hashes.extend(complete);
    }
    assert_eq!(result_hashes.len(), sources.len());
    result_hashes
}
