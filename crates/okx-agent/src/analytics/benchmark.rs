//! K9 measures the existing replay kernel *inside* the running AnalyticsService.
//! No second agent, Cargo process, controller research owner, exchange I/O, or SQL ledger copy.
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use futures_util::{StreamExt, stream};
use okx_analysis::operators::EmaParameters;
use okx_research::{
    ResearchArtifactStore,
    strategy_registry::{self as replay, Bar, Costs, Mechanics, ReplaySpec},
};
use serde_json::{Value, json};

use super::executor::{CpuExecutor, cpu_worker_budget};

const INSTRUMENTS: usize = 8;
const BARS_PER_INSTRUMENT: usize = 125_000;
const VARIANTS: usize = 64;
const WARMUPS: usize = 5;
const SAMPLES: usize = 30;

/// A single ordinary bounded research job, with durable result in the current
/// artifact store. Its 3 GiB estimate is held by the caller's shared 4 GiB pool.
pub(super) async fn run(
    cpu: CpuExecutor,
    artifacts: ResearchArtifactStore,
    cancel: Arc<AtomicBool>,
) -> Result<String, String> {
    // Run source construction on the existing bounded CPU executor, not on
    // the WSS/SQL/REST async control thread. Fixture never contacts an exchange.
    let source = cpu
        .run(cancel.clone(), move |token| {
            let mut source = Vec::with_capacity(INSTRUMENTS);
            for instrument in 0..INSTRUMENTS {
                if token.load(Ordering::Acquire) {
                    return Err("CANCELLED".into());
                }
                let bars = (0..BARS_PER_INSTRUMENT)
                    .map(|i| {
                        let price = 100.0
                            + (i as f64 / 80.0).sin() * 5.0
                            + i as f64 * 0.0001
                            + instrument as f64 / 10.0;
                        Bar {
                            time: i as u64 * 60_000,
                            open: price,
                            high: price + 1.0,
                            low: price - 1.0,
                            close: price + 0.1,
                            volume: 1.0,
                            funding_rate: Some(0.0),
                        }
                    })
                    .collect::<Vec<_>>();
                source.push(bars);
            }
            let fixture_hash =
                okx_research::canonical_sha256(&source).map_err(|e| e.to_string())?;
            Ok((
                source.into_iter().map(Arc::new).collect::<Vec<_>>(),
                fixture_hash,
            ))
        })
        .await?;
    let (sources, fixture_hash) = source;
    let sources = Arc::new(sources);
    let variants = Arc::new(
        (0..VARIANTS)
            .map(|i| EmaParameters {
                fast: 2 + i % 16,
                slow: 40 + i,
            })
            .collect::<Vec<_>>(),
    );
    let spec = Arc::new(ReplaySpec {
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
        timeframe_ms: 60_000,
        historical_observed: false,
        historical_rules: false,
        historical_fees: false,
        seed: 448,
        stop_fraction: None,
        take_profit_fraction: None,
    });
    let logical_cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
    let cpu_workers = cpu_worker_budget(logical_cpus);
    let mut profile_rows = Vec::new();
    let mut expected_hashes: Option<Vec<String>> = None;
    let mut peak_working_set_bytes = process_peak_working_set_bytes();
    for requested_workers in [1usize, 4, 8]
        .into_iter()
        .filter(|count| *count <= cpu_workers)
    {
        let mut raw_ms = Vec::with_capacity(SAMPLES);
        for iteration in 0..(WARMUPS + SAMPLES) {
            if cancel.load(Ordering::Acquire) {
                return Err("CANCELLED".into());
            }
            let began = Instant::now();
            let hashes = run_grid(
                &cpu,
                &sources,
                &variants,
                &spec,
                cancel.clone(),
                requested_workers,
            )
            .await?;
            if let Some(reference) = &expected_hashes {
                if &hashes != reference {
                    return Err("K9_GRID_NON_DETERMINISTIC".into());
                }
            } else {
                expected_hashes = Some(hashes);
            }
            if iteration >= WARMUPS {
                raw_ms.push(began.elapsed().as_secs_f64() * 1000.0);
            }
            peak_working_set_bytes = process_peak_working_set_bytes()
                .or(peak_working_set_bytes)
                .map(|rss| rss.max(peak_working_set_bytes.unwrap_or_default()));
        }
        let mut sorted = raw_ms.clone();
        sorted.sort_by(f64::total_cmp);
        profile_rows.push(json!({
            "workers": requested_workers,
            "warmups": WARMUPS,
            "measurements": SAMPLES,
            "p50_ms": sorted[14],
            "p95_ms": sorted[28],
            "max_ms": sorted[29],
            "raw_ms": raw_ms,
            "research_latency_p95_pass": sorted[28] <= 60_000.0
        }));
    }
    let one_median = profile_rows
        .iter()
        .find(|row| row["workers"] == 1)
        .and_then(|row| row["p50_ms"].as_f64())
        .ok_or("K9_SINGLE_WORKER_MISSING")?;
    let four_median = profile_rows
        .iter()
        .find(|row| row["workers"] == 4)
        .and_then(|row| row["p50_ms"].as_f64());
    let result_hash =
        okx_research::canonical_sha256(&expected_hashes).map_err(|e| e.to_string())?;
    let profiles_not_evaluated = [1usize, 4, 8]
        .into_iter()
        .filter(|n| *n > cpu_workers)
        .collect::<Vec<_>>();
    let summary = json!({
        "schema": "okx.k9.native-research/v1",
        "scope": "running_agent_research_kernel; source_and_transport_latency_not_included",
        "operating_system": std::env::consts::OS,
        "logical_cpus": logical_cpus,
        "cpu_workers": cpu_workers,
        "physical_memory_bytes": physical_memory_bytes(),
        "peak_process_working_set_bytes": peak_working_set_bytes,
        "research_memory_reserved_mib": 3072,
        "research_memory_limit_mib": super::budget::MEMORY_TOTAL_MIB,
        "data_source": "deterministic_local_fixture_no_http",
        "instruments": INSTRUMENTS,
        "bars_per_instrument": BARS_PER_INSTRUMENT,
        "bars_total": INSTRUMENTS * BARS_PER_INSTRUMENT,
        "variants": VARIANTS,
        "fixture_hash": fixture_hash,
        "result_hash": result_hash,
        "profiles": profile_rows,
        "median_speedup_4_vs_1": four_median.map(|four| one_median / four),
        "profiles_not_evaluated": profiles_not_evaluated,
        "full_k9_slo_status": "NOT_EVALUATED_MCP_AND_FINANCE_UNDER_LOAD",
        "side_effects": "research_sqlite_job_and_immutable_artifact_only"
    });
    if cancel.load(Ordering::Acquire) {
        return Err("CANCELLED".into());
    }
    artifacts
        .publish_evidence(&json!({
            "kind": "k9_native_benchmark",
            "summary": summary,
            "hash": result_hash,
            "code_version": "448-k9-native/v1"
        }))
        .map(|(id, _)| id)
        .map_err(|e| e.to_string())
}

/// Exactly the same grid algorithm and bounded CPU permits as ordinary
/// Research::Grid. Requested workers are bounded by cpu_worker_budget().
async fn run_grid(
    cpu: &CpuExecutor,
    sources: &Arc<Vec<Arc<Vec<Bar>>>>,
    variants: &Arc<Vec<EmaParameters>>,
    spec: &Arc<ReplaySpec>,
    cancel: Arc<AtomicBool>,
    workers: usize,
) -> Result<Vec<String>, String> {
    let tasks = (0..sources.len()).map(|index| {
        let cpu = cpu.clone();
        let bars = sources[index].clone();
        let variants = variants.clone();
        let spec = spec.clone();
        let token = cancel.clone();
        async move {
            let hash = cpu
                .run(token, move |signal| {
                    replay::grid(&bars, &spec, &variants, &|| signal.load(Ordering::Acquire))
                        .map(|grid| grid.test.hash)
                })
                .await?;
            Ok::<_, String>((index, hash))
        }
    });
    let mut active = stream::iter(tasks).buffer_unordered(workers);
    let mut hashes = vec![String::new(); sources.len()];
    while let Some(done) = active.next().await {
        let (index, hash) = done?;
        hashes[index] = hash;
    }
    Ok(hashes)
}

#[cfg(windows)]
fn process_peak_working_set_bytes() -> Option<usize> {
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
    let mut counters = Counters {
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
    let ok = unsafe {
        GetProcessMemoryInfo(
            GetCurrentProcess(),
            &mut counters,
            std::mem::size_of::<Counters>() as u32,
        )
    };
    (ok != 0).then_some(counters.peak)
}
#[cfg(not(windows))]
fn process_peak_working_set_bytes() -> Option<usize> {
    None
}

#[cfg(windows)]
fn physical_memory_bytes() -> Option<u64> {
    #[repr(C)]
    struct Status {
        length: u32,
        load: u32,
        total_phys: u64,
        avail_phys: u64,
        total_pagefile: u64,
        avail_pagefile: u64,
        total_virtual: u64,
        avail_virtual: u64,
        avail_extended: u64,
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GlobalMemoryStatusEx(status: *mut Status) -> i32;
    }
    let mut status = Status {
        length: std::mem::size_of::<Status>() as u32,
        load: 0,
        total_phys: 0,
        avail_phys: 0,
        total_pagefile: 0,
        avail_pagefile: 0,
        total_virtual: 0,
        avail_virtual: 0,
        avail_extended: 0,
    };
    let ok = unsafe { GlobalMemoryStatusEx(&mut status) };
    (ok != 0).then_some(status.total_phys)
}
#[cfg(not(windows))]
fn physical_memory_bytes() -> Option<u64> {
    None
}
