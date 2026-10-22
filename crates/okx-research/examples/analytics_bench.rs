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
