use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Semaphore;
#[derive(Clone)]
pub struct CpuExecutor {
    cpu: Arc<Semaphore>,
    queue: Arc<Semaphore>,
}
fn cpu_worker_budget(logical_cores: usize) -> usize {
    logical_cores.saturating_sub(2).max(1)
}
impl Default for CpuExecutor {
    fn default() -> Self {
        let n = cpu_worker_budget(
            std::thread::available_parallelism().map_or(1, |n| n.get()),
        );
        Self {
            cpu: Arc::new(Semaphore::new(n)),
            queue: Arc::new(Semaphore::new(32)),
        }
    }
}
impl CpuExecutor {
    pub async fn run<T: Send + 'static>(
        &self,
        cancel: Arc<AtomicBool>,
        f: impl FnOnce(Arc<AtomicBool>) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String> {
        let queue = self
            .queue
            .clone()
            .try_acquire_owned()
            .map_err(|_| "CPU_QUEUE_FULL")?;
        let acquire = self.cpu.clone().acquire_owned();
        tokio::pin!(acquire);
        let permit = loop {
            tokio::select! {result=&mut acquire=>break result.map_err(|e|e.to_string())?,_=tokio::time::sleep(std::time::Duration::from_millis(25))=>{if cancel.load(Ordering::Acquire){return Err("CANCELLED".into())}}}
        };
        if cancel.load(Ordering::Acquire) {
            return Err("CANCELLED".into());
        }
        tokio::task::spawn_blocking(move || {
            let (_q, _p) = (queue, permit);
            f(cancel)
        })
        .await
        .map_err(|e| e.to_string())?
    }
}
pub async fn cancelled(cancel: Arc<AtomicBool>) {
    loop {
        if cancel.load(Ordering::Acquire) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn cpu_job_does_not_block_stop() {
        let executor = CpuExecutor::default();
        let cancel = Arc::new(AtomicBool::new(false));
        let work = executor.run(cancel.clone(), |c| {
            while !c.load(Ordering::Acquire) {
                std::thread::yield_now()
            }
            Ok(())
        });
        let stop = async {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            cancel.store(true, Ordering::Release);
        };
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            let (a, _) = tokio::join!(work, stop);
            a.unwrap();
        })
        .await
        .unwrap();
    }
    #[test]
    fn k9_worker_budget_reserves_two_cores_without_four_core_ceiling() {
        assert_eq!(cpu_worker_budget(1), 1);
        assert_eq!(cpu_worker_budget(2), 1);
        assert_eq!(cpu_worker_budget(8), 6);
        assert_eq!(cpu_worker_budget(16), 14);
    }
    #[tokio::test]
    async fn cancel_releases_queue_capacity() {
        let executor = CpuExecutor {
            cpu: Arc::new(Semaphore::new(0)),
            queue: Arc::new(Semaphore::new(1)),
        };
        let cancel = Arc::new(AtomicBool::new(false));
        let work = executor.run(cancel.clone(), |_| Ok(()));
        let stop = async {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            cancel.store(true, Ordering::Release);
        };
        let (a, _) = tokio::join!(work, stop);
        assert_eq!(a.unwrap_err(), "CANCELLED");
        assert_eq!(executor.queue.available_permits(), 1);
    }
}
#[cfg(test)]
mod heavy_fixtures {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_heavy_research_jobs_ingestion_stop_and_cancel_latency() {
        use okx_analysis::operators::EmaParameters;
        use okx_research::strategy_registry::*;
        let bars = Arc::new(
            (0..100000)
                .map(|i| {
                    let p = 100.0 + (i as f64 / 17.0).sin();
                    Bar {
                        time: i * 60000,
                        open: p,
                        high: p + 1.0,
                        low: p - 1.0,
                        close: p + 0.1,
                        volume: 1.0,
                        funding_rate: Some(0.0),
                    }
                })
                .collect::<Vec<_>>(),
        );
        let spec = ReplaySpec {
            strategy_id: "ema_cross_v1".into(),
            parameters: EmaParameters { fast: 10, slow: 30 },
            mechanics: Mechanics::Spot,
            costs: Costs {
                maker_fee_bps: 0.0,
                taker_fee_bps: 5.0,
                slippage_bps: 2.0,
                observed_funding: true,
            },
            timeframe_ms: 60000,
            historical_observed: false,
            historical_rules: false,
            historical_fees: false,
            seed: 398,
            stop_fraction: None,
            take_profit_fraction: None,
        };
        let variants = (0..100)
            .map(|i| EmaParameters {
                fast: i + 1,
                slow: i + 101,
            })
            .collect::<Vec<_>>();
        let mut stop_times = Vec::new();
        let mut cancel_times = Vec::new();
        for iteration in 0..35 {
            let cpu = CpuExecutor {
                cpu: Arc::new(Semaphore::new(2)),
                queue: Arc::new(Semaphore::new(32)),
            };
            let mut work = Vec::new();
            let mut starts = Vec::new();
            let mut tokens = Vec::new();
            for _ in 0..2 {
                let c = Arc::new(AtomicBool::new(false));
                tokens.push(c.clone());
                let executor = cpu.clone();
                let data = bars.clone();
                let spec = spec.clone();
                let variants = variants.clone();
                let (tx, rx) = tokio::sync::oneshot::channel();
                starts.push(rx);
                work.push(tokio::spawn(async move {
                    executor
                        .run(c, move |cancel| {
                            let _ = tx.send(());
                            grid(&data, &spec, &variants, &|| cancel.load(Ordering::Acquire))
                        })
                        .await
                }));
            }
            for start in starts {
                start.await.unwrap();
            }
            let ingestion = tokio::spawn(async {
                let mut total = 0u64;
                for chunk in 0..1000 {
                    for row in 0..100 {
                        total = total.wrapping_add(chunk + row);
                    }
                    tokio::task::yield_now().await;
                }
                total
            });
            let due = std::time::Instant::now() + std::time::Duration::from_millis(5);
            tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await;
            if iteration >= 5 {
                stop_times.push(due.elapsed().as_secs_f64() * 1000.0);
            }
            let cancel_start = std::time::Instant::now();
            for token in tokens {
                token.store(true, Ordering::Release);
            }
            for task in work {
                let result = tokio::time::timeout(std::time::Duration::from_secs(1), task)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(result.unwrap_err(), "CANCELLED");
            }
            if iteration >= 5 {
                cancel_times.push(cancel_start.elapsed().as_secs_f64() * 1000.0);
            }
            assert!(ingestion.await.unwrap() > 0);
        }
        stop_times.sort_by(f64::total_cmp);
        cancel_times.sort_by(f64::total_cmp);
        assert!(stop_times[28] <= 100.0);
        assert!(cancel_times[29] <= 1000.0);
        println!(
            "ANALYTICS_PERF_JSON:{}",
            serde_json::json!({"two_heavy_stop":{"build":if cfg!(debug_assertions){"debug"}else{"release"},"warmups":5,"measurements":30,"p50_ms":stop_times[14],"p95_ms":stop_times[28],"max_ms":stop_times[29],"cancel_p95_ms":cancel_times[28],"cancel_max_ms":cancel_times[29],"stage":"two actual causal grid jobs on blocking workers plus simulated typed ingestion; no exchange HTTP"}})
        );
    }
}
