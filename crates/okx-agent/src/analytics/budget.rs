//! Admission estimates for the existing research CPU owner.
//! MiB permits cover input, intermediate, artifact staging and output headroom.
//! These are conservative reservations, NOT observed RSS measurements.
use okx_protocol::analytics::ResearchRequest;

pub const MEMORY_TOTAL_MIB: u32 = 4096;
const MIB: u64 = 1024 * 1024;
const ESTIMATED_BYTES_PER_BAR: u64 = 2048;
const ESTIMATED_BYTES_PER_INSTRUMENT: u64 = 16 * MIB;
const ESTIMATED_BYTES_PER_VARIANT: u64 = 64 * 1024;

pub fn estimated_mib(request: &ResearchRequest) -> Result<u32, String> {
    let (instruments, start_ms, end_ms, timeframe_ms, variant_count) = match request {
        ResearchRequest::Backtest {
            instruments,
            start_ms,
            end_ms,
            timeframe_ms,
            ..
        } => (instruments.len(), *start_ms, *end_ms, *timeframe_ms, 1),
        ResearchRequest::Grid {
            instruments,
            start_ms,
            end_ms,
            timeframe_ms,
            variants,
            ..
        } => (
            instruments.len(),
            *start_ms,
            *end_ms,
            *timeframe_ms,
            variants.len(),
        ),
        _ => return Err("RESOURCE_BUDGET_NOT_RESEARCH_JOB".into()),
    };
    if instruments == 0
        || instruments > 8
        || timeframe_ms == 0
        || end_ms <= start_ms
        || variant_count == 0
        || variant_count > 64
    {
        return Err("LIMIT_EXCEEDED:RESEARCH_REQUEST".into());
    }
    let bars = (end_ms - start_ms)
        .div_ceil(timeframe_ms)
        .checked_mul(instruments as u64)
        .ok_or("LIMIT_EXCEEDED:RESEARCH_BARS")?;
    if bars > 1_000_000 {
        return Err("LIMIT_EXCEEDED:RESEARCH_BARS".into());
    }
    let bytes = bars
        .checked_mul(ESTIMATED_BYTES_PER_BAR)
        .and_then(|n| n.checked_add(instruments as u64 * ESTIMATED_BYTES_PER_INSTRUMENT))
        .and_then(|n| {
            n.checked_add(instruments as u64 * variant_count as u64 * ESTIMATED_BYTES_PER_VARIANT)
        })
        .ok_or("LIMIT_EXCEEDED:RESEARCH_MEMORY")?;
    let mib = bytes.div_ceil(MIB).max(1);
    if mib > MEMORY_TOTAL_MIB as u64 {
        return Err("LIMIT_EXCEEDED:RESEARCH_MEMORY".into());
    }
    u32::try_from(mib).map_err(|_| "LIMIT_EXCEEDED:RESEARCH_MEMORY".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_protocol::analytics::*;
    fn job(instruments: usize, bars: u64, variants: usize) -> ResearchRequest {
        ResearchRequest::Grid {
            idempotency_key: "k9-test".into(),
            instruments: (0..instruments).map(|i| format!("TEST-{i}")).collect(),
            timeframe_ms: 60_000,
            start_ms: 60_000,
            end_ms: 60_000 * (bars + 1),
            strategy_id: "ema_cross_v1".into(),
            variants: (0..variants)
                .map(|i| EmaParameters {
                    fast: 2 + i,
                    slow: 90 + i,
                })
                .collect(),
            costs: ResearchCosts {
                maker_fee_bps: 0.0,
                taker_fee_bps: 5.0,
                slippage_bps: 2.0,
                funding_mode: FundingMode::ZeroAssumption,
            },
            data_mode: ResearchDataMode::Counterfactual,
            seed: 448,
        }
    }
    #[test]
    fn k9_fixed_eight_instruments_sixty_four_variants_admitted_before_data_loading() {
        let request = job(8, 125_000, 64);
        assert!(request.valid());
        let budget = estimated_mib(&request).unwrap();
        assert!(budget > 2000 && budget < MEMORY_TOTAL_MIB);
        assert!(budget.saturating_mul(2) > MEMORY_TOTAL_MIB);
        assert_eq!(estimated_mib(&request).unwrap(), budget);
    }
    #[test]
    fn k9_rejects_oversized_input_without_allocating_source_bars() {
        let request = job(8, 125_001, 64);
        assert_eq!(
            estimated_mib(&request).unwrap_err(),
            "LIMIT_EXCEEDED:RESEARCH_BARS"
        );
        let request = job(8, 125_000, 65);
        assert_eq!(
            estimated_mib(&request).unwrap_err(),
            "LIMIT_EXCEEDED:RESEARCH_REQUEST"
        );
    }
    #[tokio::test]
    async fn k9_shared_four_gib_memory_permits_not_released_until_task_exits() {
        let request = job(8, 125_000, 64);
        let permits = estimated_mib(&request).unwrap();
        let pool = std::sync::Arc::new(tokio::sync::Semaphore::new(MEMORY_TOTAL_MIB as usize));
        let held = pool.clone().acquire_many_owned(permits).await.unwrap();
        assert!(pool.clone().try_acquire_many_owned(permits).is_err());
        drop(held);
        assert!(pool.clone().try_acquire_many_owned(permits).is_ok());
    }
}
