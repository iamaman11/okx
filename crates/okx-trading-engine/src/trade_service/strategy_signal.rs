//! Closed-bar decisions and durable identities for installed Engine strategies.
use okx_ipc_contracts::trading::StrategyParametersV1;
use okx_trade_execution::StrategyInstance;
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Position {
    Long,
    Short,
    Flat,
}

pub(super) fn supports_definition(definition_id: &str) -> bool {
    matches!(
        definition_id,
        "ema_cross_v1" | "no_trade" | "close_momentum" | "two_bar_momentum"
    )
}

pub(super) fn evaluate(
    definition_id: &str,
    parameters: &StrategyParametersV1,
    bars: &[(u64, f64)],
) -> Result<Option<Position>, &'static str> {
    if bars.is_empty()
        || bars.len() > 10_000
        || !parameters.valid()
        || bars
            .iter()
            .any(|&(time, close)| time == 0 || !close.is_finite() || close <= 0.0)
        || bars.windows(2).any(|pair| pair[0].0 >= pair[1].0)
    {
        return Err("INVALID_STRATEGY_CLOSED_BARS");
    }

    let position = match (definition_id, bars) {
        ("no_trade", _) => Some(Position::Flat),
        ("close_momentum", [.., (_, previous), (_, close)]) => Some(direction(*close, *previous)),
        ("two_bar_momentum", [.., (_, first), (_, previous), (_, close)]) => {
            let prior = direction(*previous, *first);
            let current = direction(*close, *previous);
            Some(if prior == current {
                current
            } else {
                Position::Flat
            })
        }
        ("ema_cross_v1", [(_, first), rest @ ..]) if bars.len() >= parameters.slow => {
            let mut fast = *first;
            let mut slow = *first;
            for &(_, close) in rest {
                fast += 2.0 / (parameters.fast + 1) as f64 * (close - fast);
                slow += 2.0 / (parameters.slow + 1) as f64 * (close - slow);
            }
            Some(direction(fast, slow))
        }
        ("close_momentum" | "two_bar_momentum" | "ema_cross_v1", _) => None,
        _ => return Err("UNSUPPORTED_STRATEGY"),
    };
    Ok(position)
}

fn direction(current: f64, previous: f64) -> Position {
    if current > previous {
        Position::Long
    } else if current < previous {
        Position::Short
    } else {
        Position::Flat
    }
}

pub(super) fn require_fresh_close(
    last_closed_ms: u64,
    timeframe_ms: u64,
    now_ms: i64,
) -> Result<(), &'static str> {
    let now = u64::try_from(now_ms).map_err(|_| "STRATEGY_SIGNAL_TIME_INVALID")?;
    if last_closed_ms == 0
        || last_closed_ms >= now
        || now - last_closed_ms >= timeframe_ms.saturating_mul(2)
    {
        return Err("STRATEGY_SIGNAL_EXPIRED_OR_NONCAUSAL");
    }
    Ok(())
}

/// Preserve the persisted key preimage so a previously processed bar cannot
/// acquire a second SQL claim during the transition to local evaluation.
pub(super) fn scoped_decision_key(
    authenticated_account_key: &str,
    instance: &StrategyInstance,
    instrument: &str,
    timeframe_ms: u64,
    last_closed_ms: u64,
) -> Result<String, &'static str> {
    Ok(format!(
        "bar_{}",
        &digest(&(
            authenticated_account_key,
            &instance.instance_id,
            &instance.definition_id,
            &instance.definition_version,
            &instance.parameters,
            instrument,
            timeframe_ms,
            last_closed_ms,
        ))?[..40]
    ))
}

fn digest(value: &impl Serialize) -> Result<String, &'static str> {
    let bytes = serde_json::to_vec(value).map_err(|_| "STRATEGY_SIGNAL_IDENTITY_ENCODE")?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installed_definitions_preserve_closed_bar_decisions() {
        let p = StrategyParametersV1 { fast: 1, slow: 3 };
        for (prices, expected) in [
            ([10.0, 12.0, 14.0], Position::Long),
            ([14.0, 12.0, 10.0], Position::Short),
            ([10.0, 10.0, 10.0], Position::Flat),
        ] {
            let bars = [(1, prices[0]), (2, prices[1]), (3, prices[2])];
            for definition in ["ema_cross_v1", "close_momentum", "two_bar_momentum"] {
                assert_eq!(evaluate(definition, &p, &bars), Ok(Some(expected)));
            }
            assert_eq!(evaluate("ema_cross_v1", &p, &bars[..2]), Ok(None));
            assert_eq!(evaluate("no_trade", &p, &bars), Ok(Some(Position::Flat)));
        }
        assert_eq!(
            evaluate("ema_cross_v1", &p, &[(1, 10.0), (1, 12.0)]),
            Err("INVALID_STRATEGY_CLOSED_BARS")
        );
        assert_eq!(
            evaluate("ema_cross_v1", &p, &[(1, f64::NAN)]),
            Err("INVALID_STRATEGY_CLOSED_BARS")
        );
    }

    #[test]
    fn decision_rejects_stale_and_noncausal_closed_bars() {
        assert!(require_fresh_close(60_000, 60_000, 60_001).is_ok());
        assert!(require_fresh_close(60_000, 60_000, 179_999).is_ok());
        assert!(require_fresh_close(60_000, 60_000, 180_000).is_err());
        assert!(require_fresh_close(60_000, 60_000, 60_000).is_err());
        assert!(require_fresh_close(60_000, 60_000, 59_999).is_err());
    }
}
