use okx_strategy_core::{
    query_plan::{CheckedComposition, CompositionRequest, CompositionValues},
    stream::{CausalGraphStream, ClosedBar},
};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

/// Source cadence is validated at the application boundary. The pure
/// mathematical kernel does not own source clocks or transport semantics.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(try_from = "i64")]
pub struct TimeframeMs(i64);

impl TryFrom<i64> for TimeframeMs {
    type Error = &'static str;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if matches!(
            value,
            60_000 | 180_000 | 300_000 | 900_000 | 1_800_000 | 3_600_000 | 14_400_000
                | 86_400_000
        ) {
            Ok(Self(value))
        } else {
            Err("research.replay:UNSUPPORTED_TIMEFRAME")
        }
    }
}

/// Stable diagnostic codes at the Research application boundary. The pure
/// operator crate may retain detailed errors without treating them as
/// unvalidated protocol state.
#[derive(Debug, PartialEq, Eq)]
pub enum ReplayError {
    EmptySeries,
    Misaligned { index: usize },
    GapOrReorder { index: usize },
    NotAvailable { index: usize },
    AvailabilityReordered { index: usize },
    InvalidClose { index: usize },
    Graph(String),
}

impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySeries => write!(f, "research.replay:EMPTY_SERIES"),
            Self::Misaligned { index } => write!(f, "research.replay:OFF_GRID:index={index}"),
            Self::GapOrReorder { index } => {
                write!(f, "research.replay:GAP_OR_REORDER:index={index}")
            }
            Self::NotAvailable { index } => {
                write!(f, "research.replay:BAR_NOT_CLOSED_OR_AVAILABLE:index={index}")
            }
            Self::AvailabilityReordered { index } => {
                write!(f, "research.replay:AVAILABILITY_REORDERED:index={index}")
            }
            Self::InvalidClose { index } => {
                write!(f, "research.replay:INVALID_CLOSE:index={index}")
            }
            Self::Graph(detail) => write!(f, "research.replay:GRAPH:{detail}"),
        }
    }
}

impl Error for ReplayError {}

impl From<String> for ReplayError {
    fn from(error: String) -> Self {
        Self::Graph(error)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputBar {
    pub timestamp_ms: i64,
    /// Observation availability is never inferred from the candle timestamp.
    pub available_at_ms: i64,
    pub close: Option<f64>,
}

#[derive(Serialize)]
pub struct ResultBar {
    timestamp_ms: i64,
    available_at_ms: i64,
    values: CompositionValues,
}

pub fn replay(
    graph: CompositionRequest,
    timeframe: TimeframeMs,
    bars: Vec<InputBar>,
) -> Result<Vec<ResultBar>, ReplayError> {
    if bars.is_empty() {
        return Err(ReplayError::EmptySeries);
    }
    // Reject the *whole* invalid series before computing or publishing any
    // mathematical prefix. No rounding, forward filling or inferred closes.
    let mut previous: Option<(i64, i64)> = None;
    for (index, bar) in bars.iter().enumerate() {
        let close_at = bar
            .timestamp_ms
            .checked_add(timeframe.0)
            .filter(|_| bar.timestamp_ms >= 0 && bar.timestamp_ms % timeframe.0 == 0)
            .ok_or(ReplayError::Misaligned { index })?;
        if bar.available_at_ms < close_at {
            return Err(ReplayError::NotAvailable { index });
        }
        if bar.close.is_some_and(|price| !price.is_finite() || price <= 0.0) {
            return Err(ReplayError::InvalidClose { index });
        }
        if let Some((last_timestamp, last_available)) = previous {
            if last_timestamp.checked_add(timeframe.0) != Some(bar.timestamp_ms) {
                return Err(ReplayError::GapOrReorder { index });
            }
            if bar.available_at_ms < last_available {
                return Err(ReplayError::AvailabilityReordered { index });
            }
        }
        previous = Some((bar.timestamp_ms, bar.available_at_ms));
    }
    let plan = CheckedComposition::compile(graph, bars.len() as u64)?;
    let mut stream = CausalGraphStream::new(&plan)?;
    bars.into_iter()
        .map(|bar| {
            let values = stream.on_closed_bar(ClosedBar {
                timestamp_ms: bar.timestamp_ms,
                close: bar.close,
            })?;
            Ok(ResultBar {
                timestamp_ms: bar.timestamp_ms,
                available_at_ms: bar.available_at_ms,
                values,
            })
        })
        .collect::<Result<Vec<_>, ReplayError>>()
}
