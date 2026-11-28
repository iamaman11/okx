//! Shared semantic vocabulary for existing native operation catalogs.
//!
//! This is contract metadata, not an executor or a second registry. Market,
//! Research and the checked compiler continue using their existing owners.
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationShape {
    Scalar,
    Series,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationEffect {
    PureNoIo,
}

/// The time at which a value is defined is not necessarily the time at which
/// an external source made it available. Source admission remains authoritative.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeBasis {
    ClosedBar,
    OutcomeFutureHorizon,
    ClosedCandles,
    ExactClosedCandleJoin,
    CurrentTickerSnapshotNotHistorical,
    RealizedFundingEventTimeNotPublicationTime,
    OpenInterestEventTimeNotPublicationTime,
    RequestParameters,
}

/// Discovery metadata only; it must not grant historical source availability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DataAvailability {
    CheckedPerRequest,
    RequiresThirtyCommonEligiblePairs,
    LiveSnapshotTimestampUnverified,
    HistoricalPublicationTimeUnverified,
    RequiresTypedRequestArguments,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_and_series_catalogs_use_one_semantic_vocabulary() {
        let scalar = crate::operators::registry(crate::operators::Operator::Returns);
        let series = crate::query_plan::series_operation_contract(
            crate::query_plan::SeriesOp::ReturnsSeries,
        );
        assert_eq!(scalar.output_shape, OperationShape::Scalar);
        assert_eq!(series.output_shape, OperationShape::Series);
        assert_eq!(scalar.effects, series.effects);
        assert_eq!(serde_json::to_value(scalar.time_basis).unwrap(), "closed_candles");
        assert_eq!(serde_json::to_value(series.time_basis).unwrap(), "closed_bar");
    }
}
