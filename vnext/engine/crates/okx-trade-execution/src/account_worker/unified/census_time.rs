//! Unix millisecond timestamps supplied by the census producer.
use crate::{StoreError, StoreResult};

/// The time at which a census was observed, distinct from its source read floor.
///
/// ```
/// use okx_trade_execution::account_worker::{CensusObservedAtMs, CensusReadFloorMs, ReportedRiskCensus};
/// fn assign(census: &mut ReportedRiskCensus, observed: CensusObservedAtMs, floor: CensusReadFloorMs) {
///     census.generated_at_ms = observed;
///     census.read_floor_ms = floor;
/// }
/// ```
///
/// ```compile_fail,E0308
/// use okx_trade_execution::account_worker::{CensusObservedAtMs, CensusReadFloorMs, ReportedRiskCensus};
/// fn assign(census: &mut ReportedRiskCensus, observed: CensusObservedAtMs, floor: CensusReadFloorMs) {
///     census.generated_at_ms = floor;
///     census.read_floor_ms = observed;
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CensusObservedAtMs(i64);

impl CensusObservedAtMs {
    pub fn new(value: i64) -> StoreResult<Self> {
        if value <= 0 {
            return Err(StoreError::Invalid("NATIVE_ADMISSION_CENSUS_INVALID"));
        }
        Ok(Self(value))
    }

    pub fn raw_millis(self) -> i64 {
        self.0
    }
}

/// The earliest source observation included in a census, in Unix milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CensusReadFloorMs(i64);

impl CensusReadFloorMs {
    pub fn new(value: i64) -> StoreResult<Self> {
        if value <= 0 {
            return Err(StoreError::Invalid("NATIVE_ADMISSION_CENSUS_INVALID"));
        }
        Ok(Self(value))
    }

    pub fn raw_millis(self) -> i64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_require_positive_milliseconds() {
        assert!(CensusObservedAtMs::new(0).is_err());
        assert!(CensusReadFloorMs::new(-1).is_err());
        assert_eq!(CensusObservedAtMs::new(1).unwrap().raw_millis(), 1);
        assert_eq!(CensusReadFloorMs::new(2).unwrap().raw_millis(), 2);
    }
}
