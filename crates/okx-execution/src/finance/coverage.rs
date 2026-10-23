use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Coverage {
    Complete,
    Partial { reason: String },
    UnavailableBeforeT0,
}
pub fn coverage_at(time: i64, t0: i64, gaps: &[(i64, i64)]) -> Coverage {
    if time < t0 {
        Coverage::UnavailableBeforeT0
    } else if gaps.iter().any(|(a, b)| time >= *a && time <= *b) {
        Coverage::Partial {
            reason: "SOURCE_COVERAGE_GAP".into(),
        }
    } else {
        Coverage::Complete
    }
}
