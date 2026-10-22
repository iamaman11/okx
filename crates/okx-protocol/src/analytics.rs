//! Bounded analytical result contracts; missing values remain explicit.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultPage<T> {
    pub rows: Vec<T>,
    pub returned_rows: usize,
    pub total_rows: usize,
    pub next_cursor: Option<String>,
}

impl<T> ResultPage<T> {
    pub fn valid(&self) -> bool {
        self.rows.len() <= 50
            && self.returned_rows == self.rows.len()
            && self.returned_rows <= self.total_rows
    }
}
