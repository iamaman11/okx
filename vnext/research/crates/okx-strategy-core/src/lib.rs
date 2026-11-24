//! Deterministic strategy operators and compiled causal query plans.
//! Research owns this crate and its internal graph types.
#![forbid(unsafe_code)]

pub mod operators;
pub mod query_plan;
pub mod strategy_contract;
pub mod stream;
pub mod time_series;
