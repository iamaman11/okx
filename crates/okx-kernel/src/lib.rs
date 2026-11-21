//! Stable facts, risk and wire contracts for native execution and research.
//! No venue API, transport, database, filesystem adapter or async runtime.
#![forbid(unsafe_code)]

pub mod fee_model;
pub mod observation_model;
pub mod risk;
pub mod strategy_contract;
pub mod time_series;
