//! Pure, deterministic typed analysis primitives shared by research and live code.
//! No venue API, transport, database, filesystem adapter or async runtime.
#![forbid(unsafe_code)]

pub mod operators;
pub mod query_plan;
pub mod time_series;
