//! Deterministic strategy operators and compiled causal query plans.
//! Research owns this crate. Native Engine depends only on stable contracts.
#![forbid(unsafe_code)]

pub mod operators;
pub mod query_plan;
pub mod stream;
