//! Typed ownership schema for the future Control application.
//!
//! This crate does not launch processes, install software, or authorize money
//! movement. A command describes an operation; verification of user authority,
//! artifact provenance, and Engine fencing must happen before side effects.

pub mod model;

pub use model::*;
