//! Research owns jobs and artifacts; financial execution belongs to the host.
pub mod analytics;
pub mod edge_runtime;
pub mod independent_source;
pub(crate) mod invocation;
pub mod ipc;
pub mod process_memory;
pub mod producer;
mod public_catalog;
mod public_microstructure;
mod public_overview;
pub mod source_api;
pub(crate) mod source_io;
