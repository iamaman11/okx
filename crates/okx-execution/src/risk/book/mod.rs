//! Native SQLite campaign owner, not a second policy authority.
//! The module is private until authenticated initialization, legacy fencing,
//! full terminal rules and production callers are proved by #510.
//! No public opening or POST permission exists in this intermediate draft.
mod store;
mod owner;
#[cfg(test)]
mod tests;
