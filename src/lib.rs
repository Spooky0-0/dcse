#![warn(missing_docs, clippy::pedantic)]

//! Distributed Clearing & Settlement Engine (DCSE)
//! 
//! A high-throughput, crash-consistent settlement engine implementing
//! the Two-Phase Commit (2PC) pattern.

pub mod error;
/// Distributed Two-Phase Commit transaction coordinator.
pub mod transaction_coordinator;
/// Ledger account storage and preparation phase mechanics.
pub mod ledger_participant;
/// Idempotent messaging layer and bloom filters.
pub mod idempotency;

pub use error::*;
pub use transaction_coordinator::*;
pub use ledger_participant::*;
pub use idempotency::*;
