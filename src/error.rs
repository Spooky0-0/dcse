#![allow(missing_docs)]
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum SettlementError {
    #[error("Ledger error: {0}")]
    Ledger(#[from] LedgerError),

    #[error("Coordinator error: {0}")]
    Coordinator(#[from] CoordinatorError),

    #[error("Idempotency error: {0}")]
    Idempotency(#[from] IdempotencyError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
}

pub type DcseError = SettlementError;

#[derive(Error, Debug, PartialEq)]
pub enum LedgerError {
    #[error("Insufficient funds for client {0}")]
    InsufficientFunds(Uuid),

    #[error("Account not found for client {0}")]
    AccountNotFound(Uuid),

    #[error("Transaction {0} is not in prepared state")]
    NotPrepared(Uuid),

    #[error("Transaction {0} is already processed")]
    AlreadyProcessed(Uuid),
}

#[derive(Error, Debug)]
pub enum CoordinatorError {
    #[error("Participant timed out or failed to prepare")]
    PrepareFailed,

    #[error("Participant failed to commit")]
    CommitFailed,

    #[error("Transaction {0} not found in coordinator")]
    TransactionNotFound(Uuid),

    #[error("Invalid state transition for transaction {0}")]
    InvalidStateTransition(Uuid),

    #[error("I/O Error: {0}")]
    IoError(String),

    #[error("Serialization Error: {0}")]
    SerializationError(String),
}

#[derive(Error, Debug)]
pub enum IdempotencyError {
    #[error("Store error: {0}")]
    StoreError(String),
}
