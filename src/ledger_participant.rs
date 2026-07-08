use dashmap::{DashMap, DashSet};
use uuid::Uuid;
use crate::error::LedgerError;

/// Represents the state of a client account.
#[derive(Debug, Clone)]
pub struct AccountState {
    /// Funds available for new transactions.
    pub available: u64,
    /// Funds locked for pending transactions.
    pub reserved: u64,
    /// MVCC version of the account.
    pub version: u64,
}

impl AccountState {
    /// Creates a new `AccountState` with an initial balance.
    #[must_use]
    pub fn new(initial_balance: u64) -> Self {
        Self {
            available: initial_balance,
            reserved: 0,
            version: 0,
        }
    }
}

/// Represents a locked reservation during the 2PC Prepare phase.
#[derive(Debug, Clone)]
pub struct PreparedReservation {
    /// The client holding the reservation.
    pub client_id: Uuid,
    /// The amount of funds reserved.
    pub amount: u64,
}

/// The ledger participant responsible for maintaining balances and handling 2PC phases.
#[derive(Default)]
pub struct LedgerParticipant {
    /// Client accounts mapped by `Uuid`.
    accounts: DashMap<Uuid, AccountState>,
    /// Tracks which trades are currently holding reserved funds.
    prepared_trades: DashMap<Uuid, PreparedReservation>,
    /// Tracks successfully committed trades (debits) to ensure idempotency.
    processed_commits: DashSet<Uuid>,
    /// Tracks successfully credited trades to ensure idempotency.
    processed_credits: DashSet<Uuid>,
}

impl LedgerParticipant {
    /// Creates a new, empty `LedgerParticipant`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an account with an initial balance.
    pub fn create_account(&self, client_id: Uuid, initial_balance: u64) {
        self.accounts.insert(client_id, AccountState::new(initial_balance));
    }

    /// Retrieves a copy of the balance for a given client.
    #[must_use]
    pub fn get_balance(&self, client_id: &Uuid) -> Option<AccountState> {
        self.accounts.get(client_id).map(|ref_multi| ref_multi.clone())
    }

    /// Prepare Phase: Reserve funds.
    /// 
    /// # Errors
    /// Returns a `LedgerError` if funds are insufficient or the account is missing.
    pub fn prepare(&self, trade_id: Uuid, client_id: Uuid, amount: u64) -> Result<(), LedgerError> {
        if self.processed_commits.contains(&trade_id) {
            return Err(LedgerError::AlreadyProcessed(trade_id));
        }
        
        let mut account = self.accounts.get_mut(&client_id).ok_or(LedgerError::AccountNotFound(client_id))?;

        if account.available < amount {
            return Err(LedgerError::InsufficientFunds(client_id));
        }

        account.available -= amount;
        account.reserved += amount;
        account.version += 1;

        self.prepared_trades.insert(trade_id, PreparedReservation { client_id, amount });

        Ok(())
    }

    /// Commit Phase: Deduct funds permanently.
    /// 
    /// # Errors
    /// Returns a `LedgerError` if the trade is not in a prepared state.
    pub fn commit(&self, trade_id: Uuid) -> Result<(), LedgerError> {
        if self.processed_commits.contains(&trade_id) {
            return Ok(()); // Idempotent success
        }

        let (_, reservation) = self.prepared_trades.remove(&trade_id).ok_or(LedgerError::NotPrepared(trade_id))?;
        
        let mut account = self.accounts.get_mut(&reservation.client_id).ok_or(LedgerError::AccountNotFound(reservation.client_id))?;

        account.reserved -= reservation.amount;
        account.version += 1;

        self.processed_commits.insert(trade_id);

        Ok(())
    }

    /// Abort Phase: Release reservations.
    /// 
    /// # Errors
    /// Never fails functionally, but returns `Result` for consistency.
    pub fn abort(&self, trade_id: Uuid) -> Result<(), LedgerError> {
        if self.processed_commits.contains(&trade_id) {
            return Ok(());
        }

        if let Some((_, reservation)) = self.prepared_trades.remove(&trade_id)
            && let Some(mut account) = self.accounts.get_mut(&reservation.client_id) {
                account.reserved -= reservation.amount;
                account.available += reservation.amount;
                account.version += 1;
            }

        Ok(())
    }

    /// Credits funds to an account (e.g., the seller receiving funds).
    /// 
    /// # Errors
    /// Returns `LedgerError` if the account does not exist.
    pub fn credit(&self, trade_id: Uuid, client_id: Uuid, amount: u64) -> Result<(), LedgerError> {
         if self.processed_credits.contains(&trade_id) {
            return Ok(()); // Idempotent success
        }
        let mut account = self.accounts.get_mut(&client_id).ok_or(LedgerError::AccountNotFound(client_id))?;
        account.available += amount;
        account.version += 1;
        self.processed_credits.insert(trade_id);
        Ok(())
    }
}
