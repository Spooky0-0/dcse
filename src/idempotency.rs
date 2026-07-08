use bloomfilter::Bloom;
use sled::Db;
use std::path::Path;
use uuid::Uuid;
use crossbeam_channel::{Receiver, Sender};
use crate::error::IdempotencyError;
use crate::transaction_coordinator::{TransactionCoordinator, TransactionState};

/// Hybrid Idempotency Store utilizing a Bloom filter and a persistent KV store.
pub struct IdempotencyStore {
    bloom: Bloom<Uuid>,
    db: Db,
}

impl IdempotencyStore {
    /// Creates a new `IdempotencyStore`.
    /// 
    /// # Errors
    /// Returns an `std::io::Error` if the underlying Sled DB cannot be opened.
    ///
    /// # Panics
    /// Panics if the Bloom filter fails to allocate the requested size.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let db = sled::open(path)?;
        // We use a bloom filter for 10M items, 1% false positive rate
        let bloom = Bloom::new_for_fp_rate(10_000_000, 0.01).expect("Failed to initialize bloom filter");
        
        Ok(Self { bloom, db })
    }

    /// Checks if a trade ID has been processed. Returns true if it exists.
    /// 
    /// # Errors
    /// Returns `IdempotencyError` on storage failure.
    pub fn is_processed(&self, trade_id: &Uuid) -> Result<bool, IdempotencyError> {
        if !self.bloom.check(trade_id) {
            // Definitively not in the set
            return Ok(false);
        }

        // Bloom filter says yes (or maybe). Fallback to persistent DB
        let exists = self.db.contains_key(trade_id.as_bytes())
            .map_err(|e| IdempotencyError::StoreError(e.to_string()))?;

        Ok(exists)
    }

    /// Records a trade ID as processed.
    /// 
    /// # Errors
    /// Returns `IdempotencyError` on storage failure.
    pub fn record_trade(&mut self, trade_id: &Uuid) -> Result<(), IdempotencyError> {
        self.bloom.set(trade_id);
        self.db.insert(trade_id.as_bytes(), &[])
            .map_err(|e| IdempotencyError::StoreError(e.to_string()))?;
        self.db.flush()
            .map_err(|e| IdempotencyError::StoreError(e.to_string()))?;
        Ok(())
    }
}

/// A trade event payload representing a settlement request.
#[derive(Debug, Clone)]
pub struct TradeEvent {
    /// Unique identifier for the trade.
    pub trade_id: Uuid,
    /// Client ID of the buyer.
    pub buyer_id: Uuid,
    /// Client ID of the seller.
    pub seller_id: Uuid,
    /// Amount to be settled.
    pub amount: u64,
}

/// The trade processor orchestrating inbound events with the 2PC engine.
pub struct TradeProcessor {
    coordinator: TransactionCoordinator,
    idempotency_store: IdempotencyStore,
    event_receiver: Receiver<TradeEvent>,
    /// Sender channel for injecting trade events.
    pub event_sender: Sender<TradeEvent>, 
}

impl TradeProcessor {
    /// Creates a new `TradeProcessor`.
    #[must_use]
    pub fn new(coordinator: TransactionCoordinator, idempotency_store: IdempotencyStore) -> Self {
        let (event_sender, event_receiver) = crossbeam_channel::unbounded();
        Self {
            coordinator,
            idempotency_store,
            event_receiver,
            event_sender,
        }
    }

    /// Process the next event from the queue asynchronously.
    /// 
    /// # Errors
    /// Returns `DcseError` if coordination or idempotency checks fail.
    pub async fn process_next(&mut self) -> Result<Option<TradeEvent>, crate::error::DcseError> {
        let Ok(event) = self.event_receiver.try_recv() else {
            return Ok(None);
        };

        let trade_id = event.trade_id;

        // 1. Idempotency Check
        if self.idempotency_store.is_processed(&trade_id)? {
            // Duplicate event, skip processing
            return Ok(Some(event));
        }

        // 2. Start Coordinator Phase
        self.coordinator.start_transaction(trade_id)?;

        // Move to Prepare Phase
        self.coordinator.transition_state(trade_id, TransactionState::Prepared).await?;

        // ... Await network ledgers ...
        
        // Move to Commit Phase
        self.coordinator.transition_state(trade_id, TransactionState::Committed).await?;

        // 3. Mark as processed
        self.idempotency_store.record_trade(&trade_id)?;

        Ok(Some(event))
    }
}
