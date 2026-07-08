use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

use crate::error::CoordinatorError;

/// Represents the global state of a distributed transaction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction has been received but not yet processed.
    Pending,
    /// Coordinator has instructed ledgers to lock funds.
    Prepared,
    /// Transaction is definitively finalized.
    Committed,
    /// Transaction has been cancelled.
    Aborted,
}

/// An entry written to the Write-Ahead Log.
#[derive(Debug, Serialize, Deserialize)]
pub struct WalEntry {
    /// The unique trade ID.
    pub trade_id: Uuid,
    /// The new state being transitioned into.
    pub state: TransactionState,
}

/// The asynchronous orchestrator that drives the Two-Phase Commit process.
pub struct TransactionCoordinator {
    pending_transactions: Arc<DashMap<Uuid, TransactionState>>,
    wal_sender: mpsc::Sender<(WalEntry, tokio::sync::oneshot::Sender<Result<(), CoordinatorError>>)>,
}

impl TransactionCoordinator {
    /// Creates a new `TransactionCoordinator` and spawns the background WAL group commit task.
    /// 
    /// # Errors
    /// Returns an `std::io::Error` if the WAL file cannot be opened.
    pub async fn new<P: AsRef<Path>>(wal_path: P) -> Result<Self, std::io::Error> {
        let wal_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(wal_path)
            .await?;

        // Channel for sending entries to the WAL flusher.
        // Bounded channel provides backpressure if I/O is saturated.
        let (wal_sender, wal_receiver) = mpsc::channel(100_000);

        // Spawn the group commit background task
        tokio::spawn(Self::wal_group_commit_task(wal_file, wal_receiver));

        Ok(Self {
            pending_transactions: Arc::new(DashMap::new()),
            wal_sender,
        })
    }

    /// Background task that buffers WAL entries and flushes them to disk
    /// based on a 64KB size threshold or a 10ms timer, achieving Group Commit.
    async fn wal_group_commit_task(
        wal_file: File,
        mut receiver: mpsc::Receiver<(WalEntry, tokio::sync::oneshot::Sender<Result<(), CoordinatorError>>)>,
    ) {
        let mut writer = BufWriter::with_capacity(65536, wal_file); // 64KB buffer
        let mut pending_acks: Vec<tokio::sync::oneshot::Sender<Result<(), CoordinatorError>>> = Vec::new();
        let mut bytes_written_since_flush = 0;

        loop {
            tokio::select! {
                // 10ms latency bound to ensure no transaction lingers un-flushed
                () = sleep(Duration::from_millis(10)), if !pending_acks.is_empty() => {
                    if let Err(e) = writer.flush().await {
                        for ack in pending_acks.drain(..) {
                            let _ = ack.send(Err(CoordinatorError::IoError(e.to_string())));
                        }
                    } else {
                        // After flush, fsync to guarantee durability
                        if let Err(e) = writer.get_mut().sync_all().await {
                            for ack in pending_acks.drain(..) {
                                let _ = ack.send(Err(CoordinatorError::IoError(e.to_string())));
                            }
                        } else {
                            for ack in pending_acks.drain(..) {
                                let _ = ack.send(Ok(()));
                            }
                        }
                    }
                    bytes_written_since_flush = 0;
                }
                
                // Receive new WAL entry requests
                msg = receiver.recv() => {
                    match msg {
                        Some((entry, ack_sender)) => {
                            let encoded = match bincode::serialize(&entry) {
                                Ok(enc) => enc,
                                Err(e) => {
                                    let _ = ack_sender.send(Err(CoordinatorError::SerializationError(e.to_string())));
                                    continue;
                                }
                            };

                            let len_bytes = (encoded.len() as u64).to_le_bytes();
                            
                            // Write length prefix and payload to buffer
                            let write_res = async {
                                writer.write_all(&len_bytes).await?;
                                writer.write_all(&encoded).await?;
                                Ok::<(), std::io::Error>(())
                            }.await;

                            if let Err(e) = write_res {
                                let _ = ack_sender.send(Err(CoordinatorError::IoError(e.to_string())));
                                continue;
                            }

                            bytes_written_since_flush += len_bytes.len() + encoded.len();
                            pending_acks.push(ack_sender);

                            // Flush if we exceed 64KB threshold
                            if bytes_written_since_flush >= 65536 {
                                if let Err(e) = writer.flush().await {
                                    for ack in pending_acks.drain(..) {
                                        let _ = ack.send(Err(CoordinatorError::IoError(e.to_string())));
                                    }
                                } else if let Err(e) = writer.get_mut().sync_all().await {
                                    for ack in pending_acks.drain(..) {
                                        let _ = ack.send(Err(CoordinatorError::IoError(e.to_string())));
                                    }
                                } else {
                                    for ack in pending_acks.drain(..) {
                                        let _ = ack.send(Ok(()));
                                    }
                                }
                                bytes_written_since_flush = 0;
                            }
                        }
                        None => break, // Channel closed
                    }
                }
            }
        }
    }

    /// Transitions the state of a transaction asynchronously.
    /// 
    /// This method guarantees crash consistency by enforcing a WAL flush
    /// (via Group Commit) before updating the in-memory state.
    /// 
    /// # Errors
    /// Returns a `CoordinatorError` if the transition is invalid or I/O fails.
    pub async fn transition_state(
        &self,
        trade_id: Uuid,
        new_state: TransactionState,
    ) -> Result<(), CoordinatorError> {
        let current_state = self
            .pending_transactions
            .get(&trade_id)
            .map_or(TransactionState::Pending, |s| s.clone());

        match (current_state.clone(), &new_state) {
            (
                TransactionState::Pending,
                TransactionState::Prepared | TransactionState::Aborted,
            )
            | (
                TransactionState::Prepared,
                TransactionState::Committed | TransactionState::Aborted,
            ) => {}
            _ => {
                if current_state == new_state {
                    return Ok(());
                }
                return Err(CoordinatorError::InvalidStateTransition(trade_id));
            }
        }

        let entry = WalEntry {
            trade_id,
            state: new_state.clone(),
        };

        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        
        self.wal_sender
            .send((entry, ack_tx))
            .await
            .map_err(|_| CoordinatorError::CommitFailed)?; // Channel closed

        // Await confirmation that the group commit successfully fsync'd
        ack_rx.await.map_err(|_| CoordinatorError::CommitFailed)??;

        // Update in-memory state
        self.pending_transactions.insert(trade_id, new_state);

        Ok(())
    }

    /// Retrieves the current state of a transaction.
    #[must_use]
    pub fn get_state(&self, trade_id: &Uuid) -> Option<TransactionState> {
        self.pending_transactions.get(trade_id).map(|s| s.clone())
    }

    /// Marks the beginning of a 2PC workflow for a given trade.
    /// 
    /// # Errors
    /// Returns a `CoordinatorError` if the transaction cannot be started.
    pub fn start_transaction(&self, trade_id: Uuid) -> Result<(), CoordinatorError> {
        if self.pending_transactions.contains_key(&trade_id) {
            return Ok(()); // Idempotency
        }
        
        self.pending_transactions.insert(trade_id, TransactionState::Pending);
        Ok(())
    }
}
