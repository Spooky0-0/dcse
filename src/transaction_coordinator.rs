use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use uuid::Uuid;
use std::fs::rename;
use std::io::Write;

use crate::error::CoordinatorError;

/// Represents the global state of a distributed transaction.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
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

/// Payload written during a WAL compaction snapshot.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct SnapshotPayload {
    /// The sequence/transaction ID boundary of this snapshot.
    pub sequence_id: u64,
    /// The compact memory layout of transaction states.
    pub states: Vec<(Uuid, TransactionState)>,
}

/// The asynchronous orchestrator that drives the Two-Phase Commit process.
pub struct TransactionCoordinator {
    pending_transactions: Arc<DashMap<Uuid, TransactionState>>,
    wal_sender: mpsc::Sender<(WalEntry, tokio::sync::oneshot::Sender<Result<(), CoordinatorError>>)>,
}

impl TransactionCoordinator {
    /// Creates a new `TransactionCoordinator` and spawns the background WAL group commit task.
    /// 
    /// This will automatically attempt to recover from `wal_snapshot.bin` (if it exists),
    /// and then replay any remaining un-compacted logs from `wal.bin`.
    /// 
    /// # Errors
    /// Returns an `std::io::Error` if the WAL file cannot be opened.
    pub async fn new<P: AsRef<Path>>(wal_path: P) -> Result<Self, std::io::Error> {
        let wal_path_buf = wal_path.as_ref().to_path_buf();
        let wal_path_str = wal_path_buf.to_string_lossy().to_string();
        let snap_path_str = format!("{}_snapshot.bin", wal_path_str.trim_end_matches(".bin"));

        let pending_transactions = Arc::new(DashMap::new());
        let mut recovered_sequence = 0;

        // 1. Recovery Phase: Load Snapshot
        if std::path::Path::new(&snap_path_str).exists() {
            if let Ok(snap_bytes) = std::fs::read(&snap_path_str) {
                if let Ok(snapshot) = bincode::deserialize::<SnapshotPayload>(&snap_bytes) {
                    for (id, state) in snapshot.states {
                        pending_transactions.insert(id, state);
                    }
                    recovered_sequence = snapshot.sequence_id;
                }
            }
        }

        // 2. Recovery Phase: Replay active WAL over snapshot
        if std::path::Path::new(&wal_path_str).exists() {
            if let Ok(wal_bytes) = std::fs::read(&wal_path_str) {
                let mut cursor = 0;
                while cursor < wal_bytes.len() {
                    if cursor + 8 > wal_bytes.len() { break; }
                    let mut len_bytes = [0u8; 8];
                    len_bytes.copy_from_slice(&wal_bytes[cursor..cursor+8]);
                    let len = u64::from_le_bytes(len_bytes) as usize;
                    cursor += 8;

                    if cursor + len > wal_bytes.len() { break; }
                    if let Ok(entry) = bincode::deserialize::<WalEntry>(&wal_bytes[cursor..cursor+len]) {
                        pending_transactions.insert(entry.trade_id, entry.state);
                    }
                    cursor += len;
                }
            }
        }

        let wal_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path_buf)
            .await?;

        // Channel for sending entries to the WAL flusher.
        // Bounded channel provides backpressure if I/O is saturated.
        let (wal_sender, wal_receiver) = mpsc::channel(100_000);

        // Spawn the group commit background task
        tokio::spawn(Self::wal_group_commit_task(
            wal_file, 
            wal_receiver,
            wal_path_str,
            snap_path_str,
            pending_transactions.clone(),
            recovered_sequence,
        ));

        Ok(Self {
            pending_transactions,
            wal_sender,
        })
    }

    /// Background task that buffers WAL entries and flushes them to disk
    /// based on a 64KB size threshold or a 10ms timer, achieving Group Commit.
    /// Also handles safe WAL Compaction Roll.
    async fn wal_group_commit_task(
        wal_file: File,
        mut receiver: mpsc::Receiver<(WalEntry, tokio::sync::oneshot::Sender<Result<(), CoordinatorError>>)>,
        wal_path: String,
        snap_path: String,
        state_map: Arc<DashMap<Uuid, TransactionState>>,
        initial_sequence: u64,
    ) {
        let mut active_file = wal_file;
        let mut writer = BufWriter::with_capacity(65536, active_file); // 64KB buffer
        let mut pending_acks: Vec<tokio::sync::oneshot::Sender<Result<(), CoordinatorError>>> = Vec::new();
        let mut bytes_written_since_flush = 0;
        
        let mut counter = 0;
        let threshold = 1_000_000;
        let mut current_sequence = initial_sequence;

        loop {
            tokio::select! {
                // 10ms latency bound to ensure no transaction lingers un-flushed
                () = sleep(Duration::from_millis(10)), if !pending_acks.is_empty() => {
                    if let Err(e) = writer.flush().await {
                        for ack in pending_acks.drain(..) {
                            let _ = ack.send(Err(CoordinatorError::IoError(e.to_string())));
                        }
                    } else {
                        // After flush, fsync to guarantee hardware durability
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
                            counter += 1;
                            current_sequence += 1;

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

                            // Perform Non-Blocking File Roll if threshold breached
                            if counter >= threshold {
                                // 1. Flush and sync current WAL
                                let _ = writer.flush().await;
                                let _ = writer.get_mut().sync_all().await;
                                
                                // 2. Extract active file handle and rename
                                let old_wal_path = format!("{}.old", wal_path);
                                let _ = rename(&wal_path, &old_wal_path);
                                
                                // 3. Open pristine WAL for ingestion (unblocking path immediately)
                                if let Ok(new_file) = OpenOptions::new().create(true).append(true).open(&wal_path).await {
                                    writer = BufWriter::with_capacity(65536, new_file);
                                }
                                
                                // 4. Shallow copy DashMap state to minimize lock duration
                                let mut snapshot_data = Vec::with_capacity(state_map.len());
                                for entry in state_map.iter() {
                                    snapshot_data.push((*entry.key(), *entry.value()));
                                }
                                
                                let snapshot_payload = SnapshotPayload {
                                    sequence_id: current_sequence,
                                    states: snapshot_data,
                                };
                                
                                let snap_path_clone = snap_path.clone();
                                
                                // 5. Spawn blocking thread for heavy serialization
                                tokio::task::spawn_blocking(move || {
                                    let tmp_path = format!("{}.tmp", snap_path_clone);
                                    if let Ok(mut tmp_file) = std::fs::File::create(&tmp_path) {
                                        if let Ok(serialized) = bincode::serialize(&snapshot_payload) {
                                            let _ = tmp_file.write_all(&serialized);
                                            let _ = tmp_file.sync_all(); // Hardware persistence
                                            let _ = rename(&tmp_path, &snap_path_clone); // Atomic Overwrite
                                            
                                            // Clean up old rolled WAL
                                            let _ = std::fs::remove_file(old_wal_path);
                                        }
                                    }
                                });
                                
                                counter = 0;
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
            .map_or(TransactionState::Pending, |s| *s);

        match (current_state, new_state) {
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
            state: new_state,
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
        self.pending_transactions.get(trade_id).map(|s| *s)
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
