# DCSE Architectural Analysis

This document provides an engineering breakdown of the Distributed Clearing & Settlement Engine (DCSE), explaining *how* it enforces Strict Serializability without collapsing under the weight of blocking locks.

## The Problem: Eventual Consistency vs Financial Integrity
In a distributed microservice architecture, Eventual Consistency is the industry standard (e.g. Amazon DynamoDB). Data is updated on Node A and eventually propagates to Node B. 

In financial settlement, Eventual Consistency is a catastrophic failure condition. If a user spends $1,000 to buy stock, that $1,000 must be debited from their account at the **exact same logical moment** the stock is credited to them. If the network crashes between these two steps, the system cannot be allowed to end up in a state where the money is gone but the stock was never delivered, or vice-versa.

## The Solution: Two-Phase Commit (2PC)
The DCSE guarantees **Atomicity** across distributed ledger participants using the 2PC protocol.

### Phase 1: Prepare (The Lock)
When a trade is initiated, the `TransactionCoordinator` instructs the Buyer's ledger participant to "Prepare". 
- The ledger checks if `account.available` >= `amount`.
- If true, it subtracts the funds from `available` and adds them to `reserved`. 
- **Critical Factor**: The funds are frozen. They have not left the account, but they cannot be spent elsewhere.

### Phase 2: Commit (The Burn)
Only if **all** participants successfully reply to the "Prepare" phase does the Coordinator transition the trade to `Committed`. 
- The Coordinator writes `Committed` to the Write-Ahead Log (WAL) on disk. This is the "Point of No Return".
- The ledger participants are then instructed to deduct the `reserved` balance permanently and add the funds to the Seller's `available` balance.

### Phase 2 Alternative: Abort (The Rollback)
If the buyer lacks funds, or if a network partition occurs causing a timeout during the Prepare phase, the Coordinator transitions the trade to `Aborted`.
- All ledgers are instructed to release `reserved` funds back to `available`. 

## Bypassing Amdahl's Law (Concurrency)
A standard implementation of a ledger utilizes a global Mutex over a HashMap of accounts. By Amdahl's Law, this serial execution bottleneck places a hard ceiling on system throughput.

DCSE solves this using `DashMap`. `DashMap` divides the account storage into shards, locking only the specific shard containing the `client_id` being modified. This enables the ledger to process thousands of independent settlements in parallel on modern multi-core processors. 

## Bypassing Disk Latency (Group Commit WAL)
If the Coordinator called an OS-level `fsync` to physical disk for every single state transition, the maximum global throughput would be roughly ~500 transactions per second (limited by SSD sequential write latency). 

To solve this, the `TransactionCoordinator` utilizes an asynchronous **BufferedWAL**. 
State transitions are sent down an `mpsc` queue to a dedicated background `tokio` thread. This thread buffers the incoming logs into memory. It only issues a blocking physical disk write when:
1. The memory buffer hits **64KB**. 
2. A **10ms latency timer** fires (guaranteeing that in periods of low volume, transactions don't hang unconfirmed in memory).

## Tier-1 Production Hardening

While the 2PC engine solves the ledger invariant problem, a production environment requires three additional architectural pillars to guarantee total fault tolerance.

### 1. Recovery Time Objective (RTO): Non-Blocking Active File Rolling
If a system writes to a WAL indefinitely, an application crash could require hours to replay millions of transactions sequentially from disk. This violates enterprise RTO thresholds.

To cap RTO below 5 seconds, the DCSE implements **Active File Rolling and Snapshotting**:
- **The Threshold**: Once 1,000,000 transactions are processed, the compaction loop triggers.
- **Copy-On-Write**: Instead of halting the engine to serialize the live `DashMap`, the coordinator performs a shallow memory copy of the state layout.
- **Active File Rolling**: The engine performs an atomic filesystem swap, renaming `wal.bin` to `wal.bin.old` and instantly opening a pristine `wal.bin`. This immediately unblocks the hot execution path.
- **Background Serialization**: A detached I/O thread takes the shallow copy, serializes it via `bincode`, flushes it to disk (`sync_all()`), and deletes the old WAL.
- **Result**: The compaction stutter is reduced to $\mathcal{O}(1)$ relative to the active thread, ensuring no tail-latency spikes.

### 2. Coordinator High Availability: Raft Consensus Integration
The flaw in standard 2PC is that the Coordinator is a single point of failure (SPOF). If the Coordinator crashes mid-transaction, ledgers may hold locked funds indefinitely.

To solve this, the DCSE Coordinator state can be distributed using **Raft Consensus**:
- The Coordinator operates as the Raft Leader.
- When generating a `WalEntry`, instead of immediately issuing a local `fsync`, the Leader broadcasts the entry to a cluster of Raft Follower nodes.
- Only once a **Quorum** of followers (e.g., 2 out of 3) append the entry to their own local WALs does the Leader consider the transaction committed.
- If the Leader dies, the Followers hold an election. The node with the most up-to-date WAL becomes the new Leader, inheriting the un-aborted pending transactions seamlessly.

### 3. Network Partitions: Split-Brain Protections (CP over AP)
According to the CAP Theorem, a distributed system can only provide two of three guarantees: Consistency, Availability, and Partition Tolerance.

The DCSE explicitly chooses **Consistency and Partition Tolerance (CP)**. 
- **The Split-Brain Scenario**: If a network partition occurs and the Coordinator cannot reach Ledger B, the system does not try to guess Ledger B's state. 
- **Failing Closed**: The Coordinator's `prepare` network call will encounter a timeout. Because the 2PC protocol requires unanimous consent, the Coordinator will forcibly transition the trade to `Aborted` and instruct Ledger A to unlock funds.
- **Result**: Availability drops to zero, but the financial invariant is perfectly preserved. The system will never credit assets on Ledger A if Ledger B is unreachable.
