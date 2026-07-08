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

By grouping hundreds of transactions into a single disk I/O call, the DCSE can theoretically process tens of thousands of durable, crash-consistent settlements per second.
