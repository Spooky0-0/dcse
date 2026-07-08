# DCSE Performance & Benchmarks

To quantify the scalability of the Distributed Clearing & Settlement Engine, we utilize the rigorous `criterion` statistical benchmarking framework. 

*Note: Benchmarks are calibrated for Linux environments. Running benchmarks locally on Windows without MSVC C++ Build Tools installed may result in linker errors.*

## Target Benchmarks

The benchmarking suite (`benches/engine_benchmark.rs`) evaluates two critical paths in the DCSE:

### 1. Ledger Throughput (`ledger_prepare_throughput`)
**Objective**: Measure the maximum number of Two-Phase Commit `prepare` operations the in-memory ledger can handle per second.
**Architecture Context**: 
Originally, ledgers used standard `HashMap` structures wrapped in a global `RwLock`. This meant that processing 10,000 independent trades required 10,000 sequential lock acquisitions, creating a severe bottleneck. 
By upgrading to **`DashMap`**, we utilize shard-level concurrent locking. This benchmark proves the lack of lock-contention, allowing the CPU to parallelize state transitions infinitely across different `client_id`s.

### 2. WAL Group Commit Latency (`wal_group_commit_latency`)
**Objective**: Measure the average delay added to a transaction by the durability layer.
**Architecture Context**: 
Calling `fsync` to flush a single transaction to an SSD takes roughly 1-2 milliseconds. A naive loop would cap maximum global throughput to ~500 Transactions Per Second (TPS).
The `TransactionCoordinator` instead utilizes an asynchronous `BufferedWAL`. It queues transactions and flushes them to disk together in one batch (Group Commit) via a `tokio::spawn` task when a 64KB buffer fills up, or when a 10ms micro-timer fires.
This benchmark quantifies the dramatic TPS boost achieved by eliminating blocking I/O from the main orchestrator event loop.

## Evaluating Results
A production-grade settlement engine running on modern multi-core hardware should aim for:
- **Ledger Transitions**: > 1,000,000 ops/sec.
- **WAL Throughput**: > 50,000 transactions/sec (bottlenecked by SSD sequential write speeds, but drastically optimized via the 64KB group commit).
