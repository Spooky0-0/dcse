# DCSE Test Suite

This document outlines the rigorous testing strategy employed to mathematically prove the safety, consistency, and durability of the Distributed Clearing & Settlement Engine (DCSE).

## 1. Property-Based Testing (Proptest)

Unlike standard unit tests which check hardcoded scenarios, DCSE employs **Property-Based Testing** using the `proptest` framework. This generates thousands of randomized inputs to continuously test core system invariants.

### `property_test_balance_invariant`
This test verifies the most fundamental law of financial ledgers: **The Law of Conservation of Balances**.

The test generates:
- Random initial balances for Buyer and Seller (100 to 10,000 units).
- Random `trade_amount`s (1 to 20,000 units).

**Execution Flow**:
1. The orchestrator attempts a Two-Phase Commit (`prepare` -> `commit` + `credit`).
2. If `trade_amount` exceeds the buyer's balance, the `prepare` phase intentionally fails and triggers an `abort`.
3. If it succeeds, the transaction is finalized.

**The Invariant Check**:
```rust
let initial_total = initial_buyer_balance + initial_seller_balance;
let final_total = buyer.available + seller.available;
assert_eq!(initial_total, final_total);
```
Regardless of random input vectors, the total capital inside the ledger must remain exactly the same. No funds can be created or destroyed.

## 2. Distributed Chaos Testing

To simulate real-world failure states, the test suite includes orchestrated crash and timeout scenarios.

### `test_chaos_crashed_participant_aborts`
Simulates a critical network failure during the 2PC Prepare Phase.

**Execution Flow**:
1. The Coordinator issues a `start_transaction`.
2. The Buyer successfully completes the `prepare` phase, locking funds in their `reserved` balance.
3. The system intentionally simulates a crash or timeout before the Seller can prepare.
4. The Coordinator explicitly transitions the transaction state to `Aborted`.
5. `ledger.abort(trade_id)` is invoked.

**The Invariant Check**:
- We verify that the Buyer's `reserved` funds are successfully unlocked and returned to their `available` balance, proving that a partial failure in a distributed environment does not result in permanently locked capital.

## 3. Asynchronous Concurrency

Because the DCSE orchestrator (`TransactionCoordinator`) runs on the `tokio` async runtime, all integration tests run using `#[tokio::test]`. This ensures that our test environment replicates the non-blocking I/O characteristics of the production network and file system.
