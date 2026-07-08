# Errors Found & Fixed

This document logs critical bugs, race conditions, or logic errors discovered during the QA and Quality Gates phase, highlighting the problem and the Senior-tier architectural solution.

## 1. The Interleaved Idempotency Collision
**Found in**: Property-Based Integration Tests (`property_test_balance_invariant`).

### The Symptom
The property-based test simulated random trade amounts between a buyer and a seller. The mathematical invariant checked that the total capital on the ledger remained absolutely constant. The test failed, reporting that exactly the `trade_amount` of capital had vanished from the system.

### The Root Cause
A single `LedgerParticipant` was acting as the node for both the Buyer and the Seller. 
1. The orchestrator called `ledger.commit(trade_id)` to deduct funds from the buyer. To ensure fault tolerance, the `commit` method correctly logged the `trade_id` into a `DashSet` called `processed_trades` to prevent duplicate processing.
2. The orchestrator then called `ledger.credit(trade_id, seller_id, ...)` to deliver the funds to the seller. 
3. Because `credit` also checked `processed_trades` for idempotency, it saw the `trade_id` was already present (placed there milliseconds ago by the `commit` call!). 
4. The `credit` method returned `Ok(())` under the false assumption it was a duplicate retry packet. The funds were successfully deducted from the buyer, but the seller's account was silently never credited.

### The Fix
Using a single namespace for both sides of a transaction was fundamentally flawed. The fix separated the cache into two distinct domains:
- `processed_commits: DashSet<Uuid>` tracks debited transactions.
- `processed_credits: DashSet<Uuid>` tracks credited transactions.

This allows the ledger to safely execute both the debit and credit legs of the exact same `trade_id` asynchronously, ensuring true mathematical invariants are maintained during Two-Phase Commit completion.
