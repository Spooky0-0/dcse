use dcse::*;
use proptest::prelude::*;
use tempfile::tempdir;
use uuid::Uuid;

#[tokio::test]
async fn test_chaos_crashed_participant_aborts() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.bin");
    let coordinator = TransactionCoordinator::new(&wal_path).await.unwrap();

    let ledger = LedgerParticipant::new();
    let buyer_id = Uuid::new_v4();
    let seller_id = Uuid::new_v4();

    // Initial balances
    ledger.create_account(buyer_id, 1000);
    ledger.create_account(seller_id, 500);

    let trade_id = Uuid::new_v4();
    let amount = 200;

    // Start transaction
    coordinator.start_transaction(trade_id).unwrap();

    // Prepare Phase (Simulate only buyer prepares, then system "crashes" before seller prepares)
    let prepare_result = ledger.prepare(trade_id, buyer_id, amount);
    assert!(prepare_result.is_ok());

    coordinator.transition_state(trade_id, TransactionState::Prepared).await.unwrap();

    // Simulate timeout / crash on seller side. We explicitly trigger an abort.
    // In a real 2PC, coordinator timeout timer triggers this.
    coordinator.transition_state(trade_id, TransactionState::Aborted).await.unwrap();
    ledger.abort(trade_id).unwrap();

    // Verify balances are restored to original state (no funds lost)
    assert_eq!(ledger.get_balance(&buyer_id).unwrap().available, 1000);
    assert_eq!(ledger.get_balance(&buyer_id).unwrap().reserved, 0);

    // Verify state is aborted in coordinator
    assert_eq!(
        coordinator.get_state(&trade_id).unwrap(),
        TransactionState::Aborted
    );
}

proptest! {
    #[test]
    fn property_test_balance_invariant(
        initial_buyer_balance in 100..10_000u64,
        initial_seller_balance in 100..10_000u64,
        trade_amount in 1..20_000u64
    ) {
        // Since proptest does not natively support async functions elegantly without block_on
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ledger = LedgerParticipant::new();
            let buyer_id = Uuid::new_v4();
            let seller_id = Uuid::new_v4();

            ledger.create_account(buyer_id, initial_buyer_balance);
            ledger.create_account(seller_id, initial_seller_balance);

            let initial_total = initial_buyer_balance + initial_seller_balance;
            let trade_id = Uuid::new_v4();

            // Simulate 2PC flow
            let prepare_ok = ledger.prepare(trade_id, buyer_id, trade_amount).is_ok();

            if prepare_ok {
                // Commit flow
                ledger.commit(trade_id).unwrap();
                ledger.credit(trade_id, seller_id, trade_amount).unwrap();
            } else {
                // Abort flow (insufficient funds)
                ledger.abort(trade_id).unwrap();
            }

            // The sum of balances must remain constant
            let final_total = ledger.get_balance(&buyer_id).unwrap().available
                + ledger.get_balance(&seller_id).unwrap().available;

            assert_eq!(initial_total, final_total);
        });
    }
}
