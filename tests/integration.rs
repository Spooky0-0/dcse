use dcse::*;
use proptest::prelude::*;
use tempfile::tempdir;
use uuid::Uuid;

proptest! {
    #[test]
    fn test_distributed_chaos_abort(
        inject_timeout in any::<bool>(),
        buyer_balance in 1000..5000u64,
        seller_balance in 1000..5000u64,
        trade_amount in 100..900u64
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = tempdir().unwrap();
            let wal_path = dir.path().join("chaos_wal.bin");
            let coordinator = TransactionCoordinator::new(&wal_path).await.unwrap();

            // Simulate two separate isolated ledgers
            let buyer_ledger = LedgerParticipant::new();
            let seller_ledger = LedgerParticipant::new();

            let buyer_id = Uuid::new_v4();
            let seller_id = Uuid::new_v4();
            let trade_id = Uuid::new_v4();

            buyer_ledger.create_account(buyer_id, buyer_balance);
            seller_ledger.create_account(seller_id, seller_balance);

            coordinator.start_transaction(trade_id).await.unwrap();

            // Phase 1: Prepare
            let mut prepare_success = true;

            if buyer_ledger.prepare(trade_id, buyer_id, trade_amount).is_err() {
                prepare_success = false;
            }

            // Simulating the "chaos" midway through prepare
            if inject_timeout {
                prepare_success = false;
            } else {
                if seller_ledger.get_balance(&seller_id).is_none() {
                    prepare_success = false;
                }
            }

            // Coordinator Decision
            if prepare_success {
                coordinator.transition_state(trade_id, TransactionState::Prepared).await.unwrap();
                coordinator.transition_state(trade_id, TransactionState::Committed).await.unwrap();
                
                buyer_ledger.commit(trade_id).unwrap();
                seller_ledger.credit(trade_id, seller_id, trade_amount).unwrap();
                
                assert_eq!(buyer_ledger.get_balance(&buyer_id).unwrap().available, buyer_balance - trade_amount);
                assert_eq!(seller_ledger.get_balance(&seller_id).unwrap().available, seller_balance + trade_amount);
            } else {
                coordinator.transition_state(trade_id, TransactionState::Aborted).await.unwrap();
                
                // Broadcast abort to both ledgers
                buyer_ledger.abort(trade_id).unwrap();
                seller_ledger.abort(trade_id).unwrap();

                // Verify Bulletproof State - no funds lost or locked
                assert_eq!(buyer_ledger.get_balance(&buyer_id).unwrap().available, buyer_balance);
                assert_eq!(buyer_ledger.get_balance(&buyer_id).unwrap().reserved, 0);
                
                assert_eq!(seller_ledger.get_balance(&seller_id).unwrap().available, seller_balance);
                assert_eq!(seller_ledger.get_balance(&seller_id).unwrap().reserved, 0);
            }
        });
    }
}
