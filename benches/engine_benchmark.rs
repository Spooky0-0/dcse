use criterion::{black_box, criterion_group, criterion_main, Criterion};
use dcse::{LedgerParticipant, TransactionCoordinator, TransactionState};
use tempfile::tempdir;
use uuid::Uuid;

fn bench_ledger_throughput(c: &mut Criterion) {
    let ledger = LedgerParticipant::new();
    let client_id = Uuid::new_v4();
    ledger.create_account(client_id, 100_000_000);

    c.bench_function("ledger_prepare_throughput", |b| {
        b.iter(|| {
            let trade_id = Uuid::new_v4();
            // We ignore errors here for raw throughput speed
            let _ = ledger.prepare(black_box(trade_id), black_box(client_id), black_box(10));
        })
    });
}

fn bench_wal_group_commit_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("bench_wal.bin");
    
    let coordinator = rt.block_on(async {
        TransactionCoordinator::new(&wal_path).await.unwrap()
    });

    c.bench_function("wal_group_commit_latency", |b| {
        b.to_async(&rt).iter(|| async {
            let trade_id = Uuid::new_v4();
            let _ = coordinator.start_transaction(black_box(trade_id));
            let _ = coordinator.transition_state(black_box(trade_id), TransactionState::Prepared).await;
        });
    });
}

criterion_group!(benches, bench_ledger_throughput, bench_wal_group_commit_latency);
criterion_main!(benches);
