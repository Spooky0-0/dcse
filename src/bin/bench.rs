#![allow(clippy::all, clippy::pedantic)]
use std::time::Instant;
use uuid::Uuid;
use dcse::LedgerParticipant;

fn main() {
    println!("==================================================");
    println!("  DCSE LedgerParticipant Micro-Benchmarks");
    println!("==================================================");

    let num_iterations = 1_000_000;
    
    // Create ledger participant
    let ledger = LedgerParticipant::new();
    
    // Pre-create accounts
    let buyer_id = Uuid::new_v4();
    let seller_id = Uuid::new_v4();
    
    ledger.create_account(buyer_id, 10_000_000_000);
    ledger.create_account(seller_id, 10_000_000_000);
    
    // Pre-generate trade IDs to avoid UUID generation overhead in measurement
    let mut trade_ids = Vec::with_capacity(num_iterations);
    for _ in 0..num_iterations {
        trade_ids.push(Uuid::new_v4());
    }

    println!("Benchmarking {} iterations of LedgerParticipant state transitions...", num_iterations);

    // 1. Benchmark prepare
    let start = Instant::now();
    for i in 0..num_iterations {
        let _ = ledger.prepare(trade_ids[i], buyer_id, 1);
    }
    let duration_prepare = start.elapsed();
    let prepare_tps = num_iterations as f64 / duration_prepare.as_secs_f64();
    let prepare_latency = duration_prepare.as_nanos() as f64 / num_iterations as f64;

    // 2. Benchmark commit
    let start = Instant::now();
    for i in 0..num_iterations {
        let _ = ledger.commit(trade_ids[i]);
    }
    let duration_commit = start.elapsed();
    let commit_tps = num_iterations as f64 / duration_commit.as_secs_f64();
    let commit_latency = duration_commit.as_nanos() as f64 / num_iterations as f64;

    // 3. Benchmark credit
    let mut credit_trade_ids = Vec::with_capacity(num_iterations);
    for _ in 0..num_iterations {
        credit_trade_ids.push(Uuid::new_v4());
    }
    
    let start = Instant::now();
    for i in 0..num_iterations {
        let _ = ledger.credit(credit_trade_ids[i], seller_id, 1);
    }
    let duration_credit = start.elapsed();
    let credit_tps = num_iterations as f64 / duration_credit.as_secs_f64();
    let credit_latency = duration_credit.as_nanos() as f64 / num_iterations as f64;

    // 4. Benchmark abort
    // Let's generate another batch, prepare them, then abort them
    let mut abort_trade_ids = Vec::with_capacity(num_iterations);
    for _ in 0..num_iterations {
        abort_trade_ids.push(Uuid::new_v4());
    }
    for i in 0..num_iterations {
        let _ = ledger.prepare(abort_trade_ids[i], buyer_id, 1);
    }
    
    let start = Instant::now();
    for i in 0..num_iterations {
        let _ = ledger.abort(abort_trade_ids[i]);
    }
    let duration_abort = start.elapsed();
    let abort_tps = num_iterations as f64 / duration_abort.as_secs_f64();
    let abort_latency = duration_abort.as_nanos() as f64 / num_iterations as f64;

    println!("\nResults:");
    println!("--------------------------------------------------");
    println!("Operation  | Throughput (TPS)   | Latency (ns/op)");
    println!("--------------------------------------------------");
    println!("Prepare    | {:<18.2} | {:.2}", prepare_tps, prepare_latency);
    println!("Commit     | {:<18.2} | {:.2}", commit_tps, commit_latency);
    println!("Credit     | {:<18.2} | {:.2}", credit_tps, credit_latency);
    println!("Abort      | {:<18.2} | {:.2}", abort_tps, abort_latency);
    println!("--------------------------------------------------");
}
