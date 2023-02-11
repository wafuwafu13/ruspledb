extern crate ruspledb;

use crate::ruspledb::file::file_mgr::FileMgr;
use crate::ruspledb::logging::log_mgr::LogMgr;
use ruspledb::buffer::buffer_mgr::BufferMgr;
use ruspledb::file::block_id::BlockId;
use ruspledb::tx::transaction::Transaction;
use std::fs;
use std::path::Path;
use std::thread;
use std::time::Duration;

#[test]
fn integration_concurrency() {
    let db_dir = "./db/concurrencytest";
    if Path::new(db_dir).exists() {
        fs::remove_dir_all(db_dir).unwrap();
    }
    let block_size = 400;
    let mut fm = FileMgr::new(db_dir.to_string(), block_size);
    let log_file = "ruspledb.log";
    let mut lm = LogMgr::new(&mut fm, &mut log_file.to_string());
    let buffer_size = 8;
    let mut bm = BufferMgr::new(&mut fm, &mut lm, buffer_size);
    let handle = thread::spawn(move || {
        run_a(&mut fm, &mut lm, &mut bm);
        run_b(&mut fm, &mut lm, &mut bm);
        run_c(&mut fm, &mut lm, &mut bm);
    });
    handle.join().unwrap();
}

fn run_a(fm: &mut FileMgr, lm: &mut LogMgr, bm: &mut BufferMgr) {
    let mut tx_a = Transaction::new(fm, lm, bm);
    let mut blk1 = BlockId::new("testfile".to_string(), 1);
    let mut blk2 = BlockId::new("testfile".to_string(), 2);
    tx_a.pin(&mut blk1);
    tx_a.pin(&mut blk2);
    println!("Tx A: request slock 1");
    tx_a.get_int(&mut blk1, 0);
    println!("Tx A: receive slock 1");
    thread::sleep(Duration::from_secs(10));
    println!("Tx A: request slock 2");
    tx_a.get_int(&mut blk2, 0);
    println!("Tx A: receive slock 2");
    tx_a.commit();
    println!("Tx A: commit");
}

fn run_b(fm: &mut FileMgr, lm: &mut LogMgr, bm: &mut BufferMgr) {
    let mut tx_b = Transaction::new(fm, lm, bm);
    let mut blk1 = BlockId::new("testfile".to_string(), 1);
    let mut blk2 = BlockId::new("testfile".to_string(), 2);
    tx_b.pin(&mut blk1);
    tx_b.pin(&mut blk2);
    println!("Tx B: request xlock 2");
    tx_b.set_int(&mut blk2, 0, 0, false);
    println!("Tx B: receive xlock 2");
    thread::sleep(Duration::from_secs(10));
    println!("Tx B: request slock 1");
    tx_b.get_int(&mut blk1, 0);
    println!("Tx B: receive slock 1");
    tx_b.commit();
    println!("Tx B: commit");
}

fn run_c(fm: &mut FileMgr, lm: &mut LogMgr, bm: &mut BufferMgr) {
    let mut tx_c = Transaction::new(fm, lm, bm);
    let mut blk1 = BlockId::new("testfile".to_string(), 1);
    let mut blk2 = BlockId::new("testfile".to_string(), 2);
    tx_c.pin(&mut blk1);
    tx_c.pin(&mut blk2);
    thread::sleep(Duration::from_secs(5));
    println!("Tx C: request xlock 1");
    tx_c.set_int(&mut blk1, 0, 0, false);
    println!("Tx C: receive xlock 1");
    thread::sleep(Duration::from_secs(10));
    println!("Tx C: request slock 2");
    tx_c.get_int(&mut blk2, 0);
    println!("Tx C: receive slock 2");
    tx_c.commit();
    println!("Tx C: commit");
}
