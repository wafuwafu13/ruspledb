extern crate ruspledb;

use crate::ruspledb::file::file_mgr::FileMgr;
use crate::ruspledb::logging::log_mgr::LogMgr;
use ruspledb::buffer::buffer_mgr::BufferMgr;
use ruspledb::file::block_id::BlockId;
use ruspledb::tx::transaction::Transaction;
use std::fs;
use std::path::Path;

#[test]
fn integration_tx() {
    let db_dir = "./db/txtest";
    if Path::new(db_dir).exists() {
        fs::remove_dir_all(db_dir).unwrap();
    }
    let block_size = 400;
    let mut fm = FileMgr::new(db_dir.to_string(), block_size);
    let log_file = "ruspledb.log";
    let mut lm = LogMgr::new(&mut fm, &mut log_file.to_string());
    let buffer_size = 8;
    let mut bm = BufferMgr::new(&mut fm, &mut lm, buffer_size);

    let mut tx1 = Transaction::new(&mut fm, &mut lm, &mut bm);
    let mut blk = BlockId::new("testfile".to_string(), 1);
    tx1.pin(&mut blk);
    // The block initially contains unknown bytes,
    // so don't log those values here.
    tx1.set_int(&mut blk, 80, 1, false);
    tx1.set_string(&mut blk, 40, "one".to_string(), false);
    tx1.commit();

    let mut tx2 = Transaction::new(&mut fm, &mut lm, &mut bm);
    tx2.pin(&mut blk);
    let mut i_val = tx2.get_int(&mut blk, 80);
    let mut s_val = tx2.get_string(&mut blk, 40);
    // initial value at location 80
    assert_eq!(i_val, 1);
    // initial value at location 40
    assert_eq!(s_val, "one");

    let new_i_val = i_val + 1;
    let new_s_val = s_val + "!";
    tx2.set_int(&mut blk, 80, new_i_val, true);
    tx2.set_string(&mut blk, 40, new_s_val, true);
    tx2.commit();

    let mut tx3 = Transaction::new(&mut fm, &mut lm, &mut bm);
    tx3.pin(&mut blk);

    i_val = tx3.get_int(&mut blk, 80);
    s_val = tx3.get_string(&mut blk, 40);
    // new value at location 80
    assert_eq!(i_val, 2);
    // new value at location 40
    assert_eq!(s_val, "one!");

    tx3.set_int(&mut blk, 80, 9999, true);
    i_val = tx3.get_int(&mut blk, 80);
    // pre-rollback value at location 80
    assert_eq!(i_val, 9999);

    // tx3.commit();
    // TODO: do rollback correctly (when tx3 is commmited, test fails)
    tx3.rollback();

    let mut tx4 = Transaction::new(&mut fm, &mut lm, &mut bm);
    tx4.pin(&mut blk);

    i_val = tx4.get_int(&mut blk, 80);
    assert_eq!(i_val, 2);
    tx4.commit();
}
