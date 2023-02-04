extern crate ruspledb;

use crate::ruspledb::file::file_mgr::FileMgr;
use crate::ruspledb::logging::log_mgr::LogMgr;
use ruspledb::buffer::buffer::Buffer;
use ruspledb::buffer::buffer_mgr::BufferMgr;
use ruspledb::file::block_id::BlockId;
use std::fs;
use std::path::Path;

#[test]
fn integration_buffer_mgr() {
    let db_dir = "./db/buffermgrtest";
    if Path::new(db_dir).exists() {
        fs::remove_dir_all(db_dir).unwrap();
    }
    let block_size = 400;
    let mut fm = FileMgr::new(db_dir.to_string(), block_size);
    let log_file = "ruspledb.log";
    let mut lm = LogMgr::new(&mut fm, &mut log_file.to_string());
    let buffer_size = 3;
    let mut bm = BufferMgr::new(&mut fm, &mut lm, buffer_size);

    let mut buffer: Vec<Buffer> = Vec::with_capacity(6);
    let mut b0 = BlockId::new("testfile".to_string(), 0);
    let mut b1 = BlockId::new("testfile".to_string(), 1);
    let mut b2 = BlockId::new("testfile".to_string(), 2);
    let mut b3 = BlockId::new("testfile".to_string(), 3);
    buffer.push(bm.pin(&mut b0).unwrap());
    buffer.push(bm.pin(&mut b1).unwrap());
    buffer.push(bm.pin(&mut b2).unwrap());
    assert_eq!(bm.available(), 0);
    bm.unpin(&mut buffer[1]);
    assert_eq!(bm.available(), 1);
    // block 0 pinned twice
    buffer.push(bm.pin(&mut b0).unwrap());
    assert_eq!(bm.available(), 1);
    // block 1 repinned
    buffer.push(bm.pin(&mut b1).unwrap());
    assert_eq!(bm.available(), 0);

    println!("Attempting to pin block 3...");
    // will not work; no buffers left
    let res = bm.pin(&mut b3);
    assert_eq!(res, Err("BufferAbortException".to_string()));

    bm.unpin(&mut buffer[2]);
    // now this works
    buffer.push(bm.pin(&mut b3).unwrap());

    assert_eq!(buffer[0].block().unwrap().blk_num(), 0);
    assert_eq!(buffer[3].block().unwrap().blk_num(), 0);
    assert_eq!(buffer[4].block().unwrap().blk_num(), 1);
    assert_eq!(buffer[5].block().unwrap().blk_num(), 3);
}
