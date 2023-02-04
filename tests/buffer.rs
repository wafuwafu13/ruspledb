extern crate ruspledb;

use crate::ruspledb::file::file_mgr::FileMgr;
use crate::ruspledb::logging::log_mgr::LogMgr;
use ruspledb::buffer::buffer_mgr::BufferMgr;
use ruspledb::file::block_id::BlockId;

#[test]
fn integration_buffer() {
    let db_dir = "./db/buffertest";
    let block_size = 400;
    let mut fm = FileMgr::new(db_dir.to_string(), block_size);
    let log_file = "ruspledb.log";
    let mut lm = LogMgr::new(&mut fm, &mut log_file.to_string());
    let buffer_size = 3;
    let mut bm = BufferMgr::new(&mut fm, &mut lm, buffer_size);

    let mut b1 = bm
        .pin(&mut BlockId::new("testfile".to_string(), 1))
        .unwrap();
    let mut p1 = b1.contents();
    let n = p1.get_u64(80);
    p1.set_u64(80, n + 1);
    // need to set contents directly
    b1.set_contents(p1);
    // enable to flush
    b1.set_modified(1, 0);
    // increasing by 1
    println!("The new value is {}", n + 1);
    // b1 will be chosen as unpinned buffer
    bm.unpin(&mut b1);
    // this pin will flush b1 to disk
    let mut b2 = bm
        .pin(&mut BlockId::new("testfile".to_string(), 2))
        .unwrap();
    let mut p2 = b2.contents();
    // will not get written to disk
    p2.set_u64(80, 9999);
    b2.set_modified(1, 0);
}
