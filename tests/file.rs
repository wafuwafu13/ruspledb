extern crate ruspledb;

use crate::ruspledb::file::block_id::BlockId;
use crate::ruspledb::file::file_mgr::FileMgr;
use crate::ruspledb::file::page::Page;
use std::fs;
use std::path::Path;

#[test]
fn integration_file() {
    let db_dir = "./db/filetest";
    if Path::new(db_dir).exists() {
        fs::remove_dir_all(db_dir).unwrap();
    }
    let block_size = 400;
    let mut fm = FileMgr::new(db_dir.to_string(), block_size);
    let mut blk = BlockId::new("testfile".to_string(), 2);

    let pos1 = 88;
    let mut p1 = Page::new(fm.block_size());
    p1.set_string(pos1, "abcdefghijklm".to_string());

    let size = p1.max_length("abcdefghijklm".len());
    let pos2 = pos1 + size;
    p1.set_u64(pos2, 345);

    fm.write(&mut blk, &mut p1);

    let mut p2 = Page::new(fm.block_size());

    fm.read(&mut blk, &mut p2);

    assert_eq!(p2.get_string(pos1), "abcdefghijklm");
    assert_eq!(p2.get_u64(pos2), 345);
}
