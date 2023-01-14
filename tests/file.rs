extern crate ruspledb;

use crate::ruspledb::file::block_id::BlockId;
use crate::ruspledb::file::file_mgr::FileMgr;
use crate::ruspledb::file::page::Page;

#[test]
fn integration_file() {
    let path = "./db/filetest";
    let block_size = 400;
    let mut fm = FileMgr::new(path.to_string(), block_size);
    let mut blk = BlockId::new("testfile".to_string(), 2);

    let pos1 = 88;
    let mut p1 = Page::new(fm.block_size());
    p1.set_string(pos1, "abcdefghijklm".to_string());

    let size = p1.max_length("abcdefghijklm".len());
    let pos2 = pos1 + size;
    p1.set_i32(pos2, 345);

    fm.write(&mut blk, p1);

    let mut p2 = Page::new(fm.block_size());

    fm.read(blk, &mut p2);

    assert_eq!(p2.get_string(pos1), "abcdefghijklm");
    assert_eq!(p2.get_i32(pos2), 345);
}
