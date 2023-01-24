extern crate ruspledb;

use crate::ruspledb::file::file_mgr::FileMgr;
use crate::ruspledb::file::page::Page;
use crate::ruspledb::logging::log_mgr::LogMgr;
use bytebuffer::ByteBuffer;
use std::fs;
use std::path::Path;

fn print_log_records(lm: &mut LogMgr, msg: String) {
    println!("{}", msg);
    let mut iter = lm.iterator();
    while iter.has_next() {
        let rec = iter.next();
        let mut page = Page::new_from_buffer(&mut ByteBuffer::from_vec(rec));
        let s = page.get_string(0);
        let npos = page.max_length(s.len());
        let val = page.get_u32(npos);
        println!("[ {s} , {val} ]");
    }
    println!()
}

fn create_log_records(lm: &mut LogMgr, start: u64, end: u64) {
    println!("creating records:");
    for i in start..=end {
        let s = "record".to_string() + &i.to_string();
        let n = i + 100;
        let spos = 0;
        let npos = spos + max_length(s.len());
        let mut buffer = ByteBuffer::new();
        buffer.resize(npos + 4);
        let mut page = Page::new_from_buffer(&mut buffer);
        page.set_string(spos, s);
        page.set_u32(npos, n.try_into().unwrap());
        // have to `page.buffer` not `buffer`
        let lsn = lm.append(page.buffer.into_bytes());
        println!("lsn: {lsn}")
    }
    println!()
}

fn max_length(str_len: usize) -> usize {
    4 + str_len
}

// The code creates 70 log records, each consisting of a string and an integer.
// The code prints the records once after the first 35 have been created and then again after all 70 have been created.
// If you run the code, you will discover that only 20 records are printed after the first call to printLogRecords.
// The reason is those records filled the first log block and were flushed to disk when the 21st log record was crated.
// The other 15 log records remained in the in-memory log page and were not flushed.
// The second call to crateRecords creates records 36 through 70.
// The call to flush tells the log manager to ensure that record 65 is on disk.
// But since redords 66-70 are in the same page as record 65, they are also witten to disk.
// Consequently, the second call to printLogRecords will print all 70 records, in reverse order.
#[test]
fn integration_log() {
    let db_dir = "./db/logtest";
    if Path::new(db_dir).exists() {
        fs::remove_dir_all(db_dir).unwrap();
    }
    let block_size = 400;
    let fm = FileMgr::new(db_dir.to_string(), block_size);
    let log_file = "ruspledb.log";
    let mut lm = LogMgr::new(fm, &mut log_file.to_string());
    print_log_records(&mut lm, "The initial empty log file:".to_string());
    println!("done");
    create_log_records(&mut lm, 1, 35);
    println!("done");
    print_log_records(&mut lm, "The log file now has these records:".to_string());
    println!("done");
    create_log_records(&mut lm, 36, 70);
    println!("done");
    lm.flush_with_lsn(65);
    assert_eq!(lm.get_last_saved_lsn(), 70);
    print_log_records(&mut lm, "The log file now has these records:".to_string());
}
