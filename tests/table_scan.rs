extern crate ruspledb;

use crate::ruspledb::file::file_mgr::FileMgr;
use crate::ruspledb::logging::log_mgr::LogMgr;
use rand::Rng;
use ruspledb::buffer::buffer_mgr::BufferMgr;
use ruspledb::record::layout::Layout;
use ruspledb::record::schema::Schema;
use ruspledb::record::table_scan::TableScan;
use ruspledb::tx::transaction::Transaction;
use std::fs;
use std::path::Path;

#[test]
fn integration_record() {
    let db_dir = "./db/recordtest";
    if Path::new(db_dir).exists() {
        fs::remove_dir_all(db_dir).unwrap();
    }
    let block_size = 400;
    let mut fm = FileMgr::new(db_dir.to_string(), block_size);
    let log_file = "ruspledb.log";
    let mut lm = LogMgr::new(&mut fm, &mut log_file.to_string());
    let buffer_size = 8;
    let mut bm = BufferMgr::new(&mut fm, &mut lm, buffer_size);
    let mut tx = Transaction::new(&mut fm, &mut lm, &mut bm);

    let mut schema = Schema::new();
    schema.add_int_field("A");
    schema.add_string_field("B", 9);
    let mut layout = Layout::new(&mut schema);
    for field_name in layout.schema().fields().iter_mut() {
        let offset = layout.offset(field_name);
        println!("{field_name} has offset {offset}");
    }

    println!("Filling the page with random records.");
    let mut ts = TableScan::new(&mut tx, "T", layout);
    for _ in 0..=50 {
        ts.insert();
        let mut rng = rand::thread_rng();
        let n = rng.gen_range(1..50);
        ts.set_int("A", n);
        ts.set_string("B", "rec".to_string() + &n.to_string());
        println!("inserting into slot {:?}: ({n}, rec{n})", ts.get_rid())
    }

    println!("Deleting these records, whose A values are less than 25.");
    let mut count = 0;
    ts.before_first();
    // TODO: always false
    while ts.next() {
        // TODO: a become 0
        let a = ts.get_int_u32("A");
        let b = ts.get_string("B");
        if a < 25 {
            count += 1;
            println!("slot {:?}: ({a}, {b})", ts.get_rid());
            ts.delete();
        }
    }
    println!("{count} values under 25 were deleted.");

    println!("Here are the remaining records.");
    ts.before_first();
    // TODO: always false
    while ts.next() {
        // TODO: a become 0
        let a = ts.get_int_u32("A");
        let b = ts.get_string("B");
        println!("slot {:?}: ({a}, {b})", ts.get_rid());
    }
    ts.close();
    tx.commit();
}
