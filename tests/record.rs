extern crate ruspledb;

use crate::ruspledb::file::file_mgr::FileMgr;
use crate::ruspledb::logging::log_mgr::LogMgr;
use rand::Rng;
use ruspledb::buffer::buffer_mgr::BufferMgr;
use ruspledb::record::layout::Layout;
use ruspledb::record::record_page::RecordPage;
use ruspledb::record::schema::Schema;
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
        println!("{} has offset {}", field_name, offset);
    }

    let mut blk = tx.append("testfile");
    tx.pin(&mut blk);
    let mut rp = RecordPage::new(&mut tx, &mut blk, layout);
    rp.format();

    println!("Filling the page with random records.");
    let mut slot = rp.insert_after(-1);
    let mut manual_slot = 0;
    // TODO: change to 20 and not unwrap in page#get_u64
    while manual_slot <= 15 {
        let mut rng = rand::thread_rng();
        let n = rng.gen_range(1..50);
        rp.set_int(manual_slot.try_into().unwrap(), "A".to_string(), n);
        rp.set_string(
            manual_slot.try_into().unwrap(),
            "B".to_string(),
            "rec".to_string() + &n.to_string(),
        );
        println!("inserting into slot {manual_slot}: ({n}, rec{n})");
        manual_slot += 1
    }
    // need to culc after rp.set_int
    while slot >= 0 {
        slot = rp.insert_after(slot);
    }

    println!("Deleting these records, whose A values are less than 25.");
    let mut count = 0;
    slot = rp.next_after(-1);

    while slot >= 0 {
        let a = rp.get_int_u32(slot.try_into().unwrap(), "A".to_string());
        let b = rp.get_string(slot.try_into().unwrap(), "B".to_string());
        if a < 25 {
            count += 1;
            println!("slot {slot}: ({a}, {b})");
            rp.delete(slot.try_into().unwrap());
        }
        slot = rp.next_after(slot);
    }
    println!("{count} values under 25 were deleted.");

    println!("Here are the remaining records.");
    slot = rp.next_after(-1);
    while slot >= 0 {
        let a = rp.get_int_u32(slot.try_into().unwrap(), "A".to_string());
        let b = rp.get_string(slot.try_into().unwrap(), "B".to_string());
        println!("slot {slot}: ({a}, {b})");
        slot = rp.next_after(slot);
    }
    tx.unpin(&mut blk);
    tx.commit();
}
