use crate::{file::block_id::BlockId, tx::transaction::Transaction};

use super::layout::Layout;

#[derive(Clone)]
pub struct RecordPage {
    tx: Transaction,
    blk: BlockId,
    layout: Layout,
}

impl RecordPage {
    pub fn new(tx: &mut Transaction, blk: &mut BlockId, layout: Layout) -> Self {
        tx.pin(blk);
        RecordPage {
            tx: tx.to_owned(),
            blk: blk.to_owned(),
            layout,
        }
    }

    pub fn get_int(&mut self, slot: u64, field_name: String) -> u64 {
        let field_pos = slot * self.layout.slot_size() + self.layout.offset(&field_name);
        self.tx.get_int(&mut self.blk, field_pos)
    }

    pub fn get_int_u32(&mut self, slot: u64, field_name: String) -> u32 {
        // need +4
        let field_pos = slot * self.layout.slot_size() + self.layout.offset(&field_name) + 4;
        self.tx.get_int_u32(&mut self.blk, field_pos)
    }

    pub fn get_string(&mut self, slot: u64, field_name: String) -> String {
        let field_pos = slot * self.layout.slot_size() + self.layout.offset(&field_name);
        self.tx.get_string(&mut self.blk, field_pos)
    }

    pub fn set_int(&mut self, slot: u64, field_name: String, value: u64) {
        let field_pos = slot * self.layout.slot_size() + self.layout.offset(&field_name);
        self.tx.set_int(&mut self.blk, field_pos, value, true)
    }

    pub fn set_string(&mut self, slot: u64, field_name: String, value: String) {
        let field_pos = slot * self.layout.slot_size() + self.layout.offset(&field_name);
        self.tx.set_string(&mut self.blk, field_pos, value, true)
    }

    pub fn delete(&mut self, slot: u64) {
        self.tx.set_int(
            &mut self.blk,
            slot * self.layout.slot_size(),
            0,
            /* EMPTY */ true,
        );
    }

    pub fn format(&mut self) {
        let mut slot = 0;
        while (slot + 1) * self.layout.slot_size() <= self.tx.block_size() {
            self.tx.set_int(
                &mut self.blk,
                slot * self.layout.slot_size(),
                0, /* EMPTY */
                false,
            );
            let mut schema = self.layout.schema();
            for field_name in schema.fields().iter_mut() {
                let field_pos = slot * self.layout.slot_size() + self.layout.offset(field_name);
                if schema.get_type(field_name) == 4
                /* INTEGER */
                {
                    self.tx.set_int(&mut self.blk, field_pos, 0, false)
                } else {
                    self.tx
                        .set_string(&mut self.blk, field_pos, "".to_string(), false)
                }
            }
            slot += 1;
        }
    }

    pub fn next_after(&mut self, slot: i64) -> i64 {
        self.search_after(slot, /* USED */ 1)
    }

    pub fn insert_after(&mut self, slot: i64) -> i64 {
        let new_slot = self.search_after(slot, /* EMPTY */ 0);
        if new_slot >= 0 {
            self.tx.set_int(
                //
                &mut self.blk,
                (new_slot as u64) * self.layout.slot_size(),
                /* USED */ 1,
                true,
            );
        };
        new_slot
    }

    fn search_after(&mut self, slot: i64, flag: u8) -> i64 {
        let mut result_slot = slot;
        result_slot += 1;
        while (result_slot + 1) * (self.layout.slot_size() as i64)
            <= self.tx.block_size().try_into().unwrap()
        {
            if self.tx.get_int(
                &mut self.blk,
                (result_slot as u64) * self.layout.slot_size(),
            ) == flag.into()
            {
                return result_slot;
            }
            result_slot += 1
        }
        -1
    }

    pub fn block(&mut self) -> BlockId {
        self.blk.to_owned()
    }

    // fn is_valid_slot(&mut self, slot: u64) -> bool {
    //   self.offset(slot + 1) <= self.tx.block_size()
    // }

    // fn offset(&mut self, slot: u64) -> u64 {
    //   slot * self.layout.slot_size()
    // }
}

#[cfg(test)]
mod tests {
    use crate::buffer::buffer_mgr::BufferMgr;
    use crate::file::file_mgr::FileMgr;
    use crate::logging::log_mgr::LogMgr;
    use crate::record::schema::Schema;

    use super::*;
    use rand::Rng;
    use std::fs;
    use std::path::Path;

    fn prepare_rp(db_dir: &str) -> RecordPage {
        if Path::new(&db_dir.to_string()).exists() {
            fs::remove_dir_all(db_dir).unwrap();
        }
        let block_size = 400;
        let mut fm = FileMgr::new(db_dir.to_string(), block_size);
        let log_file = "ruspledb.log";
        let mut lm = LogMgr::new(&mut fm, &mut log_file.to_string());
        let buffer_size = 3;
        let mut bm = BufferMgr::new(&mut fm, &mut lm, buffer_size);
        let mut tx = Transaction::new(&mut fm, &mut lm, &mut bm);

        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_string_field("B", 9);
        let layout = Layout::new(&mut schema);
        let mut blk = tx.append("testfile");
        tx.pin(&mut blk);
        let mut rp = RecordPage::new(&mut tx, &mut blk, layout);
        rp.format();
        rp
    }

    #[test]
    fn unit_get_int() {
        let mut rp = prepare_rp("./db/recordunittest_1");
        rp.set_int(0, "A".to_string(), 5);
        rp.set_int(1, "A".to_string(), 27);
        rp.set_int(2, "A".to_string(), 214);
        let i1 = rp.get_int(0, "A".to_string());
        let i2 = rp.get_int(1, "A".to_string());
        let i3 = rp.get_int(2, "A".to_string());
        assert_eq!(i1, 5);
        assert_eq!(i2, 27);
        assert_eq!(i3, 214);
    }

    #[test]
    fn unit_get_string() {
        let mut rp = prepare_rp("./db/recordunittest_2");
        rp.set_string(0, "B".to_string(), "rec5".to_string());
        rp.set_string(1, "B".to_string(), "rec27".to_string());
        rp.set_string(2, "B".to_string(), "rec214".to_string());
        let s1 = rp.get_string(0, "B".to_string());
        let s2 = rp.get_string(1, "B".to_string());
        let s3 = rp.get_string(2, "B".to_string());
        assert_eq!(s1, "rec5");
        assert_eq!(s2, "rec27");
        assert_eq!(s3, "rec214");
    }

    #[test]
    fn unit_search_after() {
        let mut rp = prepare_rp("./db/recordunittest_3");
        rp.set_int(0, "A".to_string(), 25);
        rp.set_int(1, "A".to_string(), 23);
        rp.set_int(2, "A".to_string(), 21);
        rp.set_string(0, "B".to_string(), "rec25".to_string());
        rp.set_string(1, "B".to_string(), "rec27".to_string());
        rp.set_string(2, "B".to_string(), "rec214".to_string());
        let mut slot = rp.insert_after(-1);
        assert_eq!(slot, 0);

        slot = rp.insert_after(slot);
        assert_eq!(slot, 1);

        slot = rp.insert_after(slot);
        assert_eq!(slot, 2);

        slot = rp.insert_after(slot);
        assert_eq!(slot, 3);

        slot = rp.next_after(-1);
        assert_eq!(slot, 0);

        slot = rp.next_after(slot);
        assert_eq!(slot, 1);

        slot = rp.next_after(slot);
        assert_eq!(slot, 2);

        slot = rp.next_after(slot);
        assert_eq!(slot, 3);

        slot = rp.next_after(slot);
        // rp.insert_after(4) is not operated
        assert_eq!(slot, -1);

        let mut a = rp.get_int_u32(0, "A".to_string());
        assert_eq!(a, 25);
        a = rp.get_int_u32(2, "A".to_string());
        assert_eq!(a, 21);

        let mut b = rp.get_string(0, "B".to_string());
        assert_eq!(b, "rec25");
        b = rp.get_string(1, "B".to_string());
        assert_eq!(b, "rec27");
        b = rp.get_string(2, "B".to_string());
        assert_eq!(b, "rec214");
    }

    #[test]
    fn unit_search_after_2() {
        let mut rp = prepare_rp("./db/recordunittest_4");
        let mut slot = rp.insert_after(-1);
        assert_eq!(slot, 0);

        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 25);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 1);

        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 24);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 2);

        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 23);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 3);

        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 22);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 4);

        slot = rp.next_after(-1);
        // oops! should be 0
        assert_eq!(slot, 4);

        slot = rp.next_after(slot);
        // rp.insert_after(5) is not operated
        assert_eq!(slot, -1);

        let mut a = rp.get_int(0, "A".to_string());
        assert_eq!(a, 25);

        a = rp.get_int(3, "A".to_string());
        assert_eq!(a, 22);
    }

    #[test]
    fn unit_search_after_while() {
        let mut rp = prepare_rp("./db/recordunittest_4");
        let mut slot = rp.insert_after(-1);
        assert_eq!(slot, 0);

        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 25);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 1);

        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 24);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 2);
        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 23);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 3);
        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 22);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 4);
        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 22);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 5);
        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 22);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 6);
        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 22);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 7);
        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 22);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 8);
        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 22);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 9);
        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 22);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 10);
        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 22);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 11);
        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 22);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 12);
        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 22);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 13);
        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 22);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 14);
        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 22);
        slot = rp.insert_after(slot);
        assert_eq!(slot, 15);
        rp.set_int(slot.try_into().unwrap(), "A".to_string(), 22);
        slot = rp.insert_after(slot);
        assert_eq!(slot, -1);

        slot = rp.next_after(-1);
        // rp.insert_after(16) is not operated
        assert_eq!(slot, -1);

        let mut a = rp.get_int(0, "A".to_string());
        assert_eq!(a, 25);

        a = rp.get_int(3, "A".to_string());
        assert_eq!(a, 22);
    }
}
