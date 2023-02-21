use crate::{file::block_id::BlockId, tx::transaction::Transaction};

use super::{layout::Layout, record_page::RecordPage, rid::RID};

pub struct TableScan {
    tx: Transaction,
    table_name: String,
    layout: Layout,
    rp: Option<RecordPage>,
    current_slot: i64,
    file_name: String,
}

impl TableScan {
    pub fn new(tx: &mut Transaction, table_name: &str, layout: Layout) -> Self {
        let mut rp: Option<RecordPage> = None;
        let mut current_slot = 0;
        let file_name = table_name.to_string() + ".tbl";
        if tx.size(&file_name) == 0 {
            // move_to_new_block
            let mut blk = tx.append(&file_name);
            rp = Some(RecordPage::new(tx, &mut blk, layout.clone()));
            rp.as_mut().unwrap().format();
            current_slot -= 1;
        } else {
            // move_to_block(0)
            let mut blk = BlockId::new(file_name.to_string(), 0);
            rp = Some(RecordPage::new(tx, &mut blk, layout.clone()));
            current_slot -= 1;
        };
        TableScan {
            tx: tx.to_owned(),
            table_name: table_name.to_string(),
            layout,
            rp,
            current_slot,
            file_name,
        }
    }

    pub fn before_first(&mut self) {
        self.move_to_block(0)
    }

    pub fn next(&mut self) -> bool {
        self.current_slot = self.rp.as_mut().unwrap().next_after(self.current_slot);
        while self.current_slot < 0 {
            if self.at_last_block() {
                return false;
            }
            self.move_to_block(self.rp.clone().as_mut().unwrap().block().blk_num() + 1);
            self.current_slot = self.rp.as_mut().unwrap().next_after(self.current_slot);
        }
        true
    }

    pub fn get_int(&mut self, field_name: &str) -> u64 {
        self.rp.as_mut().unwrap().get_int(
            self.current_slot.try_into().unwrap(),
            field_name.to_string(),
        )
    }

    pub fn get_int_u32(&mut self, field_name: &str) -> u32 {
        self.current_slot += 1;
        self.rp.as_mut().unwrap().get_int_u32(
            self.current_slot.try_into().unwrap(),
            field_name.to_string(),
        )
    }

    pub fn get_string(&mut self, field_name: &str) -> String {
        self.rp.as_mut().unwrap().get_string(
            self.current_slot.try_into().unwrap(),
            field_name.to_string(),
        )
    }

    pub fn set_int(&mut self, field_name: &str, value: u64) {
        self.rp.as_mut().unwrap().set_int(
            self.current_slot.try_into().unwrap(),
            field_name.to_string(),
            value,
        )
    }

    pub fn set_string(&mut self, field_name: &str, value: String) {
        self.rp.as_mut().unwrap().set_string(
            self.current_slot.try_into().unwrap(),
            field_name.to_string(),
            value,
        )
    }

    pub fn close(&mut self) {
        match self.rp.clone() {
            Some(mut rp) => self.tx.unpin(&mut rp.block()),
            None => {}
        }
    }

    pub fn insert(&mut self) {
        self.current_slot = self.rp.as_mut().unwrap().insert_after(self.current_slot);
        while self.current_slot < 0 {
            if self.at_last_block() {
                self.move_to_new_block()
            } else {
                self.move_to_block(self.rp.clone().as_mut().unwrap().block().blk_num() + 1)
            }
            self.current_slot = self.rp.as_mut().unwrap().insert_after(self.current_slot);
        }
    }

    pub fn delete(&mut self) {
        self.rp
            .as_mut()
            .unwrap()
            .delete(self.current_slot.try_into().unwrap())
    }

    pub fn get_rid(&mut self) -> RID {
        RID::new(
            self.rp.as_mut().unwrap().block().blk_num(),
            self.current_slot.try_into().unwrap(),
        )
    }

    fn move_to_block(&mut self, blk_num: u64) {
        Self::close(self);
        let mut blk = BlockId::new(self.file_name.to_string(), blk_num);
        self.rp = Some(RecordPage::new(&mut self.tx, &mut blk, self.layout.clone()));
        self.current_slot = -1;
    }

    fn move_to_new_block(&mut self) {
        Self::close(self);
        let mut blk = self.tx.append(&self.file_name);
        self.rp = Some(RecordPage::new(&mut self.tx, &mut blk, self.layout.clone()));
        self.rp.as_mut().unwrap().format();
        self.current_slot = -1;
    }

    fn at_last_block(&mut self) -> bool {
        self.rp.as_mut().unwrap().block().blk_num() == self.tx.size(&self.file_name) - 1
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::buffer_mgr::BufferMgr;
    use crate::file::file_mgr::FileMgr;
    use crate::logging::log_mgr::LogMgr;
    use crate::record::schema::Schema;

    use super::*;
    use std::fs;
    use std::path::Path;

    fn prepare_ts(db_dir: &str) -> TableScan {
        if Path::new(&db_dir.to_string()).exists() {
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
        let layout = Layout::new(&mut schema);
        TableScan::new(&mut tx, "T", layout)
    }

    #[test]
    fn unit_get_int() {
        let mut ts = prepare_ts("./db/tablescanunittest_1");
        ts.insert();
        ts.set_int("A", 25);
        ts.set_int("A", 24);
        ts.set_int("A", 23);

        ts.before_first();

        let mut a = ts.get_int("A");
        assert_eq!(a, 23);
        a = ts.get_int("A");
        assert_eq!(a, 23);
    }
}
