use std::collections::HashMap;

use crate::{file::block_id::BlockId, tx::concurrency::lock_table::LockTable};

#[derive(Clone)]
pub struct ConcurrencyMgr {
    lock_tbl: LockTable,
    locks: HashMap<BlockId, String>,
}

impl ConcurrencyMgr {
    pub fn new() -> Self {
        ConcurrencyMgr {
            lock_tbl: LockTable::new(),
            locks: HashMap::new(),
        }
    }

    pub fn s_lock(&mut self, blk: &mut BlockId) {
        match self.locks.get(blk) {
            Some(_) => self.lock_tbl.s_lock(blk).unwrap(),
            None => {}
        }
    }

    pub fn x_lock(&mut self, blk: &mut BlockId) {
        if !Self::has_x_lock(self, blk) {
            Self::s_lock(self, blk);
        }
    }

    pub fn release(&mut self) {
        for blk in self.locks.keys() {
            self.lock_tbl.unlock(&mut blk.to_owned())
        }
        self.locks.clear();
    }

    fn has_x_lock(&mut self, blk: &mut BlockId) -> bool {
        let lock_type = self.locks.get(&blk);
        match lock_type {
            Some(lock_type) => lock_type.eq("X"),
            None => false,
        }
    }
}
