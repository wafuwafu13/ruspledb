use std::collections::HashMap;

use crate::file::block_id::BlockId;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct LockTable {
    max_time: u64,
    locks: HashMap<BlockId, usize>,
}

impl LockTable {
    pub fn new() -> Self {
        LockTable {
            max_time: 10000,
            locks: HashMap::new(),
        }
    }

    pub fn s_lock(&mut self, blk: &mut BlockId) -> Result<(), String> {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let timestamp =
            since_the_epoch.as_secs() * 1000 + since_the_epoch.subsec_nanos() as u64 / 1_000_000;
        while Self::has_x_lock(self, blk) && !Self::wait_too_long(self, timestamp) {
            let mut child = Command::new("sleep").arg(10.to_string()).spawn().unwrap();
            child.wait().unwrap();
        }
        if Self::has_x_lock(self, blk) {
            return Err("LockAbortException".to_string());
        }
        let value = self.get_lock_val(blk);
        self.locks.insert(blk.to_owned(), value + 1);
        Ok(())
    }

    pub fn unlock(&mut self, blk: &mut BlockId) {
        let value = self.get_lock_val(blk);
        if value > 1 {
            self.locks.insert(blk.to_owned(), value - 1);
        } else {
            self.locks.remove(blk);
            // notifyAll();
        }
    }

    fn has_x_lock(&mut self, blk: &mut BlockId) -> bool {
        Self::get_lock_val(self, blk) < 0
    }

    fn get_lock_val(&mut self, blk: &mut BlockId) -> usize {
        let i_val = self.locks.get(blk);
        match i_val {
            Some(val) => *val,
            None => 0,
        }
    }

    fn wait_too_long(&mut self, start_time: u64) -> bool {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let timestamp =
            since_the_epoch.as_secs() * 1000 + since_the_epoch.subsec_nanos() as u64 / 1_000_000;
        timestamp - start_time > self.max_time
    }
}
