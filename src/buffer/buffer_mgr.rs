use crate::file::page::Page;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    buffer::buffer::Buffer,
    file::{block_id::BlockId, file_mgr::FileMgr},
    logging::log_mgr::LogMgr,
};

#[derive(Clone)]
pub struct BufferMgr {
    fm: FileMgr,
    lm: LogMgr,
    buffer_size: usize,
    pub buffer_pool: Vec<Buffer>,
    available_num: i32,
    max_time: u64,
}

struct BufferRes {
    buffer: Option<Buffer>,
    idx: usize,
}

impl BufferMgr {
    pub fn new(fm: &mut FileMgr, lm: &mut LogMgr, buffer_size: usize) -> Self {
        let mut buffer_pool = Vec::with_capacity(buffer_size);
        for _ in 0..buffer_size {
            buffer_pool.push(Buffer::new(fm, lm));
        }
        BufferMgr {
            fm: fm.to_owned(),
            lm: lm.to_owned(),
            buffer_size,
            buffer_pool,
            available_num: buffer_size.try_into().unwrap(),
            max_time: 10000,
        }
    }

    pub fn available(&mut self) -> i32 {
        self.available_num
    }

    pub fn flush_all(&mut self, tx_num: i32) {
        for buffer in self.buffer_pool.iter_mut() {
            if buffer.modifying_tx() == tx_num {
                buffer.flush()
            }
        }
    }

    pub fn unpin(&mut self, buffer: &mut Buffer) {
        buffer.unpin();
        let finded_buffer_res = Self::find_existing_buffer(self, &mut buffer.block().unwrap());
        // update buffer_pool
        self.buffer_pool[finded_buffer_res.idx] = buffer.to_owned();
        if !buffer.is_pinned() {
            self.available_num += 1;
            // notifyAll();
        }
    }

    pub fn pin(&mut self, blk: &mut BlockId) -> Result<Buffer, String> {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let timestamp =
            since_the_epoch.as_secs() * 1000 + since_the_epoch.subsec_nanos() as u64 / 1_000_000;
        let mut buffer = Self::try_to_pin(self, blk);
        while buffer == None && !Self::wait_too_long(self, timestamp) {
            let mut child = Command::new("sleep").arg(10.to_string()).spawn().unwrap();
            child.wait().unwrap();
            buffer = Self::try_to_pin(self, blk)
        }
        match buffer {
            Some(buffer) => Ok(buffer),
            None => Err("BufferAbortException".to_string()),
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

    fn try_to_pin(&mut self, blk: &mut BlockId) -> Option<Buffer> {
        let mut found_buffer_res = Self::find_existing_buffer(self, blk);
        match found_buffer_res.buffer {
            Some(ref mut buffer) => {
                if !buffer.is_pinned() {
                    self.available_num -= 1;
                }
                buffer.pin();
                // update buffer_pool
                self.buffer_pool[found_buffer_res.idx] = buffer.to_owned();
                return Some(buffer.to_owned());
            }
            None => {
                let mut choosed_buffer_res = Self::choose_unpinned_buffer(self);
                match choosed_buffer_res.buffer {
                    Some(ref mut buffer) => {
                        buffer.assign_to_block(blk);
                        // if blk file don't exist, contents of buffer will be reset when assign_to_block -> fm.read -> page.set_buffer
                        if buffer.contents().buffer.len() == 0 {
                            buffer.set_contents(Page::new(self.fm.block_size()))
                        }
                        if !buffer.is_pinned() {
                            self.available_num -= 1;
                        }
                        buffer.pin();
                        // update buffer_pool
                        self.buffer_pool[choosed_buffer_res.idx] = buffer.to_owned();
                        return Some(buffer.to_owned());
                    }
                    None => return None,
                };
            }
        };
    }

    fn find_existing_buffer(&mut self, blk: &mut BlockId) -> BufferRes {
        for (i, buffer) in self.buffer_pool.iter_mut().enumerate() {
            let b = buffer.block();
            match b {
                Some(b) => {
                    if b.equals(blk) {
                        return BufferRes {
                            buffer: Some(buffer.to_owned()),
                            idx: i,
                        };
                    }
                }
                None => {}
            };
        }
        return BufferRes {
            buffer: None,
            idx: 1000000,
        };
    }

    fn choose_unpinned_buffer(&mut self) -> BufferRes {
        for (i, buffer) in self.buffer_pool.iter_mut().enumerate() {
            if !buffer.is_pinned() {
                return BufferRes {
                    buffer: Some(buffer.to_owned()),
                    idx: i.try_into().unwrap(),
                };
            }
        }
        BufferRes {
            buffer: None,
            idx: 1000000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    fn prepare_bm(db_dir: &str) -> BufferMgr {
        if Path::new(&db_dir.to_string()).exists() {
            fs::remove_dir_all(db_dir).unwrap();
        }
        let block_size = 400;
        let mut fm = FileMgr::new(db_dir.to_string(), block_size);
        let log_file = "ruspledb.log";
        let mut lm = LogMgr::new(&mut fm, &mut log_file.to_string());
        let buffer_size = 3;
        let bm = BufferMgr::new(&mut fm, &mut lm, buffer_size);
        bm
    }

    #[test]
    fn unit_find_existing_buffer() {
        let mut bm = prepare_bm("./db/buffermgrunittest_1");
        let mut b0 = BlockId::new("testfile".to_string(), 0);
        let buffer_res = bm.find_existing_buffer(&mut b0);
        assert_eq!(buffer_res.buffer, None);
        assert_eq!(buffer_res.idx, 1000000);
        assert_eq!(bm.available(), 3);
    }

    #[test]
    fn unit_choose_unpined_buffer() {
        let mut bm = prepare_bm("./db/buffermgrunittest_2");
        let mut buffer_res = bm.choose_unpinned_buffer();
        assert_eq!(buffer_res.buffer.as_mut().unwrap().block(), None);
        assert_eq!(buffer_res.idx, 0);

        assert_eq!(bm.available(), 3);
    }

    #[test]
    fn unit_try_to_pin() {
        let mut bm = prepare_bm("./db/buffermgrunittest_3");
        let mut b0 = BlockId::new("testfile".to_string(), 0);
        let mut buffer = bm.try_to_pin(&mut b0);
        assert_eq!(
            buffer.as_mut().unwrap().block().unwrap().file_name(),
            "testfile"
        );
        assert_eq!(bm.available(), 2);
        assert_eq!(buffer.as_mut().unwrap().pins, 1);

        let mut buffer = bm.try_to_pin(&mut b0);
        assert_eq!(
            buffer.as_mut().unwrap().block().unwrap().file_name(),
            "testfile"
        );
        assert_eq!(bm.available(), 2);
        assert_eq!(buffer.unwrap().pins, 2);
    }
}
