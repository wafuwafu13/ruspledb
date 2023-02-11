use crate::tx::recovery::commit_record::CommitRecord;
use crate::tx::recovery::set_int_record::SetIntRecord;
use crate::tx::recovery::set_string_record::SetStringRecord;
use crate::{
    buffer::buffer_mgr::BufferMgr,
    file::{block_id::BlockId, file_mgr::FileMgr, page::Page},
    logging::log_mgr::LogMgr,
    tx::concurrency::concurrency_mgr::ConcurrencyMgr,
};
use bytebuffer::ByteBuffer;

use super::buffer_list::BufferList;

pub struct Transaction {
    fm: FileMgr,
    lm: LogMgr,
    bm: BufferMgr,
    tx_num: i32,
    next_tx_num: usize,
    end_of_file: i32,
    // recovery_mgr: RecoveryMgr,
    concurrency_mgr: ConcurrencyMgr,
    my_buffers: BufferList,
}

impl Transaction {
    pub fn new(fm: &mut FileMgr, lm: &mut LogMgr, bm: &mut BufferMgr) -> Self {
        let tx_num = 1;
        Transaction {
            fm: fm.to_owned(),
            lm: lm.to_owned(),
            bm: bm.to_owned(),
            tx_num,
            next_tx_num: tx_num.try_into().unwrap(),
            end_of_file: -1,
            concurrency_mgr: ConcurrencyMgr::new(),
            my_buffers: BufferList::new(bm),
        }
    }

    pub fn commit(&mut self) {
        self.bm.flush_all(self.tx_num);
        let lsn = CommitRecord::write_to_log(&mut self.lm, self.tx_num);
        self.lm.flush_with_lsn(lsn);
        println!("transaction {} commited", self.tx_num);
        self.concurrency_mgr.release();
        self.my_buffers.unpin_all();
    }

    pub fn rollback(&mut self) {
        let mut iter = self.lm.iterator();
        while iter.has_next() {
            let bytes = iter.next();
            let mut page = Page::new_from_buffer(&mut ByteBuffer::from_bytes(&bytes));
            let rec_int = match page.get_u64(0) {
                4 => Some(SetIntRecord::new(page.clone())),
                _ => None,
            };
            let rec_string = match page.get_u64(0) {
                5 => Some(SetStringRecord::new(page.clone())),
                _ => None,
            };
            match rec_int {
                Some(mut rec) => {
                    if rec.tx_num() == self.tx_num {
                        rec.undo(self);
                    }
                }
                None => {}
            };
            match rec_string {
                Some(mut rec) => {
                    if rec.tx_num() == self.tx_num {
                        rec.undo(self);
                    }
                }
                None => {}
            };
        }
        println!("transaction {} rolled back", self.tx_num);
        self.concurrency_mgr.release();
        self.my_buffers.unpin_all();
    }

    pub fn pin(&mut self, blk: &mut BlockId) {
        self.my_buffers.pin(blk)
    }

    pub fn unpin(&mut self, blk: &mut BlockId) {
        self.my_buffers.unpin(blk)
    }

    pub fn get_int(&mut self, blk: &mut BlockId, offset: u64) -> u64 {
        self.concurrency_mgr.s_lock(blk);
        let mut buffer = self.my_buffers.get_buffer(blk).unwrap().to_owned();
        buffer.contents().get_u64(offset.try_into().unwrap())
    }

    pub fn get_string(&mut self, blk: &mut BlockId, offset: u64) -> String {
        self.concurrency_mgr.s_lock(blk);
        let mut buffer = self.my_buffers.get_buffer(blk).unwrap().to_owned();
        buffer.contents().get_string(offset.try_into().unwrap())
    }

    pub fn set_int(&mut self, blk: &mut BlockId, offset: u64, val: u64, ok_to_log: bool) {
        self.concurrency_mgr.x_lock(blk);
        let mut buffer = self.my_buffers.get_buffer(blk).unwrap().to_owned();
        let mut lsn = -1;
        if ok_to_log {
            let old_val = buffer.contents().get_u64(offset.try_into().unwrap());
            let mut blk = buffer.block().unwrap();
            lsn = SetIntRecord::write_to_log(&mut self.lm, self.tx_num, &mut blk, offset, old_val)
        }
        let mut page = buffer.contents();
        page.set_u64(offset.try_into().unwrap(), val);
        buffer.set_modified(self.tx_num.try_into().unwrap(), lsn.try_into().unwrap());
        // need to set contents directly
        buffer.set_contents(page);
        // update my_buffers
        self.my_buffers.set_buffer(blk.to_owned(), buffer.clone());
        // update buffer_pool
        // TODO: set index correctly
        self.bm.buffer_pool[0] = buffer;
    }

    pub fn set_string(&mut self, blk: &mut BlockId, offset: u64, val: String, ok_to_log: bool) {
        self.concurrency_mgr.x_lock(blk);
        let mut buffer = self.my_buffers.get_buffer(blk).unwrap().to_owned();
        let mut lsn = -1;
        if ok_to_log {
            let old_val = buffer.contents().get_string(offset.try_into().unwrap());
            let blk = buffer.block();
            lsn = SetStringRecord::write_to_log(
                &mut self.lm,
                self.tx_num,
                &mut blk.unwrap(),
                offset,
                old_val,
            )
        }
        let mut page = buffer.contents();
        page.set_string(offset.try_into().unwrap(), val);
        buffer.set_modified(self.tx_num.try_into().unwrap(), lsn.try_into().unwrap());
        // need to set contents directly
        buffer.set_contents(page);
        // update my_buffers
        self.my_buffers.set_buffer(blk.to_owned(), buffer.clone());
        // update buffer_pool
        // TODO: set index correctly
        self.bm.buffer_pool[0] = buffer;
    }

    fn next_tx_num(mut self) -> usize {
        self.next_tx_num += 1;
        self.next_tx_num
    }
}
