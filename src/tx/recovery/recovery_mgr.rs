use crate::{
    buffer::buffer_mgr::BufferMgr, file::file_mgr::FileMgr, logging::log_mgr::LogMgr,
    tx::transaction::Transaction,
};

pub struct RecoveryMgr {
    tx: Box<Transaction>,
    tx_num: usize,
    lm: LogMgr,
    bm: BufferMgr,
}

impl RecoveryMgr {
    pub fn new(tx: Transaction, tx_num: usize, lm: LogMgr, bm: BufferMgr) -> Self {
        // startrecord
        RecoveryMgr {
            tx: Box::new(tx),
            tx_num,
            lm,
            bm,
        }
    }
}
