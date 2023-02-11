use bytebuffer::ByteBuffer;

use crate::{file::page::Page, logging::log_mgr::LogMgr};

pub struct CommitRecord {}

impl CommitRecord {
    pub fn write_to_log(lm: &mut LogMgr, tx_num: i32) -> i64 {
        let mut rec = ByteBuffer::new();
        rec.resize(2 * 4);
        let mut p = Page::new_from_buffer(&mut rec);
        // SETINT = 2
        p.set_u64(0, 2);
        p.set_i32(4, tx_num);
        lm.append(rec.into_bytes())
    }
}
