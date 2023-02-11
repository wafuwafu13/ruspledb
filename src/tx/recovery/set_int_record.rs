use bytebuffer::ByteBuffer;

use crate::{
    file::{block_id::BlockId, page::Page},
    logging::log_mgr::LogMgr,
    tx::transaction::Transaction,
};

pub struct SetIntRecord {
    tx_num: i32,
    offset: u64,
    val: u64,
    blk: BlockId,
}

impl SetIntRecord {
    pub fn new(mut page: Page) -> Self {
        let t_pos = 4;
        let tx_num = page.get_i32(t_pos);
        let f_pos = t_pos + 4;
        let file_name = page.get_string(f_pos);
        let b_pos = f_pos + page.max_length(file_name.len());
        let blk_num = page.get_u64(b_pos);
        let blk = BlockId::new(file_name, blk_num);
        let o_pos = b_pos + 4;
        let offset = page.get_u64(o_pos);
        let v_pos = o_pos + 4;
        let val = page.get_u64(v_pos);
        SetIntRecord {
            tx_num,
            offset,
            val,
            blk,
        }
    }

    pub fn op(&mut self) -> u64 {
        // SETINT = 4
        4
    }

    pub fn tx_num(&mut self) -> i32 {
        self.tx_num
    }

    pub fn undo(&mut self, tx: &mut Transaction) {
        tx.pin(&mut self.blk);
        tx.set_int(&mut self.blk, self.offset, self.val, false); // don't log the undo!
        tx.unpin(&mut self.blk);
    }

    pub fn write_to_log(
        lm: &mut LogMgr,
        tx_num: i32,
        blk: &mut BlockId,
        offset: u64,
        val: u64,
    ) -> i64 {
        let t_pos = 4;
        let f_pos = t_pos + 4;
        let b_pos = f_pos + Self::max_length(blk.file_name().len());
        let o_pos = b_pos + 4;
        let v_pos = o_pos + 4;
        let mut rec = ByteBuffer::new();
        rec.resize(v_pos + 4);
        let mut p = Page::new_from_buffer(&mut rec);
        // SETINT = 4
        p.set_u64(0, 4);
        p.set_i32(t_pos, tx_num);
        p.set_string(f_pos, blk.file_name().to_string());
        p.set_u64(b_pos, blk.blk_num());
        p.set_u64(o_pos, offset);
        p.set_u64(v_pos, val);
        lm.append(rec.into_bytes())
    }

    fn max_length(str_len: usize) -> usize {
        4 + str_len
    }
}
