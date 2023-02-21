use std::collections::HashMap;

use crate::{
    buffer::{buffer::Buffer, buffer_mgr::BufferMgr},
    file::block_id::BlockId,
};

#[derive(Clone)]
pub struct BufferList {
    bm: BufferMgr,
    pins: Vec<BlockId>,
    buffers: HashMap<BlockId, Buffer>,
}

impl BufferList {
    pub fn new(bm: &mut BufferMgr) -> Self {
        BufferList {
            bm: bm.to_owned(),
            pins: vec![],
            buffers: HashMap::new(),
        }
    }

    pub fn get_buffer(&mut self, blk: &mut BlockId) -> Option<&Buffer> {
        self.buffers.get(blk)
    }

    pub fn set_buffer(&mut self, blk: BlockId, buffer: Buffer) -> Option<Buffer> {
        self.buffers.insert(blk, buffer)
    }

    pub fn pin(&mut self, blk: &mut BlockId) {
        let buffer = self.bm.pin(blk).unwrap();
        self.buffers.insert(blk.to_owned(), buffer);
        self.pins.append(&mut vec![blk.to_owned()]);
    }

    pub fn unpin(&mut self, blk: &mut BlockId) {
        let mut buffer = self.buffers.get(blk).unwrap().to_owned();
        self.bm.unpin(&mut buffer);
        if let Some(remove_index) = self.pins.iter().position(|pin| *pin == blk.to_owned()) {
            self.pins.remove(remove_index);
        }
    }

    pub fn unpin_all(&mut self) {
        for blk in self.pins.iter_mut() {
            let buffer = self.buffers.get(blk).unwrap();
            self.bm.unpin(&mut buffer.to_owned());
        }
        self.buffers.clear();
        self.pins.clear();
    }
}
