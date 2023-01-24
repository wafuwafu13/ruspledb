use bytebuffer::ByteBuffer;

use crate::file::{block_id::BlockId, file_mgr::FileMgr, page::Page};

pub struct LogIterator {
    fm: FileMgr,
    blk: BlockId,
    page: Page,
    current_pos: usize,
    boundary: usize,
}

impl LogIterator {
    pub fn new(fm: &mut FileMgr, blk: &mut BlockId) -> Self {
        let mut buffer = ByteBuffer::new();
        buffer.resize(fm.block_size().try_into().unwrap());
        let mut page = Page::new_from_buffer(&mut buffer);

        fm.read(blk, &mut page);
        let boundary = page.get_u64(0);

        LogIterator {
            fm: fm.to_owned(),
            blk: blk.to_owned(),
            page,
            current_pos: boundary.try_into().unwrap(),
            boundary: boundary.try_into().unwrap(),
        }
    }

    pub fn has_next(&mut self) -> bool {
        self.current_pos < self.fm.block_size().try_into().unwrap() || self.blk.blk_num() > 0
    }

    pub fn next(&mut self) -> Vec<u8> {
        if self.current_pos == self.fm.block_size().try_into().unwrap() {
            // blk_num -= 1
            self.blk = BlockId::new(self.blk.file_name().to_string(), self.blk.blk_num() - 1);
            self.fm.read(&mut self.blk, &mut self.page);
            self.boundary = self.page.get_u64(0).try_into().unwrap();
            self.current_pos = self.boundary;
        }
        let rec = self.page.get_log_bytes(self.current_pos);
        self.current_pos += 4 + rec.len();
        rec
    }

    // fn move_to_block(&mut self, mut blk: BlockId) {
    //     self.fm.read(&mut blk, &mut self.page);
    //     self.boundary = self.page.get_u64(0).try_into().unwrap();
    //     self.current_pos = self.boundary;
    // }
}
