use crate::{
    file::{block_id::BlockId, file_mgr::FileMgr, page::Page},
    logging::log_mgr::LogMgr,
};

#[derive(PartialEq, Clone, Debug)]
pub struct Buffer {
    fm: FileMgr,
    lm: LogMgr,
    contents: Page,
    blk: Option<BlockId>,
    pub pins: i32,
    tx_num: i32,
    lsn: i32,
}

impl Buffer {
    pub fn new(fm: &mut FileMgr, lm: &mut LogMgr) -> Self {
        let contents = Page::new(fm.block_size());
        Buffer {
            fm: fm.to_owned(),
            lm: lm.to_owned(),
            contents,
            blk: None,
            pins: 0,
            tx_num: -1,
            lsn: -1,
        }
    }

    pub fn contents(&mut self) -> Page {
        self.contents.to_owned()
    }

    pub fn set_contents(&mut self, page: Page) {
        self.contents = page
    }

    pub fn block(&mut self) -> Option<BlockId> {
        self.blk.to_owned()
    }

    pub fn set_modified(&mut self, tx_num: i32, lsn: i32) {
        self.tx_num = tx_num;
        if lsn >= 0 {
            self.lsn = lsn;
        }
    }

    pub fn is_pinned(&mut self) -> bool {
        self.pins > 0
    }

    pub fn modifying_tx(&mut self) -> i32 {
        self.tx_num
    }

    pub fn assign_to_block(&mut self, blk: &mut BlockId) {
        Self::flush(self);
        self.blk = Some(blk.to_owned());
        self.fm.read(blk, &mut self.contents);
        self.pins = 0;
    }

    pub fn flush(&mut self) {
        if self.tx_num >= 0 {
            self.lm.flush_with_lsn(self.lsn.try_into().unwrap());
            self.fm
                .write(&mut self.blk.as_mut().unwrap(), &mut self.contents);
            self.tx_num -= 1;
        }
    }

    pub fn pin(&mut self) {
        self.pins += 1
    }

    pub fn unpin(&mut self) {
        self.pins -= 1
    }
}
