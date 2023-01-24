use crate::file::{block_id::BlockId, file_mgr::FileMgr, page::Page};
use bytebuffer::ByteBuffer;

use super::log_iterator::LogIterator;

pub struct LogMgr {
    fm: FileMgr,
    log_file: String,
    log_page: Page,
    current_blk: BlockId,
    latest_lsn: usize,
    last_saved_lsn: usize,
}

impl LogMgr {
    pub fn new(mut fm: FileMgr, log_file: &mut String) -> Self {
        let mut buffer = ByteBuffer::new();
        buffer.resize(fm.block_size().try_into().unwrap());
        let mut log_page = Page::new_from_buffer(&mut buffer);
        let log_size = fm.length(log_file.to_string());
        let mut current_blk = match log_size {
            0 => Self::append_new_block(&mut fm, log_file, &mut log_page),
            _ => BlockId::new(log_file.to_string(), log_size - 1),
        };
        if log_size != 0 {
            fm.read(&mut current_blk, &mut log_page)
        }
        LogMgr {
            fm,
            log_file: log_file.to_string(),
            log_page,
            current_blk,
            latest_lsn: 0,
            last_saved_lsn: 0,
        }
    }

    pub fn iterator(&mut self) -> LogIterator {
        self.flush();
        LogIterator::new(&mut self.fm, &mut self.current_blk)
    }

    pub fn append(&mut self, log_rec: Vec<u8>) -> usize {
        let mut boundary = self.log_page.get_u64(0);
        let rec_size = log_rec.len();
        let bytes_needed = rec_size + 4;
        let sub = boundary.checked_sub(bytes_needed.try_into().unwrap());
        match sub {
            Some(result) => {
                if result < 4 {
                    self.flush();
                    self.current_blk = Self::append_new_block(
                        &mut self.fm,
                        &mut self.log_file,
                        &mut self.log_page,
                    );
                    boundary = self.log_page.get_u64(0)
                }
            }
            None => {
                self.flush();
                self.current_blk =
                    Self::append_new_block(&mut self.fm, &mut self.log_file, &mut self.log_page);
                boundary = self.log_page.get_u64(0)
            }
        }
        let rec_pos = boundary as usize - bytes_needed;

        self.log_page.set_bytes(rec_pos, log_rec);
        // set the new boundary
        self.log_page.set_u64(0, rec_pos.try_into().unwrap());
        self.latest_lsn += 1;
        self.latest_lsn
    }

    fn append_new_block(fm: &mut FileMgr, log_file: &mut String, log_page: &mut Page) -> BlockId {
        let mut blk = fm.append(log_file);
        // write block_size(400) to buffer
        log_page.set_u64(0, fm.block_size());
        fm.write(&mut blk, log_page);
        blk
    }

    pub fn get_last_saved_lsn(&mut self) -> usize {
        self.last_saved_lsn
    }

    pub fn flush_with_lsn(&mut self, lsn: usize) {
        if lsn >= self.last_saved_lsn {
            self.flush()
        }
    }

    fn flush(&mut self) {
        self.fm.write(&mut self.current_blk, &mut self.log_page);
        self.last_saved_lsn = self.latest_lsn;
    }
}
