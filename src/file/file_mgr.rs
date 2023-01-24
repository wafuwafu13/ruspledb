use crate::file::block_id::BlockId;
use crate::file::page::Page;
use bytebuffer::ByteBuffer;
use std::fs;
use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;

#[derive(Clone)]
pub struct FileMgr {
    pub db_dir: String,
    pub block_size: u64,
}

impl FileMgr {
    pub fn new(db_dir: String, block_size: u64) -> Self {
        if Self::is_new(&db_dir) {
            fs::create_dir_all(&db_dir).unwrap();
        }
        FileMgr { db_dir, block_size }
    }

    pub fn read(&mut self, blk: &mut BlockId, page: &mut Page) {
        let file_path = Path::new(&self.db_dir).join(blk.file_name());
        let mut file = match Self::is_new(file_path.to_str().unwrap()) {
            true => OpenOptions::new()
                .write(true)
                .read(true)
                .create_new(true)
                .open(file_path)
                .unwrap(),
            false => OpenOptions::new().read(true).open(file_path).unwrap(),
        };
        file.seek(SeekFrom::Start(blk.blk_num() * self.block_size))
            .unwrap();
        let mut buf_reader = BufReader::new(file);
        let buf = buf_reader.fill_buf().unwrap();
        page.set_buffer(ByteBuffer::from_bytes(buf));
    }

    pub fn write(&mut self, blk: &mut BlockId, page: &mut Page) {
        let file_path = Path::new(&self.db_dir).join(blk.file_name());
        let mut file = match Self::is_new(file_path.to_str().unwrap()) {
            true => OpenOptions::new()
                .write(true)
                .read(true)
                .create_new(true)
                .open(file_path)
                .unwrap(),
            false => OpenOptions::new().write(true).open(file_path).unwrap(),
        };
        file.seek(SeekFrom::Start(blk.blk_num() * self.block_size))
            .unwrap();
        file.write_all(page.contents().as_bytes()).unwrap();
    }

    pub fn append(&mut self, file_name: &mut String) -> BlockId {
        let blk_num = self.length(file_name.to_string());
        let mut blk = BlockId::new(file_name.to_string(), blk_num);
        let mut buffer = ByteBuffer::new();
        buffer.resize(self.block_size.try_into().unwrap());
        let file_path = Path::new(&self.db_dir).join(blk.file_name());
        let mut file = match Self::is_new(file_path.to_str().unwrap()) {
            true => OpenOptions::new()
                .write(true)
                .read(true)
                .create_new(true)
                .open(file_path)
                .unwrap(),
            false => OpenOptions::new().write(true).open(file_path).unwrap(),
        };
        file.seek(SeekFrom::Start(blk.blk_num() * self.block_size))
            .unwrap();
        file.write(buffer.as_bytes()).unwrap();
        blk
    }

    pub fn length(&mut self, file_name: String) -> u64 {
        let file_path = Path::new(&self.db_dir).join(file_name);
        let file = match Self::is_new(file_path.to_str().unwrap()) {
            true => OpenOptions::new()
                .write(true)
                .read(true)
                .create_new(true)
                .open(file_path)
                .unwrap(),
            false => OpenOptions::new().read(true).open(file_path).unwrap(),
        };
        file.metadata().unwrap().len() / self.block_size
    }

    pub fn is_new(path: &str) -> bool {
        !Path::new(path).exists()
    }

    pub fn block_size(&mut self) -> u64 {
        self.block_size
    }
}
