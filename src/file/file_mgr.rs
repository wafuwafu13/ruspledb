use crate::file::block_id::BlockId;
use crate::file::page::Page;
use bytebuffer::ByteBuffer;
use std::fs::OpenOptions;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;

pub struct FileMgr {
    pub db_path: String,
    pub block_size: u64,
}

impl FileMgr {
    pub fn new(db_path: String, block_size: u64) -> Self {
        if Self::is_new(&db_path) {
            let path = std::path::Path::new(&db_path);
            let prefix = path.parent().unwrap();
            std::fs::create_dir_all(prefix).unwrap();
            Self::touch(path).unwrap_or_else(|why| {
                println!("! {:?}", why.kind());
            });
        }

        // TODO: Remove any leftover temporary tables

        FileMgr {
            db_path,
            block_size,
        }
    }

    fn touch(path: &Path) -> io::Result<()> {
        match OpenOptions::new().create(true).write(true).open(path) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub fn read(&mut self, mut blk: BlockId, p: &mut Page) {
        let mut file = OpenOptions::new().read(true).open(&self.db_path).unwrap();
        file.seek(SeekFrom::Start(blk.blk_num())).unwrap();
        let mut buf_reader = BufReader::new(file);
        let buf = buf_reader.fill_buf().unwrap();
        p.set_buffer(ByteBuffer::from_bytes(buf))
    }

    pub fn write(&mut self, blk: &mut BlockId, mut p: Page) {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.db_path)
            .unwrap();
        file.seek(SeekFrom::Start(blk.blk_num())).unwrap();
        file.write_all(p.contents().as_bytes()).unwrap();
    }

    pub fn is_new(db_path: &str) -> bool {
        !Path::new(db_path).exists()
    }

    pub fn block_size(&mut self) -> u64 {
        self.block_size
    }
}
