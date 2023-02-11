use bytebuffer::ByteBuffer;
use std::ops::Add;

#[derive(Clone, PartialEq, Debug)]
pub struct Page {
    block_size: u64,
    pub buffer: ByteBuffer,
}

impl Page {
    pub fn new(block_size: u64) -> Self {
        let mut buffer = ByteBuffer::new();
        buffer.resize(block_size.try_into().unwrap());
        Page { block_size, buffer }
    }

    pub fn new_from_buffer(buffer: &mut ByteBuffer) -> Self {
        Page {
            block_size: 0,
            buffer: buffer.to_owned(),
        }
    }

    pub fn set_buffer(&mut self, buf: ByteBuffer) {
        self.buffer = buf
    }

    pub fn get_bytes(&mut self, offset: usize) -> Vec<u8> {
        self.buffer.set_rpos(offset);
        let length = self.buffer.read_u64().unwrap();
        self.buffer.set_rpos(offset);
        self.buffer.read_bytes(length.try_into().unwrap()).unwrap()
    }

    pub fn get_log_bytes(&mut self, offset: usize) -> Vec<u8> {
        self.buffer.set_rpos(offset);
        // read string length
        let length = self.buffer.read_u32().unwrap();
        // re set offset
        self.buffer.set_rpos(offset);
        /*
          record10 , 110
          [0, 0, 0, 8, 114, 101, 99, 111, 114, 100, 49, 48, 0, 0, 0, 110]
          record9 , 109
          [0, 0, 0, 7, 114, 101, 99, 111, 114, 100, 57, 0, 0, 0, 109]
        */
        self.buffer
            .read_bytes(length.add(8).try_into().unwrap())
            .unwrap()
    }

    pub fn get_u64(&mut self, offset: usize) -> u64 {
        self.buffer.set_rpos(offset);
        self.buffer.read_u64().unwrap()
    }

    pub fn get_u32(&mut self, offset: usize) -> u32 {
        self.buffer.set_rpos(offset);
        self.buffer.read_u32().unwrap()
    }

    pub fn get_i32(&mut self, offset: usize) -> i32 {
        self.buffer.set_rpos(offset);
        self.buffer.read_i32().unwrap()
    }

    pub fn get_string(&mut self, offset: usize) -> String {
        self.buffer.set_rpos(offset);
        self.buffer.read_string().unwrap()
    }

    pub fn set_bytes(&mut self, offset: usize, buffer: Vec<u8>) {
        if self.buffer.len() < offset {
            self.buffer.resize(offset + buffer.len())
        }
        self.buffer.set_wpos(offset);
        self.buffer.write_bytes(&buffer);
    }

    pub fn set_u64(&mut self, offset: usize, n: u64) {
        if self.buffer.len() < offset {
            self.buffer.resize(offset + n.to_be_bytes().len())
        }
        self.buffer.set_wpos(offset);
        self.buffer.write_u64(n);
    }

    pub fn set_u32(&mut self, offset: usize, n: u32) {
        if self.buffer.len() < offset {
            self.buffer.resize(offset + n.to_be_bytes().len())
        }
        self.buffer.set_wpos(offset);
        self.buffer.write_u32(n);
    }

    pub fn set_i32(&mut self, offset: usize, n: i32) {
        if self.buffer.len() < offset {
            self.buffer.resize(offset + n.to_be_bytes().len())
        }
        self.buffer.set_wpos(offset);
        self.buffer.write_i32(n);
    }

    pub fn set_string(&mut self, offset: usize, s: String) {
        if self.buffer.len() < offset {
            self.buffer.resize(offset + s.len())
        }
        self.buffer.set_wpos(offset);
        self.buffer.write_string(&s);
    }

    pub fn max_length(&mut self, str_len: usize) -> usize {
        4 + str_len
    }

    pub fn contents(&mut self) -> &ByteBuffer {
        self.buffer.set_rpos(0);
        &self.buffer
    }
}
