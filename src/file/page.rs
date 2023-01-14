use bytebuffer::ByteBuffer;

pub struct Page {
    block_size: u64,
    buffer: ByteBuffer,
}

impl Page {
    pub fn new(block_size: u64) -> Self {
        Page {
            block_size,
            buffer: ByteBuffer::new(),
        }
    }

    pub fn set_buffer(&mut self, buf: ByteBuffer) {
        self.buffer = buf
    }

    pub fn get_i32(&mut self, offset: usize) -> i32 {
        self.buffer.set_rpos(offset);
        self.buffer.read_i32().unwrap()
    }

    pub fn get_string(&mut self, offset: usize) -> String {
        self.buffer.set_rpos(offset);
        self.buffer.read_string().unwrap()
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

    // TODO: Implement
    pub fn max_length(&mut self, str_len: usize) -> usize {
        100 + str_len
    }

    pub fn contents(&mut self) -> &ByteBuffer {
        self.buffer.set_rpos(0);
        &self.buffer
    }
}
