use bytebuffer::ByteBuffer;

use crate::file::page::Page;

use super::{set_int_record::SetIntRecord, set_string_record::SetStringRecord};

pub struct LogRecord {}

impl LogRecord {
    // pub fn create_log_record(bytes: Vec<u8>) {
    //     let page = Page::new_from_buffer(&mut ByteBuffer::from_bytes(&bytes));
    //     match page.get_u64(0) {
    //         4 => SetIntRecord::new(page),
    //         5 => SetStringRecord::new(page),
    //     }
    // }
}
