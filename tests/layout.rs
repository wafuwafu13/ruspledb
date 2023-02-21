extern crate ruspledb;
use crate::ruspledb::record::layout::Layout;
use crate::ruspledb::record::schema::Schema;

use std::fs;
use std::path::Path;

#[test]
fn integration_tx() {
    let db_dir = "./db/layouttest";
    if Path::new(db_dir).exists() {
        fs::remove_dir_all(db_dir).unwrap();
    }
    let mut schema = Schema::new();
    schema.add_string_field("A", 9);
    schema.add_int_field("B");
    let mut layout = Layout::new(&mut schema);
    for field_name in layout.schema().fields().iter_mut() {
        let offset = layout.offset(field_name);
        println!("{} has offset {}", field_name, offset);
    }
}
