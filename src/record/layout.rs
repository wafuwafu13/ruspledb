use std::collections::HashMap;

use super::schema::Schema;

#[derive(Clone)]
pub struct Layout {
    schema: Schema,
    offsets: HashMap<String, u64>,
    slot_size: u64,
}

impl Layout {
    pub fn new(schema: &mut Schema) -> Self {
        let mut offsets: HashMap<String, u64> = HashMap::new();
        let mut pos = 4; // leave space for the empty/inuse flag
        for field_name in schema.fields().iter_mut() {
            offsets.insert(field_name.to_string(), pos);
            let field_type = schema.get_type(&field_name);
            pos += match field_type {
                // Integer.BYTES
                4 => 4 + 4, // also record_page#get_int_u32 plus 4
                // max_length
                _ => 4 + schema.length(&field_name),
            };
        }
        Layout {
            schema: schema.to_owned(),
            offsets,
            slot_size: pos.try_into().unwrap(),
        }
    }

    pub fn schema(&mut self) -> Schema {
        self.schema.to_owned()
    }

    pub fn offset(&mut self, field_name: &str) -> u64 {
        self.offsets.get(field_name).unwrap().to_owned()
    }

    pub fn slot_size(&mut self) -> u64 {
        self.slot_size
    }
}
