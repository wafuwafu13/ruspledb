use std::collections::HashMap;

#[derive(Clone)]
pub struct Schema {
    fields: Vec<String>,
    info: HashMap<String, FiledInfo>,
}

#[derive(Clone)]
pub struct FiledInfo {
    pub field_type: u64,
    length: u64,
}

impl Schema {
    pub fn new() -> Self {
        Schema {
            fields: vec![],
            info: HashMap::new(),
        }
    }

    pub fn add_field(&mut self, field_name: &str, field_type: u64, length: u64) {
        self.fields.append(&mut vec![field_name.to_string()]);
        self.info
            .insert(field_name.to_string(), FiledInfo { field_type, length });
    }

    pub fn add_string_field(&mut self, field_name: &str, length: u64) {
        // VARCHAR = 12
        Self::add_field(self, field_name, 12, length)
    }

    pub fn add_int_field(&mut self, field_name: &str) {
        // INT = 4
        Self::add_field(self, field_name, 4, 0)
    }

    pub fn fields(&mut self) -> Vec<String> {
        self.fields.to_owned()
    }

    pub fn get_type(&mut self, field_name: &str) -> u64 {
        self.info.get(field_name).unwrap().field_type
    }

    pub fn length(&mut self, field_name: &str) -> u64 {
        self.info.get(field_name).unwrap().length
    }
}
