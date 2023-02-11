#[derive(Clone, PartialEq, Debug, Eq, Hash)]
pub struct BlockId {
    file_name: String,
    blk_num: u64,
}

impl BlockId {
    pub fn new(file_name: String, blk_num: u64) -> Self {
        BlockId { file_name, blk_num }
    }

    pub fn file_name(&mut self) -> &String {
        &self.file_name
    }

    pub fn blk_num(&mut self) -> u64 {
        self.blk_num
    }

    pub fn equals(self, blk: &mut BlockId) -> bool {
        self.file_name.eq(blk.file_name()) && self.blk_num.eq(&blk.blk_num)
    }
}
