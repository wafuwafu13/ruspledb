#[derive(Debug)]
pub struct RID {
    blk_num: u64,
    slot: u64,
}

impl RID {
    pub fn new(blk_num: u64, slot: u64) -> Self {
        RID { blk_num, slot }
    }
}
