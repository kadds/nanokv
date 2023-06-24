#[derive(Debug, Default, Clone)]
pub struct Snapshot {
    seq: u64,
}

impl Snapshot {
    pub fn new(seq: u64) -> Self {
        Self { seq }
    }
    pub fn sequence(&self) -> u64 {
        self.seq
    }
}

impl From<u64> for Snapshot {
    fn from(seq: u64) -> Self {
        Self { seq }
    }
}
