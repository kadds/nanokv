#[derive(Debug, Default, Clone)]
pub struct Snapshot {
    ver: u64,
}

impl Snapshot {
    pub fn new(ver: u64) -> Self {
        Self { ver }
    }
    pub fn version(&self) -> u64 {
        self.ver
    }
}

impl From<u64> for Snapshot {
    fn from(ver: u64) -> Self {
        Self { ver }
    }
}
