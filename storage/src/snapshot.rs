use crate::kv::manifest::VersionRef;

#[derive(Debug, Default)]
pub struct Snapshot {
    ver: u64,
    ver_ref: Option<VersionRef>,
}

impl Snapshot {
    pub fn new(ver: u64, ver_ref: VersionRef) -> Self {
        Self {
            ver,
            ver_ref: Some(ver_ref),
        }
    }
    pub fn version(&self) -> u64 {
        self.ver
    }
    pub fn version_ref(&self) -> Option<VersionRef>  {
        self.ver_ref.clone()
    }
}

impl From<u64> for Snapshot {
    fn from(ver: u64) -> Self {
        Self { ver, ver_ref: None }
    }
}
