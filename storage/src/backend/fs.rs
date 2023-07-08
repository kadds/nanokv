use std::{
    fmt::Debug,
    io::{Read, Seek, Write},
    path::Path,
};

use crate::err::Result;

pub struct UsageTotal {
    pub usage: u64,
    pub total_limit: Option<u64>,
}

pub trait ReadablePersist: Read + Seek + Send + Sync {
    fn addr(&self) -> Result<&[u8]>;
    // fn read_slice<'a>(&'a mut self, range: Range<usize>, buffer: &'a mut Vec<u8>) -> Result<&'a [u8]>;
}

pub trait WriteablePersist: Write + Send {
    fn truncate(&mut self, size: u64) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
}

pub struct PersistFeature {
    pub mmap_supported: bool,
    pub seek_supported: bool,
}

pub trait PersistBackend: Send + Sync + Debug {
    fn open(&self, path: &Path, enable_mmap: bool) -> Result<Box<dyn ReadablePersist>>;
    fn get_feature(&self) -> PersistFeature;
    fn create(&self, path: &Path, truncate: Option<u64>) -> Result<Box<dyn WriteablePersist>>;
    fn remove(&self, path: &Path) -> Result<()>;
    fn usage_total(&self) -> UsageTotal;
    fn make_sure_dir(&self, path: &Path) -> Result<()>;
    fn rename(&self, src: &Path, dst: &Path) -> Result<()>;
}

pub mod local;
pub mod memory;
