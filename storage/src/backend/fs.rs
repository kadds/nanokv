use std::{
    fmt::Debug,
    io::{Read, Seek, Write},
    marker::PhantomData,
    path::Path,
};

use crate::err::Result;
use integer_encoding::{VarInt, VarIntReader};
use positioned_io::ReadAt;

pub struct UsageTotal {
    pub usage: u64,
    pub total_limit: Option<u64>,
}

pub trait ReadablePersist: ReadAt + Send + Sync {
    fn addr(&self) -> Result<&[u8]>;
    fn size(&self) -> u64;
}

pub struct ExtReader<'a> {
    pub(super) r: &'a dyn ReadablePersist,
    pub(super) offset: u64,
    pub(super) size: u64,
}

impl<'a> ExtReader<'a> {
    pub fn new(r: &'a dyn ReadablePersist, offset: u64, size: u64) -> Self {
        Self { r, offset, size }
    }
}

impl<'a> Read for ExtReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.r.read_at(self.offset, buf)?;
        self.offset += n as u64;
        Ok(n)
    }
}

impl<'a> Seek for ExtReader<'a> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match pos {
            std::io::SeekFrom::Start(o) => self.offset = o,
            std::io::SeekFrom::End(i) => {
                if i >= 0 {
                    self.offset = self.size + (i as u64);
                } else {
                    self.offset = self.size - ((-i) as u64);
                }
            }
            std::io::SeekFrom::Current(i) => {
                if i >= 0 {
                    self.offset += i as u64;
                } else {
                    self.offset -= (-i) as u64;
                }
            }
        };
        if self.offset >= self.size {
            self.offset = self.size;
        }
        Ok(self.offset)
    }
}

pub trait WriteablePersist: Write + Send {
    fn truncate(&mut self, size: u64) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
    fn delete(&mut self) -> Result<()>;
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
