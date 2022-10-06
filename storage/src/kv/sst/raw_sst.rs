use crate::kv::KVReader;
use crate::value::Value;
use std::fs::File;

use super::SSTWriter;

pub struct SSTMeta {}

pub struct SSTSegment {}

enum BlockTy {
    SortedIndex,
    BloomFilter,
}

struct Block {
    ty: BlockTy,
    offset: u64,
    length: u64,
}

pub struct SSTData {
    version: u64,
}

pub struct SSTIndex {}

pub struct RawSSTReader {
    mmap: memmap2::Mmap,
    file: File,
}

impl RawSSTReader {
    pub fn new(name: &str) -> Self {
        let file = File::open(name).unwrap();
        let mmap = unsafe { memmap2::Mmap::map(&file).unwrap() };
        Self { mmap, file }
    }
}

impl<'a> KVReader<'a> for RawSSTReader {
    fn get_ver(&self, key: &str, ver: u64) -> Option<Value> {
        todo!()
    }

    fn get(&self, key: &str) -> Option<Value> {
        todo!()
    }

    fn scan(&'a self, beg: &str, end: &str) -> Box<dyn Iterator<Item = (&'a str, Value)> + 'a> {
        todo!()
    }

    fn iter(&'a self) -> Box<dyn Iterator<Item = (&'a str, Value)> + 'a> {
        todo!()
    }
}

pub struct RawSSTWriter {
    file: File,
}

impl RawSSTWriter {
    pub fn new(name: &str) -> Self {
        let file = File::create(name).unwrap();
        Self { file }
    }
}

impl SSTWriter for RawSSTWriter {
    fn write_level_x(
        &mut self,
        level: u32,
        entry_iter: Box<dyn Iterator<Item = crate::kv::KvEntry>>,
    ) {
        let mut min_key = String::new();
        let mut max_key = String::new();
        for entry in entry_iter {}
    }
}
