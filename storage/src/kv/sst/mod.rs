use super::{KVReader, KvEntry};

pub mod raw_sst;

struct SSTMeta {
    min_key: String,
    max_key: String,
    keys: u32,
}

pub trait SSTWriter {
    fn write<'a, I>(&'a mut self, level: u32, iter: I)
    where
        I: Iterator<Item = &'a KvEntry>;
}

pub fn sst_name(base: &str, seq: u64) -> String {
    format!("{}/sst-{}", base, seq)
}

struct MultiSSTReader {}

impl MultiSSTReader {
    pub fn new() -> Self {
        Self {}
    }
}

impl<'a> KVReader<'a> for MultiSSTReader {
    fn get_ver(&self, key: &str, ver: u64) -> Option<crate::value::Value> {
        todo!()
    }

    fn get(&self, key: &str) -> Option<crate::value::Value> {
        todo!()
    }

    fn scan(
        &'a self,
        beg: &str,
        end: &str,
    ) -> Box<dyn Iterator<Item = (&'a str, crate::value::Value)> + 'a> {
        todo!()
    }

    fn iter(&'a self) -> Box<dyn Iterator<Item = (&'a str, crate::value::Value)> + 'a> {
        todo!()
    }
}

pub struct LeveledSSTReader {
    level: u32,
    inner: MultiSSTReader,
}

impl LeveledSSTReader {
    pub fn new(level: u32) -> Self {
        Self {
            level,
            inner: MultiSSTReader::new(),
        }
    }
}

impl<'a> KVReader<'a> for LeveledSSTReader {
    fn get_ver(&self, key: &str, ver: u64) -> Option<crate::value::Value> {
        todo!()
    }

    fn get(&self, key: &str) -> Option<crate::value::Value> {
        todo!()
    }

    fn scan(
        &'a self,
        beg: &str,
        end: &str,
    ) -> Box<dyn Iterator<Item = (&'a str, crate::value::Value)> + 'a> {
        todo!()
    }

    fn iter(&'a self) -> Box<dyn Iterator<Item = (&'a str, crate::value::Value)> + 'a> {
        todo!()
    }
}
