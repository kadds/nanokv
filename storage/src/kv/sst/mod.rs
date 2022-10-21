use std::fs;

use crate::snapshot::Snapshot;

use super::{
    manifest::{FileMetaData, Version, VersionRef},
    KVReader, KvEntry,
};

pub mod raw_sst;

pub trait SSTWriter {
    fn write<'a, I>(&'a mut self, level: u32, seq: u64, iter: I) -> FileMetaData
    where
        I: Iterator<Item = &'a KvEntry>;
}

pub fn sst_name(base: &str, level: u32, seq: u64) -> String {
    let parent = format!("{}/sst/{}", base, level);
    let _ = fs::create_dir_all(parent);
    format!("{}/sst/{}/{}.sst", base, level, seq)
}

struct MergedSSTReader {}

impl MergedSSTReader {
    pub fn new() -> Self {
        Self {}
    }
}

impl<'a> KVReader<'a> for MergedSSTReader {
    fn get<K: Into<bytes::Bytes>>(&self, opt: &super::GetOption, key: K) -> Option<crate::Value> {
        todo!()
    }

    fn scan<K: Into<bytes::Bytes>>(
        &'a self,
        opt: &super::GetOption,
        beg: K,
        end: K,
    ) -> Box<dyn crate::KvIterator<Item = (bytes::Bytes, crate::Value)> + 'a> {
        todo!()
    }
}

// pub struct LeveledSSTReader {
//     level: u32,
//     inner: MergedSSTReader,
// }

// impl LeveledSSTReader {
//     pub fn new(level: u32) -> Self {
//         Self {
//             level,
//             inner: MultiSSTReader::new(),
//         }
//     }
// }

// impl<'a> KVReader<'a> for LeveledSSTReader {
//     fn get_ver(&self, key: &str, ver: u64) -> Option<crate::value::Value> {
//         todo!()
//     }

//     fn get(&self, key: &str) -> Option<crate::value::Value> {
//         todo!()
//     }

//     fn scan(
//         &'a self,
//         beg: &str,
//         end: &str,
//     ) -> Box<dyn Iterator<Item = (&'a str, crate::value::Value)> + 'a> {
//         todo!()
//     }

//     fn iter(&'a self) -> Box<dyn Iterator<Item = (&'a str, crate::value::Value)> + 'a> {
//         todo!()
//     }
// }

pub struct SnapshotTable {
    snapshot: Snapshot,
    // L0 reader
}

impl From<Snapshot> for SnapshotTable {
    fn from(v: Snapshot) -> Self {
        Self { snapshot: v }
    }
}

impl<'a> KVReader<'a> for SnapshotTable {
    fn get<K: Into<bytes::Bytes>>(&self, opt: &super::GetOption, key: K) -> Option<crate::Value> {
        todo!()
    }

    fn scan<K: Into<bytes::Bytes>>(
        &'a self,
        opt: &super::GetOption,
        beg: K,
        end: K,
    ) -> Box<dyn crate::KvIterator<Item = (bytes::Bytes, crate::Value)> + 'a> {
        todo!()
    }
}
