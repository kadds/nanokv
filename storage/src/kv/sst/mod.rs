use std::{
    fs,
    ops::{Bound, RangeBounds},
    path::PathBuf,
};

use crate::{
    cache::Cache,
    iterator::{MergedIter, ScanIter},
    snapshot::Snapshot,
    ConfigRef,
};

use super::{
    manifest::{FileMetaData, VersionRef, MAX_LEVEL},
    KvEntry,
};

pub mod format;
pub mod raw_sst;

pub trait SSTWriter {
    fn write<'a, I>(&'a mut self, level: u32, seq: u64, iter: I) -> FileMetaData
    where
        I: Iterator<Item = &'a KvEntry>;
}

pub(crate) fn prepare_sst_dir(base: &str) {
    for level in 0..MAX_LEVEL {
        let parent = format!("{}/sst/{}", base, level);
        fs::create_dir_all(parent).unwrap();
    }
}

pub fn sst_name(base: &str, level: u32, seq: u64) -> PathBuf {
    format!("{}/sst/{}/{}.sst", base, level, seq).into()
}

pub trait SSTReader {
    fn get(&self, opt: &crate::GetOption, key: bytes::Bytes) -> Option<crate::Value>;
    fn scan(
        &self,
        opt: &crate::GetOption,
        beg: Bound<bytes::Bytes>,
        end: Bound<bytes::Bytes>,
    ) -> ScanIter<(bytes::Bytes, crate::Value)>;
}

pub struct SnapshotTable<'a> {
    snapshot: Snapshot,
    version: VersionRef,
    #[allow(unused)]
    config: ConfigRef,
    cache: &'a Cache,
}

impl<'a> SnapshotTable<'a> {
    pub fn new(
        snapshot: Snapshot,
        version: VersionRef,
        config: ConfigRef,
        cache: &'a Cache,
    ) -> Box<Self> {
        Self {
            snapshot,
            version,
            config,
            cache,
        }
        .into()
    }
}

impl<'a> SnapshotTable<'a> {
    pub fn get(&self, opt: &crate::GetOption, key: bytes::Bytes) -> Option<crate::Value> {
        // search from L0

        let iter = self
            .version
            .level_n(0)
            .rev()
            .filter(|file| file.min_ver <= self.snapshot.version())
            .map(|file| self.cache.get_opened_sst(file.level, file.seq));

        for file_reader in iter {
            let value = file_reader.get(opt, key.clone());
            if let Some(value) = value {
                return Some(value);
            }
        }

        None
    }

    pub fn scan(
        &self,
        opt: &crate::GetOption,
        range: impl RangeBounds<bytes::Bytes>,
    ) -> ScanIter<(bytes::Bytes, crate::Value)> {
        // scan from L0

        let iter = self
            .version
            .level_n(0)
            .filter(|file| file.min_ver <= self.snapshot.version())
            .map(|file| self.cache.get_opened_sst(file.level, file.seq));

        let mut iters = Vec::new();
        for file_reader in iter {
            iters.push(file_reader.scan(
                opt,
                range.start_bound().cloned(),
                range.end_bound().cloned(),
            ));
        }
        ScanIter::new(MergedIter::new(iters))
    }
}
