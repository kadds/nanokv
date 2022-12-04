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
    superversion::Lifetime,
    KvEntry,
};

pub mod format;
pub mod raw_sst;

pub trait SSTWriter {
    fn write<I>(&mut self, level: u32, seq: u64, iter: I) -> FileMetaData
    where
        I: Iterator<Item = KvEntry>;
}

pub trait SSTReader {
    fn get<'a>(
        &self,
        opt: &crate::GetOption,
        key: bytes::Bytes,
        lifetime: &Lifetime<'a>,
    ) -> Option<crate::Value>;
    fn scan<'a>(
        &self,
        opt: &crate::GetOption,
        beg: Bound<bytes::Bytes>,
        end: Bound<bytes::Bytes>,
        lifetime: &Lifetime<'a>,
    ) -> ScanIter<'a, (bytes::Bytes, crate::Value)>;
    fn raw_scan<'a>(&self, lifetime: &Lifetime<'a>) -> ScanIter<'a, KvEntry>;
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
    pub fn get<'b>(
        &self,
        opt: &crate::GetOption,
        key: bytes::Bytes,
        lifetime: &Lifetime<'b>,
    ) -> Option<crate::Value> {
        for level in 0..MAX_LEVEL {
            let iter = self
                .version
                .level_n(level)
                .filter(|fs| fs.meta().min_ver <= self.snapshot.version())
                .map(|fs| self.cache.get_opened_sst(fs.meta().level, fs.meta().seq));

            for file_reader in iter {
                let value = file_reader.get(opt, key.clone(), lifetime);
                if let Some(value) = value {
                    return Some(value);
                }
            }
        }

        None
    }

    pub fn scan<'b, R: RangeBounds<bytes::Bytes> + Clone>(
        &self,
        opt: &crate::GetOption,
        range: R,
        lifetime: &Lifetime<'b>,
    ) -> ScanIter<'b, (bytes::Bytes, crate::Value)> {
        let mut iters = Vec::new();
        for level in 0..MAX_LEVEL {
            let iter = self
                .version
                .level_n(level)
                .filter(|fs| fs.meta().min_ver <= self.snapshot.version())
                .map(|fs| self.cache.get_opened_sst(fs.meta().level, fs.meta().seq));

            for file_reader in iter {
                iters.push(file_reader.scan(
                    opt,
                    range.start_bound().cloned(),
                    range.end_bound().cloned(),
                    &lifetime,
                ));
            }
        }

        ScanIter::new(MergedIter::new(iters))
    }
}
