use std::ops::{Bound, RangeBounds};

use bytes::Bytes;

use crate::err::*;
use crate::{
    cache::Cache,
    iterator::{MergedIter, ScanIter},
    key::{InternalKey, Value},
    snapshot::Snapshot,
    Config, ConfigRef,
};

use super::{
    manifest::{FileMetaData, VersionRef, MAX_LEVEL},
    superversion::Lifetime,
};

pub mod format;
pub mod raw_sst;

pub trait SSTWriter {
    fn write<I>(&mut self, level: u32, number: u64, iter: I) -> Result<FileMetaData>
    where
        I: Iterator<Item = (InternalKey, Value)>;
}

pub trait SSTReader {
    fn get<'a>(
        &self,
        opt: &crate::GetOption,
        key: Bytes,
        lifetime: &Lifetime<'a>,
    ) -> Result<(InternalKey, Value)>;
    fn scan<'a>(
        &self,
        opt: &crate::GetOption,
        beg: Bound<bytes::Bytes>,
        end: Bound<bytes::Bytes>,
        lifetime: &Lifetime<'a>,
    ) -> ScanIter<'a, (InternalKey, Value)>;
    fn raw_scan<'a>(&self, lifetime: &Lifetime<'a>) -> ScanIter<'a, (InternalKey, Value)>;
}

pub struct SnapshotTable<'a> {
    snapshot: Snapshot,
    version: VersionRef,
    cache: &'a Cache,
}

impl<'a> SnapshotTable<'a> {
    pub fn new(snapshot: Snapshot, version: VersionRef, cache: &'a Cache) -> Box<Self> {
        Self {
            snapshot,
            version,
            cache,
        }
        .into()
    }
}

impl<'a> SnapshotTable<'a> {
    pub fn get<'b>(
        &self,
        opt: &crate::GetOption,
        config: &Config,
        key: Bytes,
        lifetime: &Lifetime<'b>,
    ) -> Result<(InternalKey, Value)> {
        for level in 0..MAX_LEVEL {
            let iter = self
                .version
                .level_n(level)
                .filter(|fs| fs.meta().min_ver <= self.snapshot.sequence())
                .map(|fs| {
                    self.cache
                        .get_opened_sst(config, fs.meta().level, fs.meta().number)
                });

            for file_reader in iter {
                match file_reader.get(opt, key.clone(), lifetime) {
                    Ok(val) => return Ok(val),
                    Err(e) => {
                        if StorageError::KeyNotExist == e {
                            return Err(e);
                        }
                    }
                }
            }
        }

        Err(StorageError::KeyNotExist)
    }

    pub fn scan<'b, R: RangeBounds<bytes::Bytes> + Clone>(
        &self,
        opt: &crate::GetOption,
        config: &Config,
        range: R,
        lifetime: &Lifetime<'b>,
    ) -> ScanIter<'b, (InternalKey, Value)> {
        let mut iters = Vec::new();
        for level in 0..MAX_LEVEL {
            let iter = self
                .version
                .level_n(level)
                .filter(|fs| fs.meta().min_ver <= self.snapshot.sequence())
                .map(|fs| {
                    self.cache
                        .get_opened_sst(config, fs.meta().level, fs.meta().number)
                });

            for file_reader in iter {
                iters.push(file_reader.scan(
                    opt,
                    range.start_bound().cloned(),
                    range.end_bound().cloned(),
                    lifetime,
                ));
            }
        }

        ScanIter::new(MergedIter::new(iters))
    }
}
