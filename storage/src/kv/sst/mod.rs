use std::ops::{Bound, RangeBounds};

use bytes::Bytes;

use crate::backend::Backend;
use crate::err::*;
use crate::iterator::LevelIter;
use crate::{
    cache::Cache,
    iterator::{MergedIter, ScanIter},
    key::{InternalKey, Value},
    snapshot::Snapshot,
    Config,
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
        backend: &Backend,
        lifetime: &Lifetime<'b>,
    ) -> Result<(InternalKey, Value)> {
        for level in 0..MAX_LEVEL {
            let runs = self.version.level_n(level);
            for run in runs.iter().rev() {
                // find key
                if let Some(fs) = run.binary_find_file(&key[..]) {
                    if fs.meta().min_ver > self.snapshot.sequence() {
                        continue;
                    }
                    let sst_reader = self.cache.get_opened_sst(config, fs.meta().number, backend);

                    match sst_reader.get(opt, key.clone(), lifetime) {
                        Ok(val) => return Ok(val),
                        Err(e) => {
                            if StorageError::KeyNotExist == e {
                                // search next run
                                continue;
                            } else {
                                return Err(e);
                            }
                        }
                    }
                }
            }
            // search next level
        }

        Err(StorageError::KeyNotExist)
    }

    pub fn scan<'b, R: RangeBounds<bytes::Bytes> + Clone>(
        &self,
        opt: &crate::GetOption,
        config: &Config,
        range: R,
        backend: &Backend,
        lifetime: &Lifetime<'b>,
    ) -> ScanIter<'b, (InternalKey, Value)> {
        let mut iters = Vec::new();
        for level in 0..MAX_LEVEL {
            let runs = self.version.level_n(level);
            for run in runs.iter().rev() {
                // find key
                let mut file_iters = Vec::new();
                for fs in run.files() {
                    if fs.meta().min_ver > self.snapshot.sequence() {
                        continue;
                    }
                    let sst_reader = self.cache.get_opened_sst(config, fs.meta().number, backend);

                    file_iters.push(sst_reader.scan(
                        opt,
                        range.start_bound().cloned(),
                        range.end_bound().cloned(),
                        lifetime,
                    ));
                }

                if file_iters.len() > 0 {
                    iters.push(ScanIter::new(LevelIter::new(file_iters)));
                }
            }
        }

        ScanIter::new(MergedIter::new(iters))
    }
}
