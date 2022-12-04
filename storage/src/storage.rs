use std::{
    mem::swap,
    ops::RangeBounds,
    sync::{
        atomic::{AtomicPtr, AtomicU64, Ordering},
        mpsc, Arc, Mutex,
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use bytes::Bytes;
use log::info;

use crate::{
    cache::Cache,
    compaction::{major::MajorSerializer, minor::MinorSerializer},
    iterator::{MergedIter, ScanIter},
    kv::{
        imemtable::Imemtables,
        manifest::{FileMetaData, VersionEdit},
        sst::SnapshotTable,
        superversion::SuperVersion,
        ColumnFamilyTables,
    },
    log::{wal::INVALID_SEQ, LogReplayer},
    snapshot::Snapshot,
    util::fname,
    GetOption, WriteOption,
};
use crate::{
    compaction::CompactSerializer,
    kv::{
        manifest::Manifest, Imemtable, KvEntry, KvEntryLogSerializer, Memtable, SetError, SetResult,
    },
    log::LogWriter,
    value::Value,
    ConfigRef,
};

type Wal = LogWriter<KvEntry, KvEntryLogSerializer>;

struct StorageInner {
    tables: ArcSwap<ColumnFamilyTables>,
    manifest: Arc<Manifest>,
    wal: Option<Wal>,
    lock: Mutex<()>,
    step_version: AtomicU64,
    super_version: ArcSwap<SuperVersion>,
}

impl StorageInner {
    pub fn new(seq: u64, config: ConfigRef, manifest: Arc<Manifest>, force_nowal: bool) -> Self {
        let log_serializer = KvEntryLogSerializer::default();
        let wal = if config.no_wal {
            None
        } else {
            Some(if force_nowal {
                LogWriter::new(
                    config,
                    log_serializer,
                    INVALID_SEQ,
                    Box::new(fname::wal_name),
                )
            } else {
                LogWriter::new(config, log_serializer, seq, Box::new(fname::wal_name))
            })
        };
        let tables = ArcSwap::new(Arc::new(ColumnFamilyTables {
            memtable: Arc::new(Memtable::new(seq)),
            imemtables: Imemtables::new(),
        }));
        let super_version = ArcSwap::new(Arc::new(SuperVersion {
            cf_tables: tables.load().clone(),
            sst_version: manifest.current(),
            step_version: 0,
        }));
        Self {
            tables,
            manifest,
            wal,
            lock: Mutex::new(()),
            step_version: AtomicU64::new(0),
            super_version,
        }
    }

    pub fn super_version(&self) -> Arc<SuperVersion> {
        loop {
            let step = self.step_version.load(Ordering::SeqCst);
            let sv = self.super_version.load();
            if sv.step_version != step {
                // slow path
                let _lock = self.lock.lock().unwrap();
            } else {
                break sv.clone();
            }
        }
    }

    pub fn modify_super_version<F: FnOnce(&SuperVersion) -> SuperVersion>(&self, f: F) {
        let _lock = self.lock.lock().unwrap();
        let sv = self.super_version.load();
        let val = self.step_version.fetch_add(1, Ordering::SeqCst);
        let sv = f(&*sv.as_ref());
        assert!(val + 1 == sv.step_version);

        self.tables.store(sv.cf_tables.clone());
        self.super_version.store(Arc::new(sv));
    }
}

pub struct Storage {
    inner: Arc<StorageInner>,

    serializer: Arc<MinorSerializer>,
    major_serializer: Arc<MajorSerializer>,

    config: ConfigRef,
    cache: Cache,
}

impl Storage {
    pub(crate) fn new(config: ConfigRef, manifest: Arc<Manifest>) -> Self {
        let mut restore_seq = manifest.last_sst_sequence();
        let (seq, need_restore) = if restore_seq > 0 {
            restore_seq -= 1;
            (restore_seq - 1, true)
        } else {
            let seq = manifest.allocate_sst_sequence();
            (seq, false)
        };

        let inner = Arc::new(StorageInner::new(
            seq,
            config,
            manifest.clone(),
            need_restore,
        ));

        let inner2 = inner.clone();
        let serializer =
            MinorSerializer::new(config.clone(), inner.manifest.clone(), move |meta| {
                let seq = meta.seq;
                inner2.manifest.add_sst_with(meta, true, |current| {
                    inner2.modify_super_version(move |sv| SuperVersion {
                        cf_tables: Arc::new(ColumnFamilyTables {
                            memtable: sv.cf_tables.memtable.clone(),
                            imemtables: sv.cf_tables.imemtables.remove(seq),
                        }),
                        sst_version: current,
                        step_version: sv.step_version + 1,
                    });
                });
            });

        let inner2 = inner.clone();
        let major_serializer = MajorSerializer::new(
            config,
            inner.manifest.clone(),
            move |additional, removal| {
                let mut vec = Vec::new();
                for meta in additional {
                    vec.push(VersionEdit::SSTAppended(meta));
                }
                for seq in removal {
                    vec.push(VersionEdit::SSTRemove(seq));
                }
                inner2.manifest.modify_with(vec, |current| {
                    inner2.modify_super_version(move |sv| SuperVersion {
                        cf_tables: sv.cf_tables.clone(),
                        sst_version: current,
                        step_version: sv.step_version + 1,
                    });
                });
            },
        );

        let mut this = Self {
            inner,
            serializer,
            major_serializer,
            cache: Cache::new(config.clone()),
            config,
        };
        if need_restore {
            this.restore();
        }
        this
    }
}

impl Storage {
    pub fn get_ex<K: Into<Bytes>>(
        &self,
        opt: &GetOption,
        key: K,
        super_version: &SuperVersion,
        snapshot: Snapshot,
    ) -> Option<Value> {
        let lifetime = super_version.lifetime();

        let key = key.into();
        // query from memtable
        if let Some(value) = super_version
            .cf_tables
            .memtable
            .get(opt, key.clone(), &lifetime)
        {
            if opt.debug() {
                info!("find key {:?} in memtable", key);
            }
            if value.deleted() {
                return None;
            }
            return Some(value);
        }

        // query from imemetable
        if let Some(value) = super_version
            .cf_tables
            .imemtables
            .get(opt, key.clone(), &lifetime)
        {
            if opt.debug() {
                info!("find key {:?} in imemtables", key);
            }

            if value.deleted() {
                return None;
            }
            return Some(value);
        }

        if let Some(value) = SnapshotTable::new(
            snapshot,
            super_version.sst_version.clone(),
            self.config,
            &self.cache,
        )
        .get(opt, key.clone(), &lifetime)
        {
            if opt.debug() {
                info!("find key {:?} in sst", key);
            }
            if value.deleted() {
                return None;
            }
            return Some(value);
        }

        None
    }

    pub fn get<K: Into<Bytes>>(&self, opt: &GetOption, key: K) -> Option<Value> {
        let inner = self.inner.as_ref();
        let super_version = self.super_version();
        let snapshot_guard = inner.manifest.new_snapshot();
        let snapshot = snapshot_guard.get();
        self.get_ex(opt, key, &super_version, snapshot)
    }

    pub fn scan<'a, R: RangeBounds<Bytes> + Clone>(
        &'a self,
        opt: &GetOption,
        range: R,
        super_version: &'a SuperVersion,
    ) -> ScanIter<'a, (Bytes, Value)> {
        let inner = self.inner.as_ref();
        let snapshot_guard = inner.manifest.new_snapshot();
        let snapshot = snapshot_guard.get();
        self.scan_ex(opt, range, super_version, snapshot)
    }

    pub fn scan_ex<'a, R: RangeBounds<Bytes> + Clone>(
        &self,
        opt: &GetOption,
        range: R,
        super_version: &'a SuperVersion,
        snapshot: Snapshot,
    ) -> ScanIter<'a, (Bytes, Value)> {
        let mut iters = Vec::new();
        let lifetime = super_version.lifetime();
        let inner = self.inner.as_ref();
        let tables = inner.tables.load();

        iters.push(tables.memtable.scan(opt, range.clone(), &lifetime));
        iters.push(tables.imemtables.scan(opt, range.clone(), &lifetime));

        iters.push(
            SnapshotTable::new(
                snapshot,
                super_version.sst_version.clone(),
                self.config,
                &self.cache,
            )
            .scan(opt, range, &lifetime),
        );

        ScanIter::<'a, (Bytes, Value)>::new(
            MergedIter::new(iters).filter(|(_, value)| !value.deleted()),
        )
    }

    pub fn set<K: Into<Bytes>>(
        &mut self,
        opt: &WriteOption,
        key: K,
        value: Bytes,
    ) -> SetResult<u64> {
        self.set_by(opt, key, value, None, None)
    }

    pub fn set_by<K: Into<Bytes>>(
        &mut self,
        opt: &WriteOption,
        key: K,
        value: Bytes,
        ver: Option<u64>,
        ttl: Option<Duration>,
    ) -> SetResult<u64> {
        if value.len() > 1024 * 1024 * 10 {
            // 10MB value
            return Err(SetError::ValueTooLarge);
        }
        let key = key.into();

        if let Some(ver) = ver {
            let latest_ver = self
                .get(&GetOption::default(), key.clone())
                .ok_or(SetError::KeyNotExist)?
                .version();
            if latest_ver != ver {
                return Err(SetError::VersionNotMatch(latest_ver));
            }
        };

        let inner = self.inner.as_ref();
        let tables = inner.tables.load();

        let cur_ver = inner.manifest.allocate_version(1);
        let entry = KvEntry::new(key.clone(), value, ttl.map(|d| d.as_secs()), cur_ver);

        if let Some(wal) = &inner.wal {
            wal.append(&entry);
            if opt.fsync() {
                wal.sync();
            }
        }

        tables.memtable.set(opt, entry)?;
        if tables.memtable.full() {
            if opt.debug() {
                info!("{:?} need flush", key);
            }
            self.flush_memtable();
        }
        self.major_serializer.notify(&inner.super_version());
        Ok(cur_ver)
    }

    pub fn del<S: Into<String>>(&mut self, opt: &WriteOption, key: S) -> SetResult<u64> {
        let inner = self.inner.as_ref();
        let cur_ver = inner.manifest.allocate_version(1);

        let entry = KvEntry::new_del(key.into(), cur_ver);
        let inner = self.inner.as_ref();
        let tables = inner.tables.load();
        tables.memtable.set(opt, entry)?;

        Ok(cur_ver)
    }

    pub fn super_version(&self) -> Arc<SuperVersion> {
        self.inner.super_version()
    }
}

impl Storage {
    fn restore(&mut self) {
        let log_serializer = KvEntryLogSerializer::default();
        let tables = self.inner.tables.load();
        LogReplayer::new(
            log_serializer,
            fname::wal_name(self.config, tables.memtable.seq()),
        )
        .execute(0, |_, entry| {
            tables.memtable.set(&WriteOption::default(), entry).unwrap()
        });

        info!("wal restore from {}", tables.memtable.seq());
        self.flush_memtable();
    }

    /// flush memtable into imemtable
    pub(crate) fn flush_memtable(&self) {
        let inner = self.inner.as_ref();
        let tables = inner.tables.load();
        if !tables.memtable.is_empty() {
            let new_seq = inner.manifest.allocate_sst_sequence();

            let imemtable = Arc::new(Imemtable::new(&tables.memtable, u64::MAX));
            let target = imemtable.clone();
            let cf_tables = Arc::new(ColumnFamilyTables {
                memtable: Arc::new(Memtable::new(new_seq)),
                imemtables: tables.imemtables.push(imemtable.clone()),
            });
            let t = cf_tables.clone();

            inner.modify_super_version(move |sv| SuperVersion {
                cf_tables: t,
                sst_version: sv.sst_version.clone(),
                step_version: sv.step_version + 1,
            });

            drop(tables);

            self.serializer.compact_async(target);
        }
    }

    pub(crate) fn flush_wait_imemtables(&mut self) {
        let inner = self.inner.as_ref();
        loop {
            if inner.tables.load().imemtables.empty() {
                break;
            }
            info!("wait imemtable flush");
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    fn shutdown(&mut self) {
        info!("shutdown storage");

        self.flush_memtable();
        self.flush_wait_imemtables();

        self.serializer.stop();
        self.inner.manifest.flush();
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        self.shutdown();
    }
}
