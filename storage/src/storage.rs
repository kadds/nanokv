use std::{
    ops::RangeBounds,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use bytes::Bytes;
use log::{error, info};
use ouroboros::self_referencing;

use crate::{
    backend::Backend,
    cache::Cache,
    compaction::{major::MajorCompactionTaskPool, minor::MinorCompactionTaskPool},
    err::{Result, StorageError},
    iterator::{KvIteratorItem, MergedIter, ScanIter},
    key::{BatchLogSerializer, KeyType, Value, WriteBatch, WriteBatchBuilder},
    kv::{
        manifest::VersionEdit, sst::SnapshotTable, superversion::SuperVersion, ColumnFamilyTables,
        Imemtables,
    },
    log::LogReplayer,
    snapshot::Snapshot,
    util::fname::{manifest_name, sst_name, wal_name},
    Config, GetOption, WriteOption,
};
use crate::{
    compaction::CompactSerializer,
    kv::{manifest::Manifest, Memtable},
    log::LogWriter,
};

#[self_referencing]
struct StorageInfoInner {
    config: Config,
    backend: Backend,
    #[borrows(config, backend)]
    #[not_covariant]
    manifest: Manifest<'this>,
    #[borrows(config, backend)]
    #[not_covariant]
    wal: Option<LogWriter<'this, BatchLogSerializer>>,
}

struct StorageInner {
    info: StorageInfoInner,
    tables: ArcSwap<ColumnFamilyTables>,
    lock: Mutex<()>,
    step_version: AtomicU64,
    super_version: ArcSwap<SuperVersion>,
    cache: Cache,
}

impl StorageInner {
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
        let sv = f(sv.as_ref());
        assert!(val + 1 == sv.step_version);

        self.tables.store(sv.cf_tables.clone());
        self.super_version.store(Arc::new(sv));
    }

    pub fn modify_super_version_opt<F: FnOnce(&SuperVersion) -> Option<SuperVersion>>(&self, f: F) {
        let _lock = self.lock.lock().unwrap();
        let sv = self.super_version.load();
        let val = self.step_version.fetch_add(1, Ordering::SeqCst);
        let sv = match f(sv.as_ref()) {
            Some(v) => v,
            None => {
                return;
            }
        };
        assert!(val + 1 == sv.step_version);

        self.tables.store(sv.cf_tables.clone());
        self.super_version.store(Arc::new(sv));
    }
}

pub struct Storage {
    inner: Arc<StorageInner>,

    minor_pool: Arc<MinorCompactionTaskPool>,
    major_pool: Arc<MajorCompactionTaskPool>,
}

impl Storage {
    pub fn clear(config: Config) {}

    pub fn new(config: Config, backend: Backend) -> Self {
        backend
            .fs
            .make_sure_dir(&config.path.to_path_buf())
            .unwrap();
        backend
            .fs
            .make_sure_dir(&sst_name(&config, 0).parent().unwrap().to_path_buf())
            .unwrap();
        backend
            .fs
            .make_sure_dir(&manifest_name(&config, 0).parent().unwrap().to_path_buf())
            .unwrap();
        backend
            .fs
            .make_sure_dir(&wal_name(&config, 0).parent().unwrap().to_path_buf())
            .unwrap();
        // prepare dir
        // fname::make_sure(conf);
        // fs::File::create(PathBuf::from(&conf.path).join("nanokv")).unwrap();

        // init manifest

        let info = StorageInfoInner::new(
            config.clone(),
            backend,
            |c, b| Manifest::new(&c, &b),
            |c, b| {
                if c.no_wal {
                    None
                } else {
                    Some(LogWriter::new(&b, BatchLogSerializer))
                }
            },
        );
        let (number, need_restore, sst_version) = info.with_manifest(|manifest| {
            let mut restore_number = manifest.last_sst_number();
            if restore_number > 0 {
                restore_number -= 1;
                (restore_number, true, manifest.current())
            } else {
                let number = manifest.allocate_sst_number();
                (number, false, manifest.current())
            }
        });

        let tables = ArcSwap::new(Arc::new(ColumnFamilyTables {
            memtable: Arc::new(Memtable::new(number)),
            imemtables: Imemtables::default(),
        }));

        let super_version = ArcSwap::new(Arc::new(SuperVersion {
            cf_tables: tables.load().clone(),
            sst_version,
            step_version: 0,
        }));
        let inner = Arc::new(StorageInner {
            tables,
            super_version,
            info,
            lock: Mutex::new(()),
            step_version: 0.into(),
            cache: Cache::new(),
        });
        // init compaction thread pool

        let inner2 = inner.clone();
        let backend = unsafe { std::mem::transmute(inner.info.borrow_backend()) };
        let minor_pool = MinorCompactionTaskPool::new(&config, backend, move |meta| {
            inner2.info.with_manifest(|m| {
                let number = meta.number;
                m.add_sst_with(meta, true, |current| {
                    inner2.modify_super_version(move |sv| SuperVersion {
                        cf_tables: Arc::new(ColumnFamilyTables {
                            memtable: sv.cf_tables.memtable.clone(),
                            imemtables: sv.cf_tables.imemtables.remove(number),
                        }),
                        sst_version: current,
                        step_version: sv.step_version + 1,
                    });
                })
            });
        });

        let inner2 = inner.clone();
        let major_pool =
            MajorCompactionTaskPool::new(&config, backend, move |additional, removal| {
                let mut vec = Vec::new();
                for meta in additional {
                    vec.push(VersionEdit::SSTAppended(meta));
                }
                for seq in removal {
                    vec.push(VersionEdit::SSTRemove(seq));
                }
                inner2.info.with_manifest(|m| {
                    m.modify_with(vec, |current| {
                        inner2.modify_super_version(move |sv| SuperVersion {
                            cf_tables: sv.cf_tables.clone(),
                            sst_version: current,
                            step_version: sv.step_version + 1,
                        });
                    })
                });
            });

        let mut this = Self {
            inner,
            minor_pool,
            major_pool,
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
    ) -> Result<Value> {
        let lifetime = super_version.lifetime();

        let key = key.into();
        // query from memtable
        match super_version
            .cf_tables
            .memtable
            .get(opt, key.clone(), &lifetime)
        {
            Ok((internal_key, value)) => {
                if opt.debug() {
                    info!("find key {:?} in memtable", key);
                }
                if internal_key.key_type() == KeyType::Del {
                    return Err(StorageError::KeyNotExist);
                }
                return Ok(value);
            }
            Err(e) => {
                if StorageError::KeyNotExist == e {
                } else {
                    return Err(e);
                }
            }
        }

        // query from imemetable
        match super_version
            .cf_tables
            .imemtables
            .get(opt, key.clone(), &lifetime)
        {
            Ok((internal_key, value)) => {
                if opt.debug() {
                    info!("find key {:?} in imemtables", key);
                }
                if internal_key.key_type() == KeyType::Del {
                    return Err(StorageError::KeyNotExist);
                }
                return Ok(value);
            }
            Err(e) => {
                if StorageError::KeyNotExist == e {
                } else {
                    return Err(e);
                }
            }
        }

        match SnapshotTable::new(
            snapshot,
            super_version.sst_version.clone(),
            &self.inner.cache,
        )
        .get(
            opt,
            self.inner.info.borrow_config(),
            key.clone(),
            self.inner.info.borrow_backend(),
            &lifetime,
        ) {
            Ok((internal_key, value)) => {
                if internal_key.key_type() == KeyType::Del {
                    return Err(StorageError::KeyNotExist);
                }
                return Ok(value);
            }
            Err(e) => return Err(e),
        }
    }

    pub fn get<K: Into<Bytes>>(&self, opt: &GetOption, key: K) -> Result<Value> {
        let inner = self.inner.as_ref();
        let super_version = self.super_version();
        let snapshot = inner.info.with_manifest(|m| m.snapshot());
        self.get_ex(opt, key, &super_version, snapshot)
    }

    pub fn scan<'a, R: RangeBounds<Bytes> + Clone>(
        &'a self,
        opt: &GetOption,
        range: R,
        super_version: &'a SuperVersion,
    ) -> ScanIter<'a, (Bytes, Value)> {
        let inner = self.inner.as_ref();
        let snapshot = inner.info.with_manifest(|m| m.snapshot());
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
            SnapshotTable::new(snapshot, super_version.sst_version.clone(), &inner.cache).scan(
                opt,
                inner.info.borrow_config(),
                range,
                self.inner.info.borrow_backend(),
                &lifetime,
            ),
        );

        ScanIter::<'a, (Bytes, Value)>::new(
            MergedIter::new(iters)
                .filter(|(key, _)| key.key_type() != KeyType::Del)
                .map(|(key, value)| (key.user_key(), value)),
        )
    }

    pub fn set<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        opt: &WriteOption,
        key: K,
        value: V,
    ) -> Result<u64> {
        let mut batch = WriteBatchBuilder::default();
        batch.set(key, value)?;

        self.set_batch(opt, batch.build())
    }

    pub fn del<K: AsRef<[u8]>>(&self, opt: &WriteOption, key: K) -> Result<u64> {
        let mut batch = WriteBatchBuilder::default();
        batch.del(key)?;

        self.set_batch(opt, batch.build())
    }

    pub fn set_batch(&self, opt: &WriteOption, batch: WriteBatch) -> Result<u64> {
        let inner = self.inner.as_ref();
        let tables = inner.tables.load();

        let cur_seq = inner
            .info
            .with_manifest(|m| m.allocate_seq(batch.count() as u64));

        inner.info.with_wal(|wal| -> Result<()> {
            if let Some(wal) = &wal {
                wal.append(&batch)?;
                if opt.fsync() {
                    wal.sync()?;
                }
            }
            Ok(())
        })?;

        tables.memtable.set_batch(batch, cur_seq)?;
        if tables.memtable.full() {
            self.flush_memtable();
        }
        Ok(cur_seq)
    }

    pub fn super_version(&self) -> Arc<SuperVersion> {
        self.inner.super_version()
    }
}

impl Storage {
    fn restore(&mut self) {
        let config = self.inner.info.borrow_config();
        let tables = self.inner.tables.load();
        let replayer = LogReplayer::new(self.inner.info.borrow_backend(), BatchLogSerializer);
        let iter = match replayer.iter(wal_name(config, tables.memtable.number())) {
            Ok(e) => e,
            Err(e) => {
                if e.is_io_not_found() {
                    return;
                } else {
                    panic!("{:?}", e);
                }
            }
        };

        for batch in iter {
            self.set_batch(&WriteOption::default(), batch.unwrap())
                .unwrap();
        }

        info!("wal restore from {}", tables.memtable.max_seq());
        self.flush_memtable();
    }

    /// flush memtable into imemtable
    pub(crate) fn flush_memtable(&self) {
        let inner = self.inner.as_ref();
        inner.modify_super_version_opt(move |sv: &SuperVersion| {
            let old_table = sv.cf_tables.memtable.clone();
            if !old_table.is_empty() {
                let new_number = inner.info.with_manifest(|m| m.allocate_sst_number());
                let memtable = Arc::new(Memtable::new(new_number));
                let current = sv.sst_version.clone();
                self.minor_pool.compact_async(old_table.clone());

                Some(SuperVersion {
                    cf_tables: Arc::new(ColumnFamilyTables {
                        memtable,
                        imemtables: sv.cf_tables.imemtables.push(old_table),
                    }),
                    sst_version: current,
                    step_version: sv.step_version + 1,
                })
            } else {
                None
            }
        });
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

        self.minor_pool.stop();
        let e = self.inner.info.with_manifest(|m| m.flush());
        if let Err(e) = e {
            error!("flush {}", e);
        }
    }
}

impl Storage {}

impl Drop for Storage {
    fn drop(&mut self) {
        self.shutdown();
    }
}
