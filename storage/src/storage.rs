use std::{
    mem::swap,
    ops::RangeBounds,
    sync::{
        atomic::{AtomicPtr, Ordering},
        mpsc, Arc, Mutex,
    },
    time::Duration,
};

use bytes::Bytes;
use log::info;

use crate::{
    cache::Cache,
    compaction::minor::MinorSerializer,
    iterator::{MergedIter, ScanIter},
    kv::{
        imemtable::Imemtables, manifest::FileMetaData, sst::SnapshotTable,
        superversion::SuperVersion,
    },
    log::LogReplayer,
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

pub struct Storage {
    memtable: Arc<Memtable>,
    imemtables: Imemtables,
    lock: Mutex<()>,
    manifest: Arc<Manifest>,

    wal: Option<LogWriter<KvEntry, KvEntryLogSerializer>>,
    serializer: Arc<MinorSerializer>,
    commit_rx: mpsc::Receiver<(Arc<Imemtable>, Arc<FileMetaData>)>,
    config: ConfigRef,
    cache: Cache,

    super_version: AtomicPtr<SuperVersion>,
}

impl Storage {
    pub(crate) fn new(config: ConfigRef, manifest: Arc<Manifest>) -> Self {
        let mut imemtables = Imemtables::new();
        let (commit_tx, commit_rx) = mpsc::sync_channel(20);

        let log_serializer = KvEntryLogSerializer::default();
        let serializer = MinorSerializer::new(config.clone(), manifest.clone(), commit_tx);

        let mut restore_seq = manifest.last_sst_sequence();
        if restore_seq > 0 {
            restore_seq -= 1;
            let log_serializer = KvEntryLogSerializer::default();
            let mut memtable = Memtable::new(restore_seq);

            memtable = LogReplayer::new(log_serializer, fname::wal_name(config, restore_seq))
                .execute(memtable, |state, entry| {
                    state.set(&WriteOption::default(), entry).unwrap()
                });

            let imm: Imemtable = Imemtable::new(&memtable, u64::MAX);
            let latest_version = imm.min_max_ver().unwrap_or_default().1 + 1;

            manifest.set_latest_version(latest_version);
            info!(
                "wal restore from {} total {} latest version {}",
                restore_seq,
                imm.len(),
                latest_version
            );

            let table = imemtables.push(imm);
            serializer.compact_async(table);
        }

        let seq = manifest.allocate_sst_sequence();
        let wal = if config.no_wal {
            None
        } else {
            Some(LogWriter::new(
                config,
                log_serializer,
                seq,
                Box::new(fname::wal_name),
            ))
        };
        let memtable = Arc::new(Memtable::new(seq));

        let super_version = Arc::new(SuperVersion {
            memtable: memtable.clone(),
            imemtables: imemtables.clone(),
            sst_version: manifest.current(),
            step_version: 0,
        });
        let ptr = Arc::into_raw(super_version) as *mut SuperVersion;
        unsafe { Arc::increment_strong_count(ptr) };

        Self {
            memtable,
            imemtables,
            lock: Mutex::new(()),

            wal,
            serializer,
            commit_rx,
            manifest,
            cache: Cache::new(config.clone()),
            config,
            super_version: AtomicPtr::new(ptr),
        }
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
        if let Some(value) = super_version.memtable.get(opt, key.clone(), &lifetime) {
            if opt.debug() {
                info!("find key {:?} in memtable", key);
            }
            if value.deleted() {
                return None;
            }
            return Some(value);
        }

        // query from imemetable
        if let Some(value) = super_version.imemtables.get(opt, key.clone(), &lifetime) {
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
        let super_version = self.super_version();
        let snapshot_guard = self.manifest.new_snapshot();
        let snapshot = snapshot_guard.get();
        self.get_ex(opt, key, super_version.as_ref(), snapshot)
    }

    pub fn scan<'a, R: RangeBounds<Bytes> + Clone>(
        &'a self,
        opt: &GetOption,
        range: R,
        super_version: &'a SuperVersion,
    ) -> ScanIter<'a, (Bytes, Value)> {
        let snapshot_guard = self.manifest.new_snapshot();
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
        iters.push(self.memtable.scan(opt, range.clone(), &lifetime));
        iters.push(self.imemtables.scan(opt, range.clone(), &lifetime));

        let snapshot_guard = self.manifest.new_snapshot();
        let sn = if let Some(snapshot) = opt.snapshot() {
            snapshot.clone()
        } else {
            snapshot_guard.get()
        };

        iters.push(
            SnapshotTable::new(sn, self.manifest.current(), self.config, &self.cache)
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

        let cur_ver = self.manifest.allocate_version(1);
        let entry = KvEntry::new(key.clone(), value, ttl.map(|d| d.as_secs()), cur_ver);

        if let Some(wal) = &mut self.wal {
            wal.append(&entry);
            if opt.fsync() {
                wal.sync();
            }
        }

        self.memtable.set(opt, entry)?;
        if self.memtable.full() {
            if opt.debug() {
                info!("{:?} need flush", key);
            }
            self.rotate();
        }
        Ok(cur_ver)
    }

    pub fn del<S: Into<String>>(&mut self, opt: &WriteOption, key: S) -> SetResult<u64> {
        let cur_ver = self.manifest.allocate_version(1);

        let entry = KvEntry::new_del(key.into(), cur_ver);
        self.memtable.set(opt, entry)?;

        Ok(cur_ver)
    }
}

impl Storage {
    /// flush memtable into imemtable
    pub(crate) fn flush_memtable(&mut self) {
        if !self.memtable.is_empty() {
            let new_seq = self.manifest.allocate_sst_sequence();
            let mut memtable = Arc::new(Memtable::new(new_seq));
            swap(&mut memtable, &mut self.memtable);

            if let Some(wal) = &mut self.wal {
                wal.rotate(new_seq);
            }

            let table = self.imemtables.push(Imemtable::new(
                &memtable,
                self.manifest.oldest_snapshot_version(),
            ));
            self.install_super_version();
            self.serializer.compact_async(table);
        }
    }

    pub(crate) fn flush_imemtables(&mut self) {
        while !self.imemtables.empty() {
            if let Ok((table, meta)) = self.commit_rx.recv() {
                if let Some(wal) = &mut self.wal {
                    wal.remove(table.seq());
                }

                self.imemtables.commit(table);
                self.manifest.add_sst(meta);
                self.install_super_version();
            } else {
                panic!("");
            }
        }
    }

    fn rotate(&mut self) {
        if self.memtable.is_empty() {
            return;
        }
        self.flush_memtable();

        // check imemtables
        while let Ok((table, meta)) = self.commit_rx.try_recv() {
            if let Some(wal) = &mut self.wal {
                wal.remove(table.seq());
            }

            self.imemtables.commit(table);
            self.manifest.add_sst(meta);
            self.install_super_version();
        }
    }

    fn shutdown(&mut self) {
        info!("shutdown storage");

        self.flush_memtable();
        self.flush_imemtables();

        self.serializer.stop();
        self.manifest.flush();
    }

    fn install_super_version(&self) {
        let ptr = self.super_version.load(Ordering::Acquire);
        let sv = unsafe { Arc::from_raw(ptr) };
        if ptr.is_null() {
            panic!("invalid pointer");
        }

        unsafe { Arc::decrement_strong_count(ptr) };

        let super_version = Arc::new(SuperVersion {
            memtable: self.memtable.clone(),
            imemtables: self.imemtables.clone(),
            sst_version: self.manifest.current(),
            step_version: sv.step_version + 1,
        });

        let ptr = Arc::into_raw(super_version) as *mut SuperVersion;
        unsafe { Arc::increment_strong_count(ptr) };
        self.super_version.store(ptr, Ordering::Release);
    }

    pub fn super_version(&self) -> Arc<SuperVersion> {
        let ptr = self.super_version.load(Ordering::Acquire);
        unsafe { Arc::increment_strong_count(ptr) };
        unsafe { Arc::from_raw(ptr) }
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        self.shutdown();
    }
}
