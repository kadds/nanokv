use std::{
    collections::VecDeque,
    fs,
    mem::swap,
    ops::{Range, RangeBounds},
    sync::{mpsc, Arc},
    time::Duration,
};

use bytes::Bytes;
use log::info;

use crate::{
    cache::Cache,
    compaction::minor::MinorSerializer,
    iterator::{MergedIter, ScanIter},
    kv::{manifest::FileMetaData, sst::SnapshotTable},
    log::LogReplayer,
    snapshot::Snapshot,
    GetOption, KvIterator, WriteOption,
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

#[derive(Debug)]
struct Imemtables {
    imemtables: VecDeque<Arc<Imemtable>>,
    config: ConfigRef,
}

impl Imemtables {
    pub fn new(config: ConfigRef) -> Self {
        let vec = VecDeque::with_capacity(20);
        Self {
            imemtables: vec,
            config,
        }
    }
    pub fn push(&mut self, table: Imemtable) -> Arc<Imemtable> {
        let table = Arc::new(table);
        self.imemtables.push_back(table.clone());
        table
    }

    fn commit(&mut self, table: Arc<Imemtable>) {
        assert!(self.imemtables.len() > 0 && self.imemtables.front().unwrap().seq() == table.seq());
        self.imemtables.pop_front();
    }
    fn empty(&self) -> bool {
        self.imemtables.len() == 0
    }
}

impl Imemtables {
    fn get(&self, opt: &GetOption, key: Bytes) -> Option<Value> {
        for table in self.imemtables.iter().rev() {
            if let Some(value) = table.get(opt, key.clone()) {
                return Some(value);
            }
        }
        None
    }

    fn scan(
        &self,
        opt: &GetOption,
        range: impl RangeBounds<Bytes> + Clone,
    ) -> ScanIter<(Bytes, Value)> {
        let mut iters = Vec::new();
        for table in self.imemtables.iter().rev() {
            iters.push(table.scan(opt, range.clone()));
        }

        ScanIter::new(MergedIter::new(iters))
    }
}

pub struct Storage {
    memtable: Memtable,
    imemtables: Imemtables,
    wal: LogWriter<KvEntry, KvEntryLogSerializer>,
    serializer: Arc<MinorSerializer>,
    commit_rx: mpsc::Receiver<(Arc<Imemtable>, Arc<FileMetaData>)>,
    manifest: Arc<Manifest>,
    config: ConfigRef,
    cache: Cache,
}

pub struct Scanner {}

impl Scanner {
    fn scanner(&self) {}

    fn iter(&self) {}
}

impl Storage {
    pub fn new(config: ConfigRef, manifest: Arc<Manifest>) -> Self {
        let mut imemtables = Imemtables::new(config);
        let (commit_tx, commit_rx) = mpsc::sync_channel(20);
        let wal_path = format!("{}/wal/", config.path);
        fs::create_dir_all(&wal_path).unwrap();

        let log_serializer = KvEntryLogSerializer::default();
        let serializer = MinorSerializer::new(config.clone(), manifest.clone(), commit_tx);

        let mut restore_seq = manifest.last_sst_sequence();
        if restore_seq > 0 {
            restore_seq -= 1;
            let log_serializer = KvEntryLogSerializer::default();
            let mut memtable = Memtable::new(restore_seq);
            memtable = LogReplayer::new(log_serializer, restore_seq, &wal_path)
                .execute(memtable, |state, entry| {
                    state.set(&WriteOption::default(), entry).unwrap()
                });

            let imm: Imemtable = Imemtable::new(memtable, u64::MAX);
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

        let sf = Self {
            memtable: Memtable::new(seq),
            imemtables: imemtables,
            wal: LogWriter::new(log_serializer, seq, wal_path.clone()),
            serializer,
            commit_rx,
            manifest,
            cache: Cache::new(config.clone()),
            config,
        };

        sf
    }
}

impl Storage {
    pub fn get<K: Into<Bytes>>(&self, opt: &GetOption, key: K) -> Option<Value> {
        let key = key.into();
        // query from memtable
        if let Some(value) = self.memtable.get(opt, key.clone()) {
            if value.deleted() {
                return None;
            }
            return Some(value);
        }

        // query from imemetable
        if let Some(value) = self.imemtables.get(opt, key.clone()) {
            if value.deleted() {
                return None;
            }
            return Some(value);
        }

        let snapshot_guard = self.manifest.new_snapshot();
        let sn = if let Some(snapshot) = opt.snapshot() {
            snapshot.clone()
        } else {
            snapshot_guard.get()
        };

        if let Some(value) =
            SnapshotTable::new(sn, self.manifest.current(), self.config, &self.cache)
                .get(opt, key.clone())
        {
            if value.deleted() {
                return None;
            }
            return Some(value);
        }

        None
    }

    pub fn scan(
        &self,
        opt: &GetOption,
        range: impl RangeBounds<Bytes> + Clone,
    ) -> ScanIter<(Bytes, Value)> {
        let mut iters = Vec::new();
        iters.push(self.memtable.scan(opt, range.clone()));
        iters.push(self.imemtables.scan(opt, range.clone()));

        let snapshot_guard = self.manifest.new_snapshot();
        let sn = if let Some(snapshot) = opt.snapshot() {
            snapshot.clone()
        } else {
            snapshot_guard.get()
        };

        iters.push(
            SnapshotTable::new(sn, self.manifest.current(), self.config, &self.cache)
                .scan(opt, range.clone()),
        );

        ScanIter::new(MergedIter::new(iters).filter(|(key, value)| !value.deleted()))
    }

    pub fn set<K: Into<Bytes>>(
        &mut self,
        opt: &WriteOption,
        key: K,
        value: Bytes,
    ) -> SetResult<()> {
        self.set_by(opt, key, value, None, None)
    }

    pub fn set_by<K: Into<Bytes>>(
        &mut self,
        opt: &WriteOption,
        key: K,
        value: Bytes,
        ver: Option<u64>,
        ttl: Option<Duration>,
    ) -> SetResult<()> {
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
        let entry = KvEntry::new(key, value, ttl.map(|d| d.as_secs()), cur_ver);
        self.wal.append(&entry);
        if opt.fsync() {
            self.wal.sync();
        }

        self.memtable.set(opt, entry)?;
        if self.memtable.full() || self.wal.bytes() >= 1024 * 1024 * 50 {
            self.rotate();
        }
        Ok(())
    }

    pub fn del<S: Into<String>>(&mut self, opt: &WriteOption, key: S) -> SetResult<()> {
        let cur_ver = self.manifest.allocate_version(1);

        let entry = KvEntry::new_del(key.into(), cur_ver);
        self.memtable.set(opt, entry)
    }
}

impl Storage {
    fn rotate(&mut self) {
        if self.memtable.len() == 0 {
            return;
        }
        let new_seq = self.manifest.allocate_sst_sequence();
        let mut memtable = Memtable::new(new_seq);
        swap(&mut memtable, &mut self.memtable);

        self.wal.rotate(new_seq);
        let table = self.imemtables.push(Imemtable::new(
            memtable,
            self.manifest.oldest_snapshot_version(),
        ));
        self.serializer.compact_async(table);

        while let Ok((table, meta)) = self.commit_rx.try_recv() {
            self.wal.remove(table.seq());
            self.imemtables.commit(table);
            self.manifest.add_sst(meta);
        }
    }

    fn shutdown(&mut self) {
        info!("shutdown storage");
        if self.memtable.len() > 0 {
            let mut memtable = Memtable::new(0);
            swap(&mut memtable, &mut self.memtable);

            let table = self.imemtables.push(Imemtable::new(
                memtable,
                self.manifest.oldest_snapshot_version(),
            ));
            self.serializer.compact_async(table);
        }

        while !self.imemtables.empty() {
            if let Ok((table, meta)) = self.commit_rx.recv() {
                self.wal.remove(table.seq());
                self.imemtables.commit(table);
                self.manifest.add_sst(meta);
            } else {
                panic!("");
            }
        }

        self.serializer.stop();
        self.manifest.flush();
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        self.shutdown();
    }
}
