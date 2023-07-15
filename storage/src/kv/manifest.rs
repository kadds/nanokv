use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashMap, LinkedList},
    fmt::Debug,
    io::{self, Read, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering},
        Arc, Mutex,
    },
};

use ::log::{error, info, warn};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use bytes::Bytes;
use superslice::Ext;

use crate::{
    backend::{
        fs::{ExtReader, ReadablePersist},
        Backend,
    },
    err::Result,
    log::{self, replayer::SegmentRead, wal::SegmentWrite, LogEntrySerializer, LogWriter},
    snapshot::Snapshot,
    util::fname::{self, manifest_name},
    Config,
};

#[derive(Debug, Clone)]
pub struct Run {
    files: Vec<Arc<FileStatistics>>,
    min: Bytes,
    max: Bytes,
}

impl Run {
    pub fn new() -> Self {
        Self {
            files: Vec::with_capacity(6),
            min: Bytes::new(),
            max: Bytes::new(),
        }
    }

    pub fn files(&self) -> &[Arc<FileStatistics>] {
        &self.files
    }

    pub fn push(&mut self, sst: Arc<FileStatistics>) {
        if self.min.is_empty() {
            self.min = sst.meta.min.clone();
        } else {
            self.min = self.min.clone().min(sst.meta.min.clone());
        }
        if self.max.is_empty() {
            self.max = sst.meta.max.clone();
        } else {
            self.max = self.max.clone().max(sst.meta.max.clone());
        }
        let idx = self.files.lower_bound_by(|f| f.meta.min.cmp(&sst.meta.max));
        if idx >= self.files.len() {
            self.files.push(sst);
        } else {
            self.files.insert(idx, sst);
        }
    }

    pub fn binary_find_file(&self, key: &[u8]) -> Option<&FileStatistics> {
        if &self.min > key || &self.max < key {
            return None;
        }
        let idx = self
            .files
            .lower_bound_by(|val| (&val.meta.max[..]).cmp(key));
        if idx >= self.files.len() {
            ::log::info!(
                "key {} in file {:?}",
                String::from_utf8(key.into()).unwrap(),
                self
            );
            return None;
        }
        Some(&self.files[idx])
    }
}

#[derive(Debug, Clone)]
struct Runs {
    pub runs: Vec<Run>,
}

impl Runs {
    pub fn new() -> Self {
        Self { runs: Vec::new() }
    }
}

#[derive(Clone)]
pub struct Version {
    id: u64,
    sst_files: Vec<Runs>,
    seq_map: HashMap<u64, Arc<FileStatistics>>,
}

pub type VersionRef = Arc<Version>;

impl Default for Version {
    fn default() -> Self {
        let mut sst_files = Vec::new();
        sst_files.resize((MAX_LEVEL + 1) as usize, Runs::new());

        Self {
            id: 0,
            sst_files,
            seq_map: HashMap::new(),
        }
    }
}

impl Version {
    pub fn level_n(&self, n: u32) -> &[Run] {
        &self.sst_files[n as usize].runs[..]
    }
    pub fn list_run<F: FnMut(&Run)>(&self, level: u32, mut f: F) {
        for run in &self.sst_files[level as usize].runs {
            f(run)
        }
    }
}

impl Debug for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Version")
            .field("id", &self.id)
            .field("sst_files", &self.sst_files)
            .finish()
    }
}

#[derive(Debug, Default)]
pub struct VersionLogSerializer;

impl LogEntrySerializer for VersionLogSerializer {
    type Entry = Version;

    fn write<W>(&self, entry: &Self::Entry, w: &mut W) -> io::Result<()>
    where
        W: SegmentWrite,
    {
        let total_iter = entry
            .sst_files
            .iter()
            .flat_map(|r| r.runs.iter())
            .flat_map(|v| v.files.iter());
        let total_files = total_iter.clone().count();
        w.write_u32::<LE>(total_files as u32)?;

        for file in total_iter {
            let s = FileMetaDataLogSerializer::default();
            s.write(&file.meta, w)?;
        }
        Ok(())
    }

    fn read<R>(&self, r: &mut R) -> io::Result<Self::Entry>
    where
        R: SegmentRead,
    {
        let total_files = r.read_u32::<LE>().unwrap();

        let mut entry = Self::Entry::default();
        entry
            .sst_files
            .resize((MAX_LEVEL + 1) as usize, Runs::new());
        let mut last_max: Option<Bytes> = None;
        let mut last_level = MAX_LEVEL + 1;

        for _ in 0..total_files {
            let s = FileMetaDataLogSerializer::default();
            let meta = s.read(r)?;
            if meta.level >= MAX_LEVEL {
                panic!("level not match {}", meta.level);
            }
            if last_level != meta.level {
                last_max = None;
                last_level = meta.level;
            }
            let level = meta.level as usize;
            let mut new_run = true;
            if let Some(last_max) = &last_max {
                if meta.min < last_max {
                    new_run = false;
                }
            }
            if new_run {
                entry.sst_files[level].runs.push(Run::new());
            }
            last_max = Some(meta.max.clone());
            let seq = meta.number;
            let sst = Arc::new(FileStatistics::new(meta));
            entry.seq_map.insert(seq, sst.clone());

            entry.sst_files[level].runs.last_mut().unwrap().push(sst);
        }
        Ok(entry)
    }
}

#[derive(Debug)]
pub enum VersionEdit {
    SSTAppended(FileMetaData),
    SSTRemove(u64),
    NewRun(u64),
    VersionChanged(u64),
    SSTSequenceChanged(u64),
    ManifestSequenceChanged(u64),
    Snapshot(VersionRef),
}

#[derive(Debug, Clone)]
pub struct FileMetaData {
    pub number: u64,

    pub min: Bytes,
    pub max: Bytes,
    pub min_ver: u64,
    pub max_ver: u64,

    pub left: usize,
    pub right: usize,

    pub keys: u64,
    pub level: u32,
}

impl FileMetaData {
    pub fn new(
        number: u64,
        min: Bytes,
        max: Bytes,
        min_ver: u64,
        max_ver: u64,
        keys: u64,
        level: u32,
    ) -> Self {
        Self {
            number,
            min,
            max,
            min_ver,
            max_ver,
            keys,
            level,

            left: 0,
            right: 0,
        }
    }
}

const FILE_STATE_USING: u8 = 0;
const FILE_STATE_PICKED: u8 = 1;
const FILE_STATE_DEPRECATED: u8 = 2;

#[derive(Debug)]
pub struct FileStatistics {
    meta: FileMetaData,
    state: AtomicU8,
    bloom_fail_count: AtomicU32,
}

impl FileStatistics {
    pub fn new(meta: FileMetaData) -> Self {
        Self {
            meta,
            state: AtomicU8::new(FILE_STATE_USING),
            bloom_fail_count: AtomicU32::new(0),
        }
    }
    pub fn meta(&self) -> &FileMetaData {
        &self.meta
    }

    pub fn set_using(&self) {
        self.state.store(FILE_STATE_USING, Ordering::Relaxed);
    }

    pub fn is_using(&self) -> bool {
        self.state.load(Ordering::Acquire) == FILE_STATE_USING
    }

    pub fn is_using_relaxed(&self) -> bool {
        self.state.load(Ordering::Relaxed) == FILE_STATE_USING
    }

    pub fn set_picked(&self) -> bool {
        self.state
            .compare_exchange(
                FILE_STATE_USING,
                FILE_STATE_PICKED,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    pub fn set_deprecated(&self) -> bool {
        self.state
            .compare_exchange(
                FILE_STATE_PICKED,
                FILE_STATE_DEPRECATED,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
    }
}

#[derive(Debug, Default)]
pub struct FileMetaDataLogSerializer;

impl LogEntrySerializer for FileMetaDataLogSerializer {
    type Entry = FileMetaData;

    fn write<W>(&self, entry: &Self::Entry, w: &mut W) -> io::Result<()>
    where
        W: SegmentWrite,
    {
        w.write_u64::<LE>(entry.number)?;
        w.write_u64::<LE>(entry.min_ver)?;
        w.write_u64::<LE>(entry.max_ver)?;
        w.write_u64::<LE>(entry.keys)?;
        w.write_u32::<LE>(entry.level)?;
        w.write_u32::<LE>(entry.min.len() as u32)?;
        w.write_all(&entry.min)?;
        w.write_u32::<LE>(entry.max.len() as u32)?;
        w.write_all(&entry.max)?;
        Ok(())
    }

    fn read<R>(&self, r: &mut R) -> io::Result<Self::Entry>
    where
        R: SegmentRead,
    {
        let number = r.read_u64::<LE>()?;
        let min_ver = r.read_u64::<LE>()?;
        let max_ver = r.read_u64::<LE>()?;
        let keys = r.read_u64::<LE>()?;
        let level = r.read_u32::<LE>()?;

        let min_key_len = r.read_u32::<LE>()?;
        let mut vec = Vec::new();
        vec.resize(min_key_len as usize, 0);
        r.read_exact(&mut vec)?;
        let min_key = vec.into();

        let max_key_len = r.read_u32::<LE>()?;
        let mut vec = Vec::new();
        vec.resize(max_key_len as usize, 0);
        r.read_exact(&mut vec)?;
        let max_key = vec.into();

        Ok(Self::Entry {
            number,
            min_ver,
            max_ver,
            keys,
            level,
            min: min_key,
            max: max_key,
            left: 0,
            right: 0,
        })
    }
}

#[derive(Debug, Default)]
pub struct ManifestLogSerializer;

impl LogEntrySerializer for ManifestLogSerializer {
    type Entry = VersionEdit;

    fn write<W>(&self, entry: &Self::Entry, w: &mut W) -> io::Result<()>
    where
        W: SegmentWrite,
    {
        match &entry {
            VersionEdit::SSTAppended(meta) => {
                w.write_u8(1)?;
                let s = FileMetaDataLogSerializer::default();
                s.write(meta, w)
            }
            VersionEdit::SSTRemove(seq) => {
                w.write_u8(2)?;
                w.write_u64::<LE>(*seq)?;
                Ok(())
            }
            VersionEdit::VersionChanged(ver) => {
                w.write_u8(3)?;
                w.write_u64::<LE>(*ver)?;
                Ok(())
            }
            VersionEdit::SSTSequenceChanged(seq) => {
                w.write_u8(4)?;
                w.write_u64::<LE>(*seq)?;
                Ok(())
            }
            VersionEdit::ManifestSequenceChanged(seq) => {
                w.write_u8(5)?;
                w.write_u64::<LE>(*seq)?;
                Ok(())
            }
            VersionEdit::Snapshot(ver) => {
                w.write_u8(6)?;
                let s = VersionLogSerializer::default();
                s.write(ver, w)
            }
            VersionEdit::NewRun(level) => {
                w.write_u8(7)?;
                w.write_u64::<LE>(*level)?;
                Ok(())
            }
        }
    }

    fn read<R>(&self, r: &mut R) -> io::Result<Self::Entry>
    where
        R: SegmentRead,
    {
        let ty = r.read_u8()?;
        match ty {
            1 => {
                let s = FileMetaDataLogSerializer::default();
                let meta = s.read(r)?;
                Ok(VersionEdit::SSTAppended(meta))
            }
            2 => Ok(VersionEdit::SSTRemove(r.read_u64::<LE>()?)),
            3 => Ok(VersionEdit::VersionChanged(r.read_u64::<LE>()?)),
            4 => Ok(VersionEdit::SSTSequenceChanged(r.read_u64::<LE>()?)),
            5 => Ok(VersionEdit::ManifestSequenceChanged(r.read_u64::<LE>()?)),
            6 => {
                let s = VersionLogSerializer::default();
                Ok(VersionEdit::Snapshot(Arc::new(s.read(r)?)))
            }
            7 => {
                let level = r.read_u64::<LE>()?;
                Ok(VersionEdit::NewRun(level))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid manifest type",
            )),
        }
    }
}

#[derive(Debug, Default)]
pub struct VersionSet {
    current: LinkedList<VersionRef>,
    version: Version,

    last_seq: u64,
    last_sst_num: u64,
    last_manifest_num: u64,
    snapshot_versions: BTreeMap<u64, usize>,
}

impl VersionSet {
    pub fn add(&mut self, edit: &VersionEdit) -> Option<()> {
        match edit {
            VersionEdit::SSTAppended(meta) => {
                let level = meta.level as usize;
                if self.version.sst_files[level].runs.is_empty() {
                    self.version.sst_files[level].runs.push(Run::new());
                }
                let cur = self.version.sst_files[level].runs.last_mut().unwrap();
                let fs = Arc::new(FileStatistics::new(meta.clone()));

                cur.push(fs.clone());

                self.version.seq_map.insert(meta.number, fs);
                Some(())
            }
            VersionEdit::SSTRemove(seq) => {
                let fs = self.version.seq_map.get(seq)?;
                let level = fs.meta.level as usize;
                let mut ok = false;
                let mut drop_idx = None;
                for (idx, run) in self.version.sst_files[level].runs.iter_mut().enumerate() {
                    if let Ok(index) = run.files.binary_search_by(|f| f.meta.number.cmp(seq)) {
                        ok = true;
                        run.files.remove(index);
                        self.version.seq_map.remove(seq);
                        if run.files.is_empty() {
                            drop_idx = Some(idx);
                        }
                        break;
                    }
                }
                if !ok {
                    error!("sst remove level {} seq {} fail", level, seq);
                    return None;
                }
                if let Some(idx) = drop_idx {
                    self.version.sst_files[level].runs.remove(idx);
                }

                Some(())
            }
            VersionEdit::NewRun(level) => {
                self.version.sst_files[*level as usize]
                    .runs
                    .push(Run::new());
                Some(())
            }
            VersionEdit::VersionChanged(ver) => {
                self.last_seq = *ver;
                Some(())
            }
            VersionEdit::SSTSequenceChanged(seq) => {
                self.last_sst_num = *seq;
                Some(())
            }
            VersionEdit::ManifestSequenceChanged(seq) => {
                self.last_manifest_num = *seq;
                Some(())
            }
            VersionEdit::Snapshot(ver) => {
                self.current.push_front(ver.clone());
                Some(())
            }
        }
    }

    pub fn current(&mut self) -> VersionRef {
        let ver = Arc::new(self.version.clone());
        self.version.id += 1;

        self.current.push_front(ver.clone());
        ver
    }

    pub fn detach_version(&self, ver: VersionRef) {
        self.current.iter().find(|v| v.id == ver.id);
    }

    pub fn current_snapshot_version(&mut self) -> u64 {
        let ver = self.last_seq;
        self.snapshot_versions
            .entry(ver)
            .and_modify(|val| *val += 1)
            .or_insert(1);
        ver
    }

    pub fn release_snapshot_version(&mut self, ver: u64) {
        let mut del = false;
        self.snapshot_versions
            .entry(ver)
            .and_modify(|val| {
                *val -= 1;
                if *val == 0 {
                    del = true;
                }
            })
            .or_default();

        if del {
            self.snapshot_versions.remove(&ver);
        }
    }
}

pub const MAX_LEVEL: u32 = 8;

pub struct Manifest<'a> {
    config: &'a Config,
    version_set: Mutex<VersionSet>,
    current_path: PathBuf,
    current_tmp_path: PathBuf,
    wal: Mutex<LogWriter<'a, ManifestLogSerializer>>,
    seq: AtomicU64,
    backend: &'a Backend,
}

impl<'a> Manifest<'a> {
    pub fn new(config: &'a Config, backend: &'a Backend) -> Self {
        // replay version log
        let version_set = Mutex::new(VersionSet::default());
        let current_path = fname::manifest_current(config);
        let current_tmp_path = fname::manifest_current_tmp(config);

        let seq = Self::load_current_log_sequence(backend, &current_path).unwrap_or_default();
        info!(
            "restore {:?} current log seq {}",
            current_path.as_os_str(),
            seq
        );

        let wal = LogWriter::new(backend.clone(), ManifestLogSerializer);
        let path = manifest_name(config, seq + 1);
        wal.rotate(path).unwrap();

        let mut this = Self {
            config,
            version_set,
            current_path,
            current_tmp_path,
            wal: Mutex::new(wal),
            seq: AtomicU64::new(seq + 1),
            backend,
        };

        this.restore_from_wal(seq).unwrap();

        this.save_current_log();

        this.version_set.lock().unwrap().last_manifest_num = seq + 2;
        this.remove_unused_wal(seq + 1);

        // TODO: reload global version from wal
        this
    }

    pub fn load_current_log_sequence(backend: &Backend, path: &PathBuf) -> Option<u64> {
        let mut buf = String::new();
        if let Err(e) = backend.fs.open(path, false).and_then(|mut f| {
            let len = f.size();
            Ok(ExtReader::new(f.as_ref(), 0, len).read_to_string(&mut buf))
        }) {
            warn!("{:?} read {}", path.as_os_str(), e);
        }
        buf.parse().ok()
    }

    pub fn save_current_log(&self) {
        let seq = self.seq.load(Ordering::Acquire);
        let _ = self
            .backend
            .fs
            .create(&self.current_tmp_path, None)
            .unwrap()
            .write(seq.to_string().as_bytes());

        self.backend
            .fs
            .rename(&self.current_tmp_path, &self.current_path)
            .unwrap();
    }

    pub fn current_log_sequence(&self) -> Option<u64> {
        Self::load_current_log_sequence(self.backend, &self.current_path)
    }

    fn restore_from_wal(&mut self, seq: u64) -> Result<()> {
        let path = fname::manifest_name(self.config, seq);
        let s = ManifestLogSerializer::default();
        let replayer = log::LogReplayer::new(self.backend.clone(), s);

        let mut state = VersionSet::default();

        let iter = match replayer.iter(path) {
            Ok(e) => e,
            Err(e) => {
                if e.is_io_not_found() {
                    return Ok(());
                } else {
                    return Err(e);
                }
            }
        };

        for edit in iter {
            state.add(&edit?);
        }

        info!("load manifest {} {:?}", seq, state);
        *self.version_set.lock().unwrap() = state;
        Ok(())
    }

    fn remove_unused_wal(&self, _except_seq: u64) {}
}

impl<'a> Manifest<'a> {
    fn commit(&self, edit: &VersionEdit) -> Result<()> {
        self.wal.lock().unwrap().append(edit)?;
        self.wal.lock().unwrap().sync()?;
        Ok(())
    }

    fn rotate(&self) -> Result<()> {
        let old_seq = {
            let wal = self.wal.lock().unwrap();
            let old_seq = self.seq.load(Ordering::Acquire);
            let seq = self.allocate_manifest_number();

            let mut ver = self.version_set.lock().unwrap();

            wal.append(&VersionEdit::VersionChanged(ver.last_seq))?;
            wal.append(&VersionEdit::ManifestSequenceChanged(ver.last_manifest_num))?;
            wal.append(&VersionEdit::SSTSequenceChanged(ver.last_sst_num))?;
            // finish old wal file

            let path = manifest_name(self.config, seq);

            wal.rotate(path)?;
            // new wal file
            wal.append(&VersionEdit::Snapshot(ver.current()))?;
            wal.append(&VersionEdit::VersionChanged(ver.last_seq))?;
            wal.append(&VersionEdit::ManifestSequenceChanged(ver.last_manifest_num))?;
            wal.append(&VersionEdit::SSTSequenceChanged(ver.last_sst_num))?;

            wal.sync()?;
            self.seq.store(seq, Ordering::Release);
            old_seq
        };
        {
            self.save_current_log();
        }
        {
            let _m = self.wal.lock().unwrap();
            let path = manifest_name(self.config, old_seq);
            let _ = self.backend.fs.remove(&path);
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.rotate()
    }

    pub fn allocate_seq(&self, num: u64) -> u64 {
        let mut ver = self.version_set.lock().unwrap();
        let return_ver = ver.last_seq;
        ver.last_seq += num;
        return_ver
    }

    pub fn set_latest_seq(&self, seq: u64) {
        let mut ver = self.version_set.lock().unwrap();
        ver.last_seq = seq;
    }

    pub fn allocate_sst_number(&self) -> u64 {
        let mut ver = self.version_set.lock().unwrap();
        let return_num = ver.last_sst_num;
        let edit = VersionEdit::SSTSequenceChanged(ver.last_sst_num + 1);
        self.commit(&edit).unwrap();

        ver.add(&edit);
        return_num
    }

    pub fn last_sst_number(&self) -> u64 {
        let ver = self.version_set.lock().unwrap();
        ver.last_sst_num
    }

    fn allocate_manifest_number(&self) -> u64 {
        let mut ver = self.version_set.lock().unwrap();
        let return_num = ver.last_manifest_num;
        ver.last_manifest_num += 1;
        return_num
    }

    pub fn add_sst_with<F: FnOnce(Arc<Version>)>(&self, meta: FileMetaData, new_run: bool, f: F) {
        if new_run {
            let edit = VersionEdit::NewRun(meta.level as u64);
            self.commit(&edit).unwrap();
            let mut vs = self.version_set.lock().unwrap();
            vs.add(&edit);
        }
        let num = meta.number;
        let edit = VersionEdit::SSTAppended(meta);
        self.commit(&edit).unwrap();
        let mut vs = self.version_set.lock().unwrap();
        vs.add(&edit);
        info!("add sst {} {:?}", num, vs.current());

        f(vs.current());
    }

    pub fn modify_with<F: FnOnce(Arc<Version>)>(&self, versions: Vec<VersionEdit>, f: F) {
        let mut vs = self.version_set.lock().unwrap();
        for v in versions {
            self.wal.lock().unwrap().append(&v).unwrap();
            vs.add(&v);
        }
        self.wal.lock().unwrap().sync().unwrap();
        f(vs.current());
    }

    pub fn current(&self) -> VersionRef {
        let mut ver = self.version_set.lock().unwrap();
        ver.current()
    }

    pub fn new_snapshot(&'a self) -> SnapshotGuard<'a> {
        let mut ver = self.version_set.lock().unwrap();
        SnapshotGuard {
            snapshot: Snapshot::new(ver.current_snapshot_version()),
            manifest: self,
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let mut ver = self.version_set.lock().unwrap();
        Snapshot::new(ver.current_snapshot_version())
    }

    fn release_snapshot(&self, snapshot_version: u64) {
        let mut ver = self.version_set.lock().unwrap();
        ver.release_snapshot_version(snapshot_version);
    }

    pub fn oldest_snapshot_version(&self) -> u64 {
        let ver = self.version_set.lock().unwrap();
        ver.snapshot_versions
            .iter()
            .map(|(k, _v)| *k)
            .next()
            .unwrap_or(u64::MAX)
    }
}

pub struct SnapshotGuard<'a> {
    snapshot: Snapshot,
    manifest: &'a Manifest<'a>,
}

impl<'a> SnapshotGuard<'a> {
    pub fn get(&self) -> Snapshot {
        self.snapshot.clone()
    }
}

impl<'a> Drop for SnapshotGuard<'a> {
    fn drop(&mut self) {
        self.manifest.release_snapshot(self.snapshot.sequence())
    }
}
