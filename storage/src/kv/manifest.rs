use std::{
    collections::{BTreeMap, HashMap, LinkedList},
    fmt::Debug,
    fs::{self, File},
    io::{Read, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, AtomicU8, Ordering},
        Arc, Mutex,
    },
};

use ::log::{error, info, warn};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use bytes::Bytes;
use superslice::Ext;

use crate::{
    log::{self, LogEntrySerializer, LogWriter},
    snapshot::Snapshot,
    util::fname,
    ConfigRef,
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
            files: Vec::new(),
            min: Bytes::new(),
            max: Bytes::new(),
        }
    }

    pub fn push(&mut self, sst: Arc<FileStatistics>) {
        if self.min.len() == 0 {
            self.min = sst.meta.min.clone();
        } else {
            self.min = self.min.clone().min(sst.meta.min.clone());
        }
        if self.max.len() == 0 {
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
    pub fn level_n(&self, n: u32) -> impl DoubleEndedIterator<Item = &FileStatistics> {
        self.sst_files[n as usize]
            .runs
            .iter()
            .map(|r| r.files.iter().map(|f| f.as_ref()))
            .flatten()
    }
    pub fn list_run<F: FnMut(&Run)>(&self, level: u32, mut f: F) {
        for run in &self.sst_files[level as usize].runs {
            f(run)
        }
    }

    pub fn level_size(&self, level: u32) -> u32 {
        self.level_n(level).count() as u32
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

    fn write<W>(&self, entry: &Self::Entry, mut w: W) -> u64
    where
        W: Write,
    {
        let total_iter = entry
            .sst_files
            .iter()
            .map(|r| r.runs.iter())
            .flatten()
            .map(|v| v.files.iter())
            .flatten();
        let total_files = total_iter.clone().count();
        w.write_u32::<LE>(total_files as u32).unwrap();

        let mut sum = 0;
        for file in total_iter {
            let mut s = FileMetaDataLogSerializer::default();
            sum += s.write(&file.meta, &mut w);
        }

        3 * 8 + 4 + sum
    }

    fn read<R>(&self, mut r: R) -> Option<Self::Entry>
    where
        R: Read,
    {
        let total_files = r.read_u32::<LE>().unwrap();

        let mut entry = Self::Entry::default();
        entry
            .sst_files
            .resize((MAX_LEVEL + 1) as usize, Runs::new());
        let mut last_max: Option<Bytes> = None;
        let mut last_level = MAX_LEVEL + 1;

        for _ in 0..total_files {
            let mut s = FileMetaDataLogSerializer::default();
            let meta = s.read(&mut r)?;
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
            let seq = meta.seq;
            let sst = Arc::new(FileStatistics::new(meta));
            entry.seq_map.insert(seq, sst.clone());

            entry.sst_files[level].runs.last_mut().unwrap().push(sst);
        }
        Some(entry)
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
    pub seq: u64,

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
        seq: u64,
        min: Bytes,
        max: Bytes,
        min_ver: u64,
        max_ver: u64,
        keys: u64,
        level: u32,
    ) -> Self {
        Self {
            seq,
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

    fn write<W>(&self, entry: &Self::Entry, mut w: W) -> u64
    where
        W: Write,
    {
        w.write_u64::<LE>(entry.seq).unwrap();
        w.write_u64::<LE>(entry.min_ver).unwrap();
        w.write_u64::<LE>(entry.max_ver).unwrap();
        w.write_u64::<LE>(entry.keys).unwrap();
        w.write_u32::<LE>(entry.level).unwrap();
        w.write_u32::<LE>(entry.min.len() as u32).unwrap();
        w.write_all(&entry.min).unwrap();
        w.write_u32::<LE>(entry.max.len() as u32).unwrap();
        w.write_all(&entry.max).unwrap();
        32 + 4 + 8 + entry.min.len() as u64 + entry.max.len() as u64
    }

    fn read<R>(&self, mut r: R) -> Option<Self::Entry>
    where
        R: Read,
    {
        let seq = r.read_u64::<LE>().unwrap();
        let min_ver = r.read_u64::<LE>().unwrap();
        let max_ver = r.read_u64::<LE>().unwrap();
        let keys = r.read_u64::<LE>().unwrap();
        let level = r.read_u32::<LE>().unwrap();

        let min_key_len = r.read_u32::<LE>().unwrap();
        let mut vec = Vec::new();
        vec.resize(min_key_len as usize, 0);
        r.read_exact(&mut vec).unwrap();
        let min_key = vec.into();

        let max_key_len = r.read_u32::<LE>().unwrap();
        let mut vec = Vec::new();
        vec.resize(max_key_len as usize, 0);
        r.read_exact(&mut vec).unwrap();
        let max_key = vec.into();

        Some(Self::Entry {
            seq,
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

    fn write<W>(&self, entry: &Self::Entry, mut w: W) -> u64
    where
        W: Write,
    {
        match &entry {
            VersionEdit::SSTAppended(meta) => {
                w.write_u8(1).unwrap();
                let mut s = FileMetaDataLogSerializer::default();
                s.write(meta, w) + 1
            }
            VersionEdit::SSTRemove(seq) => {
                w.write_u8(2).unwrap();
                w.write_u64::<LE>(*seq).unwrap();
                1 + 8
            }
            VersionEdit::VersionChanged(ver) => {
                w.write_u8(3).unwrap();
                w.write_u64::<LE>(*ver).unwrap();
                1 + 8
            }
            VersionEdit::SSTSequenceChanged(seq) => {
                w.write_u8(4).unwrap();
                w.write_u64::<LE>(*seq).unwrap();
                1 + 8
            }
            VersionEdit::ManifestSequenceChanged(seq) => {
                w.write_u8(5).unwrap();
                w.write_u64::<LE>(*seq).unwrap();
                1 + 8
            }
            VersionEdit::Snapshot(ver) => {
                w.write_u8(6).unwrap();
                let mut s = VersionLogSerializer::default();
                s.write(ver, w) + 1
            }
            VersionEdit::NewRun(level) => {
                w.write_u8(7).unwrap();
                w.write_u64::<LE>(*level).unwrap();
                1 + 8
            }
        }
    }

    fn read<R>(&self, mut r: R) -> Option<Self::Entry>
    where
        R: Read,
    {
        let ty = r.read_u8().unwrap();
        match ty {
            1 => {
                let mut s = FileMetaDataLogSerializer::default();
                let meta = s.read(r)?;
                Some(VersionEdit::SSTAppended(meta))
            }
            2 => Some(VersionEdit::SSTRemove(r.read_u64::<LE>().ok()?)),
            3 => Some(VersionEdit::VersionChanged(r.read_u64::<LE>().ok()?)),
            4 => Some(VersionEdit::SSTSequenceChanged(r.read_u64::<LE>().ok()?)),
            5 => Some(VersionEdit::ManifestSequenceChanged(
                r.read_u64::<LE>().ok()?,
            )),
            6 => {
                let mut s = VersionLogSerializer::default();
                Some(VersionEdit::Snapshot(Arc::new(s.read(&mut r)?)))
            }
            7 => {
                let level = r.read_u64::<LE>().ok()?;
                Some(VersionEdit::NewRun(level))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
pub struct VersionSet {
    current: LinkedList<VersionRef>,
    version: Version,

    last_version: u64,
    last_sst_seq: u64,
    last_manifest_seq: u64,
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

                self.version.seq_map.insert(meta.seq, fs.clone());
                Some(())
            }
            VersionEdit::SSTRemove(seq) => {
                let fs = self.version.seq_map.get(seq)?;
                let level = fs.meta.level as usize;
                let mut ok = false;
                let mut drop_idx = None;
                for (idx, run) in self.version.sst_files[level].runs.iter_mut().enumerate() {
                    match run.files.binary_search_by(|f| f.meta.seq.cmp(seq)) {
                        Ok(index) => {
                            ok = true;
                            run.files.remove(index);
                            self.version.seq_map.remove(seq);
                            if run.files.is_empty() {
                                drop_idx = Some(idx);
                            }
                            break;
                        }
                        Err(_) => {}
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
                self.last_version = *ver;
                Some(())
            }
            VersionEdit::SSTSequenceChanged(seq) => {
                self.last_sst_seq = *seq;
                Some(())
            }
            VersionEdit::ManifestSequenceChanged(seq) => {
                self.last_manifest_seq = *seq;
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
        let ver = self.last_version;
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

fn clear_log(config: ConfigRef, seq: u64) {
    let path = fname::manifest_name(config, seq);
    let parent = path.parent().unwrap();
    info!("{:?}", parent);
    for file in parent.read_dir().unwrap() {
        match file {
            Ok(file) => {
                match file
                    .path()
                    .file_stem()
                    .and_then(|n| n.to_str())
                    .and_then(|v| v.parse::<u64>().ok())
                {
                    Some(v) => {
                        if v != seq {
                            let _ = fs::remove_file(file.path());
                        }
                    }
                    None => (),
                }
            }
            Err(err) => {
                warn!("{:?} list {}", parent.as_os_str(), err)
            }
        }
    }
}

pub const MAX_LEVEL: u32 = 8;

pub struct Manifest {
    config: ConfigRef,
    version_set: Mutex<VersionSet>,
    current_path: PathBuf,
    current_tmp_path: PathBuf,
    wal: Mutex<LogWriter<VersionEdit, ManifestLogSerializer>>,
}

impl Manifest {
    pub fn new(config: ConfigRef) -> Self {
        // replay version log
        let version_set = Mutex::new(VersionSet::default());
        let current_path = fname::manifest_current(config);
        let current_tmp_path = fname::manifest_current_tmp(config);

        let seq = Self::load_current_log_sequence(&current_path).unwrap_or_default();
        info!("restore {:?} current seq {}", current_path.as_os_str(), seq);

        let mut this = Self {
            config,
            version_set,
            current_path,
            current_tmp_path,
            wal: Mutex::new(LogWriter::new(
                config,
                ManifestLogSerializer::default(),
                seq + 1,
                Box::new(fname::manifest_name),
            )),
        };

        this.restore_from_wal(seq);
        this.save_current_log();
        clear_log(config, seq + 2);

        this.version_set.lock().unwrap().last_manifest_seq = seq + 2;
        this.remove_unused_wal(seq + 1);

        // TODO: reload global version from wal
        this
    }

    pub fn load_current_log_sequence(path: &PathBuf) -> Option<u64> {
        let mut buf = String::new();
        match File::open(&path).map(|mut f| f.read_to_string(&mut buf)) {
            Err(e) => {
                warn!("{:?} read {}", path.as_os_str(), e);
            }
            Ok(_) => (),
        }
        buf.parse().ok()
    }

    pub fn save_current_log(&self) {
        let seq = self.wal.lock().unwrap().seq();
        let _ = File::create(&self.current_tmp_path)
            .unwrap()
            .write(seq.to_string().as_bytes());

        fs::rename(&self.current_tmp_path, &self.current_path).unwrap();
    }

    pub fn current_log_sequence(&self) -> Option<u64> {
        Self::load_current_log_sequence(&self.current_path)
    }

    fn restore_from_wal(&mut self, seq: u64) {
        let path = fname::manifest_name(self.config, seq);
        let s = ManifestLogSerializer::default();
        let mut replayer = log::LogReplayer::new(s, path);
        let final_state = replayer.execute(VersionSet::default(), |state, edit| {
            state.add(&edit);
        });
        info!("load manifest {} {:?}", seq, final_state);
        *self.version_set.lock().unwrap() = final_state;
    }

    fn remove_unused_wal(&self, _except_seq: u64) {}
}

impl Manifest {
    fn commit(&self, edit: &VersionEdit) {
        self.wal.lock().unwrap().append(edit);
        self.wal.lock().unwrap().sync();
    }

    fn rotate(&self) {
        let old_seq = {
            let mut wal = self.wal.lock().unwrap();
            let old_seq = wal.seq();
            let seq = self.allocate_manifest_sequence();

            let mut ver = self.version_set.lock().unwrap();

            wal.append(&VersionEdit::VersionChanged(ver.last_version));
            wal.append(&VersionEdit::ManifestSequenceChanged(ver.last_manifest_seq));
            wal.append(&VersionEdit::SSTSequenceChanged(ver.last_sst_seq));
            // finish old wal file

            wal.rotate(seq);
            // new wal file
            wal.append(&VersionEdit::Snapshot(ver.current()));
            wal.append(&VersionEdit::VersionChanged(ver.last_version));
            wal.append(&VersionEdit::ManifestSequenceChanged(ver.last_manifest_seq));
            wal.append(&VersionEdit::SSTSequenceChanged(ver.last_sst_seq));

            wal.sync();
            old_seq
        };
        {
            self.save_current_log();
        }
        {
            let wal = self.wal.lock().unwrap();
            wal.remove(old_seq);
        }
    }

    pub fn flush(&self) {
        self.rotate();
    }

    pub fn allocate_version(&self, num: u64) -> u64 {
        let mut ver = self.version_set.lock().unwrap();
        let return_ver = ver.last_version;
        ver.last_version += num;
        return_ver
    }

    pub fn set_latest_version(&self, version: u64) {
        let mut ver = self.version_set.lock().unwrap();
        ver.last_version = version;
    }

    pub fn allocate_sst_sequence(&self) -> u64 {
        let mut ver = self.version_set.lock().unwrap();
        let return_seq = ver.last_sst_seq;
        let edit = VersionEdit::SSTSequenceChanged(ver.last_sst_seq + 1);
        self.commit(&edit);

        ver.add(&edit);
        return_seq
    }

    pub fn last_sst_sequence(&self) -> u64 {
        let ver = self.version_set.lock().unwrap();
        ver.last_sst_seq
    }

    fn allocate_manifest_sequence(&self) -> u64 {
        let mut ver = self.version_set.lock().unwrap();
        let return_seq = ver.last_manifest_seq;
        ver.last_manifest_seq += 1;
        return_seq
    }

    pub fn add_sst_with<F: FnOnce(Arc<Version>)>(&self, meta: FileMetaData, new_run: bool, f: F) {
        if new_run {
            let edit = VersionEdit::NewRun(meta.level as u64);
            self.commit(&edit);
            let mut vs = self.version_set.lock().unwrap();
            vs.add(&edit);
        }
        let seq = meta.seq;
        let edit = VersionEdit::SSTAppended(meta);
        self.commit(&edit);
        let mut vs = self.version_set.lock().unwrap();
        vs.add(&edit);
        info!("add sst {} {:?}", seq, vs.current());

        f(vs.current());
    }

    pub fn modify_with<F: FnOnce(Arc<Version>)>(&self, versions: Vec<VersionEdit>, f: F) {
        let mut vs = self.version_set.lock().unwrap();
        for v in versions {
            self.wal.lock().unwrap().append(&v);
            vs.add(&v);
        }
        self.wal.lock().unwrap().sync();
        f(vs.current());
    }

    pub fn current(&self) -> VersionRef {
        let mut ver = self.version_set.lock().unwrap();
        ver.current()
    }

    pub fn new_snapshot<'a>(&'a self) -> SnapshotGuard<'a> {
        let mut ver = self.version_set.lock().unwrap();
        SnapshotGuard {
            snapshot: Snapshot::new(ver.current_snapshot_version()),
            manifest: self,
        }
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
    manifest: &'a Manifest,
}

impl<'a> SnapshotGuard<'a> {
    pub fn get(&self) -> Snapshot {
        self.snapshot.clone()
    }
}

impl<'a> Drop for SnapshotGuard<'a> {
    fn drop(&mut self) {
        self.manifest.release_snapshot(self.snapshot.version())
    }
}
