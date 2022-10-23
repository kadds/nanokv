use std::{
    borrow::Cow,
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, LinkedList},
    fs::{self, File},
    io::{Read, Write},
    sync::{Arc, Mutex},
};

use ::log::info;
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use bytes::Bytes;
use lru::LruCache;

use crate::{
    log::{self, LogEntrySerializer, LogWriter},
    snapshot::Snapshot,
    ConfigRef,
};

use super::sst::{raw_sst::RawSSTReader, sst_name};

#[derive(Debug, Clone)]
pub struct Version {
    id: u64,
    sst_files: Vec<Vec<Arc<FileMetaData>>>,
}

pub type VersionRef = Arc<Version>;

impl Default for Version {
    fn default() -> Self {
        let mut sst_files = Vec::new();
        sst_files.resize((MAX_LEVEL + 1) as usize, Vec::new());

        Self { id: 0, sst_files }
    }
}

impl Version {
    pub fn level_n(&self, n: usize) -> impl Iterator<Item = &FileMetaData> {
        self.sst_files[n].iter().map(|v| v.as_ref())
    }
}

#[derive(Debug, Default)]
pub struct VersionLogSerializer;

impl LogEntrySerializer for VersionLogSerializer {
    type Entry = Version;

    fn write<W>(&mut self, entry: &Self::Entry, mut w: W) -> u64
    where
        W: Write,
    {
        let total_iter = entry.sst_files.iter().flatten();
        let total_files = total_iter.clone().count();
        w.write_u32::<LE>(total_files as u32).unwrap();

        let mut sum = 0;
        for file in total_iter {
            let mut s = FileMetaDataLogSerializer::default();
            sum += s.write(file, &mut w);
        }

        3 * 8 + 4 + sum
    }

    fn read<R>(&mut self, mut r: R) -> Option<Self::Entry>
    where
        R: Read,
    {
        let total_files = r.read_u32::<LE>().unwrap();

        let mut entry = Self::Entry::default();
        entry.sst_files.resize((MAX_LEVEL + 1) as usize, Vec::new());

        for _ in 0..total_files {
            let mut s = FileMetaDataLogSerializer::default();
            let f = s.read(&mut r)?;
            if f.level >= MAX_LEVEL {
                panic!("level not match {}", f.level);
            }
            entry.sst_files[f.level as usize].push(Arc::new(f));
        }
        Some(entry)
    }
}

pub enum VersionEdit {
    SSTAppended(Arc<FileMetaData>),
    SSTRemove(u64),
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

#[derive(Debug, Default)]
pub struct FileMetaDataLogSerializer;

impl LogEntrySerializer for FileMetaDataLogSerializer {
    type Entry = FileMetaData;

    fn write<W>(&mut self, entry: &Self::Entry, mut w: W) -> u64
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

    fn read<R>(&mut self, mut r: R) -> Option<Self::Entry>
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

    fn write<W>(&mut self, entry: &Self::Entry, mut w: W) -> u64
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
        }
    }

    fn read<R>(&mut self, mut r: R) -> Option<Self::Entry>
    where
        R: Read,
    {
        let ty = r.read_u8().unwrap();
        match ty {
            1 => {
                let mut s = FileMetaDataLogSerializer::default();
                let meta = s.read(r)?;
                Some(VersionEdit::SSTAppended(Arc::new(meta)))
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
    pub fn add(&mut self, edit: &VersionEdit) {
        match edit {
            VersionEdit::SSTAppended(file) => {
                // TODO: binary insert
                // version.sst_files[file.level as usize].binary_search_by(|f| {f.min < file.min});
                self.version.sst_files[file.level as usize].push(file.clone());
            }
            VersionEdit::SSTRemove(seq) => {
                for level_files in &mut self.version.sst_files {
                    if let Some((idx, _)) = level_files
                        .iter()
                        .enumerate()
                        .find(|(_, val)| val.seq == *seq)
                    {
                        level_files.remove(idx);
                    }
                }
            }
            VersionEdit::VersionChanged(ver) => {
                self.last_version = ver.clone();
            }
            VersionEdit::SSTSequenceChanged(seq) => {
                self.last_sst_seq = seq.clone();
            }
            VersionEdit::ManifestSequenceChanged(seq) => {
                self.last_manifest_seq = seq.clone();
            }
            VersionEdit::Snapshot(ver) => self.current.push_front(ver.clone()),
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
        let mut insert = true;
        match self.snapshot_versions.get_mut(&ver) {
            Some(v) => {
                *v += 1;
                insert = false;
            }
            None => (),
        };
        if insert {
            self.snapshot_versions.insert(ver, 1);
        }
        ver
    }

    pub fn release_snapshot_version(&mut self, ver: u64) {
        let mut del = false;
        match self.snapshot_versions.get_mut(&ver) {
            Some(v) => {
                if *v != 0 {
                    *v -= 1;
                } else {
                    del = true;
                }
            }
            None => return,
        };
        if del {
            self.snapshot_versions.remove(&ver);
        }
    }
}

pub fn manifest_path(base: &str) -> String {
    return format!("{}/manifest/", base);
}

pub const MAX_LEVEL: u32 = 8;

pub struct Manifest {
    config: ConfigRef,
    version_set: Mutex<VersionSet>,
    current_filename: String,
    tmp_filename: String,
    wal: Mutex<LogWriter<VersionEdit, ManifestLogSerializer>>,
}

impl Manifest {
    pub fn new(config: ConfigRef) -> Self {
        // replay version log
        let version_set = Mutex::new(VersionSet::default());
        let path = manifest_path(&config.path);
        fs::create_dir_all(&path).unwrap();

        let current_filename = format!("{}current", path);
        let tmp_filename = format!("{}current_tmp", path);

        let seq = Self::load_current_log_sequence(&current_filename).unwrap_or_default();

        let mut this = Self {
            config,
            version_set,
            current_filename,
            tmp_filename,
            wal: Mutex::new(LogWriter::new(
                ManifestLogSerializer::default(),
                seq + 1,
                path,
            )),
        };

        this.restore_from_wal(seq);
        this.save_current_log();

        this.version_set.lock().unwrap().last_manifest_seq = seq + 2;
        this.remove_unused_wal(seq + 1);

        // TODO: reload global version from wal
        this
    }

    pub fn load_current_log_sequence(file: &str) -> Option<u64> {
        let mut buf = String::new();
        let _ = File::open(file).map(|mut f| f.read_to_string(&mut buf));
        buf.parse().ok()
    }

    pub fn save_current_log(&self) {
        let seq = self.wal.lock().unwrap().seq();
        let _ = File::create(&self.tmp_filename)
            .unwrap()
            .write(seq.to_string().as_bytes());

        let _ = fs::rename(&self.tmp_filename, &self.current_filename).unwrap();
    }

    pub fn current_log_sequence(&self) -> Option<u64> {
        Self::load_current_log_sequence(&self.current_filename)
    }

    fn restore_from_wal(&mut self, seq: u64) {
        let path = manifest_path(&self.config.path);
        let s = ManifestLogSerializer::default();
        let mut replayer = log::LogReplayer::new(s, seq, path);
        let final_state = replayer.execute(VersionSet::default(), |state, edit| {
            state.add(&edit);
        });
        info!("load manifest {} {:?}", seq, final_state);
        *self.version_set.lock().unwrap() = final_state;
    }

    fn remove_unused_wal(&self, except_seq: u64) {}
}

impl Manifest {
    fn commit(&self, edit: &VersionEdit) {
        self.wal.lock().unwrap().append(&edit);
        self.wal.lock().unwrap().sync();
    }

    fn rotate(&self) {
        let mut wal = self.wal.lock().unwrap();
        let old_seq = wal.seq();
        let seq = self.allocate_manifest_sequence();

        let mut ver = self.version_set.lock().unwrap();

        wal.append(&VersionEdit::VersionChanged(ver.last_version));
        wal.append(&VersionEdit::ManifestSequenceChanged(ver.last_manifest_seq));
        wal.append(&VersionEdit::SSTSequenceChanged(ver.last_sst_seq));

        {
            wal.rotate(seq);
            // new wal file
            wal.append(&VersionEdit::Snapshot(ver.current()));
            wal.append(&VersionEdit::VersionChanged(ver.last_version));
            wal.append(&VersionEdit::ManifestSequenceChanged(ver.last_manifest_seq));
            wal.append(&VersionEdit::SSTSequenceChanged(ver.last_sst_seq));

            wal.sync();
        }
        let _ = File::create(&self.current_filename)
            .unwrap()
            .write(seq.to_string().as_bytes());
        wal.remove(old_seq);
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

    pub fn add_sst(&self, meta: Arc<FileMetaData>) {
        let edit = VersionEdit::SSTAppended(meta);
        self.commit(&edit);
        self.version_set.lock().unwrap().add(&edit);
    }

    pub fn remove_sst(&self, seq: u64) {
        let edit = VersionEdit::SSTRemove(seq);
        self.commit(&edit);
        self.version_set.lock().unwrap().add(&edit);
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
            .map(|(k, v)| *k)
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
