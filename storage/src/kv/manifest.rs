use std::{
    fs::File,
    io::{Read, Write},
    sync::Mutex,
};

use ::log::info;
use byteorder::{ReadBytesExt, WriteBytesExt, LE};

use crate::{
    log::{self, LogEntrySerializer, LogWriter},
    ConfigRef,
};

#[derive(Debug, Clone)]
pub struct Version {
    sst_files: Vec<Vec<FileMetaData>>,
    last_version: u64,
    last_sst_seq: u64,
    last_manifest_seq: u64,
}

impl Default for Version {
    fn default() -> Self {
        let mut sst_files = Vec::new();
        sst_files.resize((MAX_LEVEL + 1) as usize, Vec::new());

        Self {
            sst_files,
            last_version: 0,
            last_sst_seq: 0,
            last_manifest_seq: 0,
        }
    }
}

impl Version {
    pub fn add(&mut self, edit: VersionEdit) {
        match edit {
            VersionEdit::SSTAppended(file) => {
                // TODO: binary insert
                self.sst_files[file.level as usize].push(file);
            }
            VersionEdit::SSTRemove(seq) => {
                for level_files in &mut self.sst_files {
                    if let Some((idx, _)) = level_files
                        .iter()
                        .enumerate()
                        .find(|(idx, val)| val.seq == seq)
                    {
                        level_files.remove(idx);
                    }
                }
            }
            VersionEdit::VersionChanged(ver) => {
                self.last_version = ver;
            }
            VersionEdit::SSTSequenceChanged(seq) => {
                self.last_sst_seq = seq;
            }
            VersionEdit::ManifestSequenceChanged => {}
            VersionEdit::Snapshot(ver) => {
                *self = ver;
            }
        }
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
        w.write_u64::<LE>(entry.last_version).unwrap();
        w.write_u64::<LE>(entry.last_sst_seq).unwrap();
        w.write_u64::<LE>(entry.last_manifest_seq).unwrap();
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
        let last_version = r.read_u64::<LE>().unwrap();
        let last_sst_seq = r.read_u64::<LE>().unwrap();
        let last_manifest_seq = r.read_u64::<LE>().unwrap();
        let total_files = r.read_u32::<LE>().unwrap();

        let mut entry = Self::Entry::default();
        entry.last_version = last_version;
        entry.last_sst_seq = last_sst_seq;
        entry.last_manifest_seq = last_manifest_seq;
        entry.sst_files.resize((MAX_LEVEL + 1) as usize, Vec::new());

        for _ in 0..total_files {
            let mut s = FileMetaDataLogSerializer::default();
            let f = s.read(&mut r)?;
            if f.level >= MAX_LEVEL {
                panic!("level not match");
            }
            entry.sst_files[f.level as usize].push(f);
        }
        Some(entry)
    }
}

pub enum VersionEdit {
    SSTAppended(FileMetaData),
    SSTRemove(u64),
    VersionChanged(u64),
    SSTSequenceChanged(u64),
    ManifestSequenceChanged,
    Snapshot(Version),
}

#[derive(Debug, Clone)]
pub struct FileMetaData {
    pub seq: u64,

    pub min: String,
    pub max: String,
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
        min: String,
        max: String,
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
        w.write_all(entry.min.as_bytes()).unwrap();
        w.write_u32::<LE>(entry.max.len() as u32).unwrap();
        w.write_all(entry.max.as_bytes()).unwrap();
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
        let min_key = unsafe { String::from_utf8_unchecked(vec) };

        let max_key_len = r.read_u32::<LE>().unwrap();
        let mut vec = Vec::new();
        vec.resize(max_key_len as usize, 0);
        r.read_exact(&mut vec).unwrap();
        let max_key = unsafe { String::from_utf8_unchecked(vec) };

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
            VersionEdit::ManifestSequenceChanged => {
                w.write_u8(5).unwrap();
                1
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
                Some(VersionEdit::SSTAppended(meta))
            }
            2 => Some(VersionEdit::SSTRemove(r.read_u64::<LE>().ok()?)),
            3 => Some(VersionEdit::VersionChanged(r.read_u64::<LE>().ok()?)),
            4 => Some(VersionEdit::SSTSequenceChanged(r.read_u64::<LE>().ok()?)),
            5 => Some(VersionEdit::ManifestSequenceChanged),
            6 => {
                let mut s = VersionLogSerializer::default();
                Some(VersionEdit::Snapshot(s.read(&mut r)?))
            }
            _ => None,
        }
    }
}

pub struct VersionSet {}

pub const MAX_LEVEL: u32 = 8;

pub struct Manifest {
    config: ConfigRef,
    version: Mutex<Version>,
    current_filename: String,
    wal: Mutex<LogWriter<VersionEdit, ManifestLogSerializer>>,
}

impl Manifest {
    pub fn new(config: ConfigRef) -> Self {
        // replay version log
        let version = Mutex::new(Version::default());
        let current_filename = format!("{}/manifest/current", config.path);
        let seq = Self::load_current(&current_filename).unwrap_or_default();
        let wal_path = format!("{}/manifest/", config.path);

        let mut this = Self {
            config,
            version,
            current_filename,
            wal: Mutex::new(LogWriter::new(
                ManifestLogSerializer::default(),
                seq + 1,
                wal_path,
            )),
        };

        this.load_snapshot(seq);
        this.save_current();

        // TODO: reload global version from wal
        this
    }

    pub fn load_current(file: &str) -> Option<u64> {
        let mut buf = String::new();
        let _ = File::open(file).map(|mut f| f.read_to_string(&mut buf));
        buf.parse().ok()
    }

    pub fn save_current(&self) {
        let seq = self.wal.lock().unwrap().seq();
        let _ = File::create(&self.current_filename)
            .unwrap()
            .write(seq.to_string().as_bytes());
    }

    pub fn current(&self) -> Option<u64> {
        Self::load_current(&self.current_filename)
    }

    fn load_snapshot(&mut self, seq: u64) {
        let path = format!("{}/manifest", self.config.path);
        let s = ManifestLogSerializer::default();
        let mut replayer = log::LogReplayer::new(s, seq, path);
        let final_state = replayer.execute(Version::default(), |state, edit| {
            state.add(edit);
        });
        info!("load manifest {:?}", final_state);
        *self.version.lock().unwrap() = final_state;
    }
}

impl Manifest {
    fn commit(&self, edit: &VersionEdit) {
        self.wal.lock().unwrap().append(&edit);
    }

    fn make_snapshot(&self) {
        let ver = self.version.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        let old_seq = wal.seq();
        let seq = self.allocate_manifest_sequence();
        wal.rotate(seq);
        self.commit(&VersionEdit::Snapshot(ver.clone()));

        wal.remove(old_seq);
    }

    pub fn flush(&self) {
        self.make_snapshot();
    }

    pub fn allocate_version(&self, num: u64) -> u64 {
        let mut ver = self.version.lock().unwrap();
        let return_ver = ver.last_version;
        ver.last_version += num;
        return_ver
    }

    pub fn set_latest_version(&self, version: u64) {
        let mut ver = self.version.lock().unwrap();
        ver.last_version = version;
    }

    pub fn allocate_sst_sequence(&self) -> u64 {
        let mut ver = self.version.lock().unwrap();
        let return_seq = ver.last_sst_seq;
        ver.last_sst_seq += 1;

        self.commit(&VersionEdit::SSTSequenceChanged(ver.last_sst_seq));
        return_seq
    }

    pub fn last_sst_sequence(&self) -> u64 {
        let ver = self.version.lock().unwrap();
        ver.last_sst_seq
    }

    fn allocate_manifest_sequence(&self) -> u64 {
        let mut ver = self.version.lock().unwrap();
        let return_seq = ver.last_manifest_seq;
        ver.last_manifest_seq += 1;
        return_seq
    }

    pub fn add_sst(&self, meta: FileMetaData) {
        let edit = VersionEdit::SSTAppended(meta);
        self.commit(&edit);
    }
}
