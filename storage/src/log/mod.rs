use std::{
    fs::{self, File},
    io::{Read, Write},
};

pub mod replayer;
pub mod wal;
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
pub use replayer::LogReplayer;
pub use wal::LogWriter;

pub struct LogFile {
    file: File,
    name: String,
}

impl LogFile {
    pub fn new(seq: u64, alloc_size: usize, path: &str) -> Self {
        let _ = fs::create_dir_all(path);
        let name = format!("{}/{}.log", path, seq);
        let file = File::create(&name).unwrap();
        file.set_len(alloc_size as u64).unwrap();
        Self { file, name }
    }

    pub fn open(seq: u64, path: &str) -> Option<Self> {
        let name = format!("{}/{}.log", path, seq);
        let file = File::open(&name).ok()?;
        Some(Self { file, name })
    }

    pub fn file(&mut self) -> &mut File {
        &mut self.file
    }

    pub fn remove(seq: u64, path: &str) {
        let name = format!("{}/{}.log", path, seq);
        let _ = fs::remove_file(&name);
    }

    fn make_sure(&mut self, alloc_size: usize) {
        self.file.set_len(alloc_size as u64).unwrap();
    }
}

pub trait LogEntrySerializer {
    type Entry;
    fn write<W>(&mut self, entry: &Self::Entry, w: W) -> u64
    where
        W: Write;
    fn read<R>(&mut self, r: R) -> Option<Self::Entry>
    where
        R: Read;
}

// default impl

impl LogEntrySerializer for String {
    type Entry = String;

    fn write<W>(&mut self, entry: &Self::Entry, mut w: W) -> u64
    where
        W: Write,
    {
        w.write_u32::<LE>(entry.len() as u32).unwrap();
        w.write_all(entry.as_bytes());
        4 + entry.len() as u64
    }

    fn read<R>(&mut self, mut r: R) -> Option<Self::Entry>
    where
        R: Read,
    {
        let len = r.read_u32::<LE>().unwrap();
        let mut vec = Vec::new();
        vec.resize(len as usize, 0);
        r.read_exact(&mut vec);
        unsafe { Some(String::from_utf8_unchecked(vec)) }
    }
}
