use std::{
    fs::{self, File},
    io::{Read, Write},
};

pub mod replayer;
pub mod wal;
use bitflags::bitflags;
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
pub use replayer::LogReplayer;
pub use wal::LogWriter;

pub struct LogFile {
    file: File,
    name: String,
}

const SEGMENT_SIZE: usize = 1024 * 32; // 32k
const SEGMENT_CONTENT_SIZE: usize = SEGMENT_SIZE - 4 - 2 - 1;

// crc u32
// length u16
// flags u8
// bytes

bitflags! {
    struct LogSegmentFlags: u8 {
        const UNKNOWN = 0b000;
        const FULL_FRAGMENT = 0b001;
        const BEGIN_FRAGMENT = 0b010;
        const MIDDLE_FRAGMENT = 0b011;
        const LAST_FRAGMENT = 0b100;
    }
}

impl LogFile {
    pub fn new(seq: u64, alloc_size: usize, prefix: &str) -> Self {
        let name = format!("{}{}.log", prefix, seq);
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
        w.write_all(entry.as_bytes()).unwrap();
        4 + entry.len() as u64
    }

    fn read<R>(&mut self, mut r: R) -> Option<Self::Entry>
    where
        R: Read,
    {
        let len = r.read_u32::<LE>().unwrap();
        let mut vec = Vec::new();
        vec.resize(len as usize, 0);
        r.read_exact(&mut vec).unwrap();
        unsafe { Some(String::from_utf8_unchecked(vec)) }
    }
}
