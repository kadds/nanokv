use std::io::{Read, Write};

pub mod replayer;
pub mod wal;
use bitflags::bitflags;
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
pub use replayer::LogReplayer;
pub use wal::LogWriter;

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

pub trait LogEntrySerializer {
    type Entry;
    fn write<W>(&self, entry: &Self::Entry, w: W) -> u64
    where
        W: Write;
    fn read<R>(&self, r: R) -> Option<Self::Entry>
    where
        R: Read;
}

// default impl

impl LogEntrySerializer for String {
    type Entry = String;

    fn write<W>(&self, entry: &Self::Entry, mut w: W) -> u64
    where
        W: Write,
    {
        w.write_u32::<LE>(entry.len() as u32).unwrap();
        w.write_all(entry.as_bytes()).unwrap();
        4 + entry.len() as u64
    }

    fn read<R>(&self, mut r: R) -> Option<Self::Entry>
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
