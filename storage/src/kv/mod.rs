use std::io;

use crate::value::Value;
use bitflags::bitflags;
use bytes::Bytes;

pub trait KVReader<'a> {
    fn get_ver(&self, key: &str, ver: u64) -> Option<Value>;

    fn get(&self, key: &str) -> Option<Value>;

    fn scan(&'a self, beg: &str, end: &str) -> Box<dyn Iterator<Item = (&'a str, Value)> + 'a>;

    fn iter(&'a self) -> Box<dyn Iterator<Item = (&'a str, Value)> + 'a>;
}

#[derive(Debug)]
pub enum SetError {
    VersionNotMatch(u64),
    KeyNotExist,
    ValueTooLarge,
    WriteFail(io::Error),
}

pub type SetResult<T> = std::result::Result<T, SetError>;

pub trait KVWriter {
    fn set(&mut self, entry: KvEntry) -> SetResult<()>;
}

pub trait LogSerializer<W> {
    fn serialize_log(&self, writer: &mut W);
}
pub trait LogDeserializer<R> {
    fn deserialize_log(&mut self, reader: &mut R);
}

// --------
// |  key | string
// |------|
// | flag | u8
// | ttl  | u48
// | ver  | u64
// |------|
// |value | bytes
//     or
// | pos  | u64

bitflags! {
    #[repr(transparent)]
    struct Flags: u64 {
        const TTL = 1;
        const EXTERN_VALUE = 2;
        const DEL = 4;
    }
}

#[derive(Debug, Clone)]
pub struct KvEntry {
    key: String,
    flags: u64,
    ver: u64,
    value: Bytes,
}

impl KvEntry {
    pub fn new(key: String, value: Bytes, ttl: Option<u64>, ver: u64) -> Self {
        let mut flags = 0;
        if let Some(ttl) = ttl {
            flags |= Flags::TTL.bits;
            flags = (ttl << 8) | flags;
        }

        Self {
            key,
            value,
            flags,
            ver,
        }
    }
    pub fn new_del(key: String, ver: u64) -> Self {
        let flags = Flags::DEL.bits;

        Self {
            key,
            value: Bytes::new(),
            flags,
            ver,
        }
    }

    pub fn from_search(key: String, ver: u64) -> Self {
        Self {
            key,
            value: Bytes::new(),
            flags: 0,
            ver,
        }
    }
}

impl KvEntry {
    pub fn equal_key(&self, rhs: &Self) -> bool {
        self.key == rhs.key
    }

    pub fn deleted(&self) -> bool {
        self.flags & Flags::DEL.bits != 0
    }

    pub fn big_value(&self) -> bool {
        self.flags & Flags::EXTERN_VALUE.bits != 0
    }

    pub fn flags(&self) -> u64 {
        self.flags
    }

    pub fn new_big_value(key: String, value_offset: u64, ttl: Option<u64>, ver: u64) -> Self {
        let mut flags = Flags::EXTERN_VALUE.bits;
        if let Some(ttl) = ttl {
            flags |= Flags::TTL.bits;
            flags = (ttl << 8) | flags;
        }
        let value = value_offset.to_le_bytes();

        Self {
            key,
            value: Bytes::copy_from_slice(&value),
            flags,
            ver,
        }
    }

    pub fn ttl(&self) -> Option<u64> {
        // milliseconds
        if self.flags & Flags::TTL.bits != 0 {
            Some(self.flags >> 8)
        } else {
            None
        }
    }

    pub fn version(&self) -> u64 {
        self.ver
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn value(&self) -> Bytes {
        self.value.clone()
    }
}

impl<W> LogSerializer<W> for KvEntry
where
    W: io::Write,
{
    fn serialize_log(&self, writer: &mut W) {
        let buf = self.flags.to_le_bytes();
        writer.write_all(&buf).unwrap();
        let buf = self.ver.to_le_bytes();
        writer.write_all(&buf).unwrap();
        let buf = self.key.len().to_le_bytes();
        writer.write_all(&buf).unwrap();
        let buf = self.value().len().to_le_bytes();
        writer.write_all(&buf).unwrap();

        // key
        writer.write_all(self.key.as_bytes()).unwrap();

        // value
        writer.write(&self.value).unwrap();
    }
}

impl Eq for KvEntry {}

impl Ord for KvEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.key == other.key {
            return self.version().cmp(&other.version());
        }
        other.key.cmp(&self.key)
    }
}

impl PartialEq for KvEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.ver == other.ver
    }
}

impl PartialOrd for KvEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.key.partial_cmp(&other.key) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        other.ver.partial_cmp(&self.ver)
    }
}

pub mod imemtable;
pub mod manifest;
pub mod memtable;
pub mod sst;
pub type Memtable = memtable::Memtable;
pub type Imemtable = imemtable::Imemtable;
