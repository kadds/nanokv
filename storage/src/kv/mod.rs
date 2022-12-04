use std::io;
use std::sync::Arc;

use crate::iterator::KvIteratorItem;
use crate::{iterator::EqualKey, log::LogEntrySerializer, value::Value, GetOption, WriteOption};
use bitflags::bitflags;
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use bytes::Bytes;

#[derive(Debug)]
pub enum SetError {
    VersionNotMatch(u64),
    KeyNotExist,
    ValueTooLarge,
    WriteFail(io::Error),
}

pub type SetResult<T> = std::result::Result<T, SetError>;

// pub trait LogSerializer<W> {
//     fn serialize_log(&self, writer: &mut W);
// }
// pub trait LogDeserializer<R> {
//     fn deserialize_log(&mut self, reader: &mut R);
// }

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
    key: Bytes,
    flags: u64,
    ver: u64,
    value: Bytes,
}

impl KvEntry {
    pub fn new<K: Into<Bytes>, V: Into<Bytes>>(
        key: K,
        value: V,
        ttl: Option<u64>,
        ver: u64,
    ) -> Self {
        let mut flags = 0;
        if let Some(ttl) = ttl {
            flags |= Flags::TTL.bits;
            flags |= ttl << 8;
        }

        Self {
            key: key.into(),
            value: value.into(),
            flags,
            ver,
        }
    }

    pub fn new_del<K: Into<Bytes>>(key: K, ver: u64) -> Self {
        let flags = Flags::DEL.bits;

        Self {
            key: key.into(),
            value: Bytes::new(),
            flags,
            ver,
        }
    }

    pub fn from_search<K: Into<Bytes>>(key: K, ver: u64) -> Self {
        Self {
            key: key.into(),
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

    pub fn new_big_value(key: Bytes, value_offset: u64, ttl: Option<u64>, ver: u64) -> Self {
        let mut flags = Flags::EXTERN_VALUE.bits;
        if let Some(ttl) = ttl {
            flags |= Flags::TTL.bits;
            flags |= ttl << 8;
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
    pub fn key(&self) -> Bytes {
        self.key.clone()
    }
    pub fn value(&self) -> Bytes {
        self.value.clone()
    }
    pub fn bytes(&self) -> u64 {
        self.key.len() as u64 + self.value.len() as u64 + 16
    }
}

impl EqualKey for KvEntry {
    fn equal_key(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
impl EqualKey for &KvEntry {
    fn equal_key(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl KvIteratorItem for KvEntry {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn version(&self) -> u64 {
        self.ver
    }
}

impl KvIteratorItem for &KvEntry {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn version(&self) -> u64 {
        self.ver
    }
}

pub fn kv_entry_to_value(entry: KvEntry) -> (Bytes, Value) {
    (entry.key(), Value::from_entry(&entry))
}
pub fn kv_entry_ref_to_value(entry: &KvEntry) -> (Bytes, Value) {
    (entry.key(), Value::from_entry(entry))
}

#[derive(Debug, Default)]
pub struct KvEntryLogSerializer;

impl LogEntrySerializer for KvEntryLogSerializer {
    type Entry = KvEntry;

    fn write<W>(&self, entry: &Self::Entry, mut w: W) -> u64
    where
        W: io::Write,
    {
        w.write_u64::<LE>(entry.flags).unwrap();
        w.write_u64::<LE>(entry.ver).unwrap();
        w.write_u32::<LE>(entry.key.len() as u32).unwrap();
        w.write_all(&entry.key).unwrap();

        w.write_u32::<LE>(entry.value.len() as u32).unwrap();
        w.write_all(&entry.value).unwrap();

        8 + 8 + entry.key.len() as u64 + entry.value.len() as u64
    }

    fn read<R>(&self, mut r: R) -> Option<Self::Entry>
    where
        R: io::Read,
    {
        let flags = r.read_u64::<LE>().ok()?;
        let ver = r.read_u64::<LE>().ok()?;
        let key_len = r.read_u32::<LE>().ok()?;
        if key_len == 0 {
            return None;
        }

        let mut vec = Vec::new();
        vec.resize(key_len as usize, 0);
        r.read_exact(&mut vec).unwrap();
        let key = Bytes::from(vec);

        let value_len = r.read_u32::<LE>().unwrap();
        let mut vec = Vec::new();
        vec.resize(value_len as usize, 0);

        r.read_exact(&mut vec).unwrap();
        let value = Bytes::from(vec);

        Some(Self::Entry {
            flags,
            ver,
            key,
            value,
        })
    }
}

impl Eq for KvEntry {}

impl Ord for KvEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.key.cmp(&other.key) {
            std::cmp::Ordering::Equal => {}
            o => return o,
        }
        other.ver.cmp(&self.ver)
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
pub use imemtable::Imemtable;
pub use imemtable::Imemtables;
pub use memtable::Memtable;
pub mod superversion;

pub struct ColumnFamilyTables {
    pub memtable: Arc<Memtable>,
    pub imemtables: Imemtables,
}
