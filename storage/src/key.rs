use std::io::{self, Write};

use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use bytes::{buf::Writer, Buf, BufMut, Bytes, BytesMut};
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::{
    err::{Result, StorageError},
    iterator::KvIteratorItem,
    log::{replayer::SegmentRead, wal::SegmentWrite, LogEntrySerializer},
    WriteOption,
};

#[derive(IntoPrimitive, TryFromPrimitive, Eq, PartialEq)]
#[repr(u8)]
pub enum KeyType {
    Set = 0,
    Del = 1,
}

// user_key
// seq
// type
#[derive(Debug, PartialEq, Eq, Ord, Clone)]
pub struct InternalKey {
    bytes: Bytes,
}

impl From<Bytes> for InternalKey {
    fn from(value: Bytes) -> Self {
        Self { bytes: value }
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.user_key()
            .partial_cmp(&other.user_key())
            .and_then(|v| match v {
                std::cmp::Ordering::Equal => other.seq().partial_cmp(&self.seq()),
                v => Some(v),
            })
    }
}

impl InternalKey {
    pub fn new<B: AsRef<[u8]>>(user_key: B, seq: u64, ty: KeyType) -> Self {
        let mut bytes = BytesMut::new().writer();

        let user_key = user_key.as_ref();
        let prefix: u8 = ty.into();
        let value = ((prefix as u64) << 56) | (seq & 0xFFFF_FFFF_FFFF);

        bytes.write(&user_key);
        bytes.write_u64::<LE>(value);
        Self {
            bytes: bytes.into_inner().freeze(),
        }
    }

    pub fn key_type(&self) -> KeyType {
        let bytes = self.bytes.slice((self.bytes.len() - 8)..);
        let tail = bytes.reader().read_u64::<LE>().unwrap();
        KeyType::try_from((tail >> 56) as u8).unwrap()
    }

    pub fn user_key_slice(&self) -> &[u8] {
        &self.bytes[..(self.bytes.len() - 8)]
    }

    pub fn data(&self) -> &[u8] {
        &self.bytes
    }
}

impl KvIteratorItem for InternalKey {
    fn user_key_slice(&self) -> &[u8] {
        &self.bytes[..(self.bytes.len() - 8)]
    }
    fn seq(&self) -> u64 {
        let bytes = self.bytes.slice((self.bytes.len() - 8)..);
        let tail = bytes.reader().read_u64::<LE>().unwrap();
        tail & 0xFFFF_FFFF_FFFF
    }
    fn deleted(&self) -> bool {
        self.key_type() == KeyType::Del
    }

    fn user_key(&self) -> Bytes {
        self.bytes.slice(..(self.bytes.len() - 8))
    }
}

impl KvIteratorItem for (InternalKey, Value) {
    fn user_key_slice(&self) -> &[u8] {
        self.0.user_key_slice()
    }

    fn seq(&self) -> u64 {
        self.0.seq()
    }

    fn deleted(&self) -> bool {
        self.0.deleted()
    }

    fn user_key(&self) -> Bytes {
        self.0.user_key()
    }
}

// count u64
// seq u64
// -------------------
// length
// internal_key_length
// internal_key
// value
pub struct WriteBatch {
    bytes: Bytes,
    option: WriteOption,
    total: u32,
}

impl WriteBatch {
    pub fn iter(&self) -> BatchIter {
        BatchIter {
            inner: self,
            offset: 0,
        }
    }

    pub fn options(&self) -> &WriteOption {
        &self.option
    }

    pub fn data(&self) -> &Bytes {
        &self.bytes
    }

    pub fn count(&self) -> usize {
        self.total as usize
    }
}

pub struct WriteBatchBuilder {
    option: WriteOption,
    bytes: Writer<BytesMut>,
    total: u32,
    seq: u64,
}

impl Default for WriteBatchBuilder {
    fn default() -> Self {
        Self::new(WriteOption::default())
    }
}

impl WriteBatchBuilder {
    pub fn new(option: WriteOption) -> Self {
        let mut bytes = BytesMut::with_capacity(128).writer();
        // skip 16 bytes
        bytes.write_u64::<LE>(0);
        bytes.write_u64::<LE>(0);

        Self {
            option,
            bytes,
            total: 0,
            seq: 0,
        }
    }

    pub fn add_internal<B: AsRef<[u8]>>(&mut self, key: InternalKey, value: B) -> Result<()> {
        let value = value.as_ref();

        if value.len() > 1024 * 1024 * 10 {
            // 10MB value
            return Err(StorageError::ValueTooLarge);
        }

        let total_length = key.data().len() + value.len();

        self.bytes.write_u32::<LE>(total_length as u32);
        self.bytes.write_u32::<LE>(key.data().len() as u32);
        self.bytes.write(key.data());
        self.bytes.write(value);

        self.total += 1;
        Ok(())
    }

    pub fn set<A: AsRef<[u8]>, B: AsRef<[u8]>>(&mut self, key: A, value: B) -> Result<()> {
        let internal_key = InternalKey::new(key, 0, KeyType::Set);
        self.add_internal(internal_key, value)
    }

    pub fn del<A: AsRef<[u8]>>(&mut self, key: A) -> Result<()> {
        let internal_key = InternalKey::new(key, 0, KeyType::Del);
        self.add_internal(internal_key, Bytes::default())
    }

    pub fn set_seq(&mut self, seq: u64) {
        self.seq = seq;
    }

    pub fn build(self) -> WriteBatch {
        let b = self.bytes.into_inner();
        b.clone().writer().write_u64::<LE>(self.total as u64);
        b.clone().writer().write_u64::<LE>(self.seq);

        WriteBatch {
            bytes: b.freeze(),
            option: self.option,
            total: self.total,
        }
    }
}

pub struct BatchIter<'a> {
    inner: &'a WriteBatch,
    offset: usize,
}

impl<'a> Iterator for BatchIter<'a> {
    type Item = (InternalKey, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        let mut reader = self.inner.bytes.slice(self.offset..).clone().reader();
        let offset = self.offset + 8;
        let length = reader.read_u32::<LE>().ok()? as usize;
        let internal_key_length = reader.read_u32::<LE>().ok()? as usize;
        self.offset += length;
        Some((
            self.inner
                .bytes
                .slice(offset..(offset + internal_key_length))
                .into(),
            self.inner
                .bytes
                .slice((offset + internal_key_length)..(offset + length)),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct Value {
    bytes: Bytes,
}

impl Value {
    pub fn internal(&self) -> Bytes {
        self.bytes.clone()
    }
    pub fn data(&self) -> &[u8] {
        &self.bytes
    }
}

impl From<Bytes> for Value {
    fn from(value: Bytes) -> Self {
        Self { bytes: value }
    }
}

#[derive(Debug, Default)]
pub struct WriteBatchLogSerializer;

impl LogEntrySerializer for WriteBatchLogSerializer {
    type Entry = WriteBatch;

    fn write<W>(&self, entry: &Self::Entry, w: &mut W) -> io::Result<()>
    where
        W: SegmentWrite,
    {
        w.write_all(&entry.bytes).map(|_| ())
    }

    fn read<R>(&self, r: &mut R) -> io::Result<Self::Entry>
    where
        R: SegmentRead,
    {
        let mut builder = WriteBatchBuilder::new(WriteOption::default());

        // read 16 header
        let count = r.read_u64::<LE>()?;
        let seq = r.read_u64::<LE>()?;

        for index in 0..count {
            // read length
            let length = r.read_u32::<LE>()?;
            let mut full_key_value = BytesMut::new();
            full_key_value.resize(length as usize, 0);
            r.read_exact(&mut full_key_value)?;
            let full_key_value = full_key_value.freeze();
            let mut r = full_key_value.clone().reader();

            let key_length = r.read_u32::<LE>()? as usize;
            let key = full_key_value.slice(..key_length);
            let value = full_key_value.slice(key_length..);

            builder.add_internal(key.into(), value);
        }

        Ok(builder.build())
    }
}
