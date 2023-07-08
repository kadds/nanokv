use std::io::{self, Write};

use crate::{
    err::{Result, StorageError},
    iterator::KvIteratorItem,
    log::{replayer::SegmentRead, wal::SegmentWrite, LogEntrySerializer},
    WriteOption,
};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use bytes::{buf::Writer, Buf, BufMut, Bytes, BytesMut};
use num_enum::{IntoPrimitive, TryFromPrimitive};

#[derive(IntoPrimitive, TryFromPrimitive, Eq, PartialEq, Debug)]
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

        let _ = bytes.write(&user_key);
        let _ = bytes.write_u64::<LE>(value);
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

    pub fn len(&self) -> usize {
        self.bytes.len()
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
    seq: u64,
}

impl WriteBatch {
    pub fn iter(&self) -> BatchIter {
        BatchIter {
            inner: self,
            offset: 16,
        }
    }

    pub fn options(&self) -> &WriteOption {
        &self.option
    }

    pub fn data(&self) -> &Bytes {
        &self.bytes
    }

    pub fn seq(&self) -> u64 {
        self.seq
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
        let _ = bytes.write_u64::<LE>(0);
        let _ = bytes.write_u64::<LE>(0);

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

        let total_length = key.len() + value.len();

        let _ = self.bytes.write_u32::<LE>(total_length as u32);
        let _ = self.bytes.write_u32::<LE>(key.len() as u32);
        let _ = self.bytes.write_all(key.data());
        let _ = self.bytes.write_all(value);

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
        let mut b = self.bytes.into_inner();

        let mut data: [u8; 8] = [0; 8];
        let mut w = data.writer();
        let _ = w.write_u64::<LE>(self.total as u64);
        let _ = w.write_u64::<LE>(self.seq);
        let _ = w.flush();
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_mut_ptr(), b.as_mut().as_mut_ptr(), 8);
        }

        WriteBatch {
            bytes: b.freeze(),
            option: self.option,
            total: self.total,
            seq: self.seq,
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
        let mut reader = self.inner.bytes.slice(self.offset..).reader();
        let length = reader.read_u32::<LE>().ok()? as usize;
        let internal_key_length = reader.read_u32::<LE>().ok()? as usize;
        let offset = self.offset + 8; // total_length+key_length
        self.offset += length + 8;
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
pub struct BatchLogSerializer;

impl LogEntrySerializer for BatchLogSerializer {
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

        for _ in 0..count {
            // read length
            let length = r.read_u32::<LE>()?;
            let key_length = r.read_u32::<LE>()? as usize;

            let mut full_key_value = BytesMut::new();
            full_key_value.resize(length as usize, 0);
            r.read_exact(&mut full_key_value)?;

            let full_key_value = full_key_value.freeze();
            let key = full_key_value.slice(..key_length);
            let value = full_key_value.slice(key_length..);

            builder.add_internal(key.into(), value).unwrap();
        }
        builder.set_seq(seq);

        Ok(builder.build())
    }
}

#[cfg(test)]
mod test {
    use crate::log::{replayer::DummySegmentRead, wal::DummySegmentWrite};

    use super::*;

    #[test]
    pub fn log_serializer() {
        let mut batch_builder = WriteBatchBuilder::default();
        batch_builder.set("123", "abc").unwrap();
        batch_builder.del("456").unwrap();
        batch_builder.set("1", "a").unwrap();
        let batch = batch_builder.build();
        let mut buf = BytesMut::default().writer();
        let mut w = DummySegmentWrite::new(&mut buf);

        BatchLogSerializer::default().write(&batch, &mut w).unwrap();
        let buf = buf.into_inner().freeze();
        let mut r = DummySegmentRead::new(buf.reader());
        let batch2 = BatchLogSerializer::default().read(&mut r).unwrap();

        assert_eq!(batch.count(), batch2.count());
        assert_eq!(batch.data(), batch2.data());
    }

    #[test]
    pub fn internal_key() {
        let key = InternalKey::new("123", 456, KeyType::Del);
        assert_eq!(key.user_key_slice(), "123".as_bytes());
        assert_eq!(key.seq(), 456);
        assert_eq!(key.key_type(), KeyType::Del);
    }

    #[test]
    pub fn batch_key() {
        let mut batch_builder = WriteBatchBuilder::default();
        batch_builder.set("123", "abc").unwrap();
        batch_builder.del("456").unwrap();
        batch_builder.set("1", "a").unwrap();
        let batch = batch_builder.build();

        assert_eq!(batch.data().len(), 75);

        let mut iter = batch.iter();
        let (k, v) = iter.next().unwrap();
        assert_eq!(k.user_key_slice(), "123".as_bytes());
        assert_eq!(v, "abc");

        let (k, v) = iter.next().unwrap();
        assert_eq!(k.user_key_slice(), "456".as_bytes());
        assert_eq!(v, "");

        let (k, v) = iter.next().unwrap();
        assert_eq!(k.user_key_slice(), "1".as_bytes());
        assert_eq!(v, "a");

        assert!(iter.next().is_none());
    }
}
