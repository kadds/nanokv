use byteorder::{ByteOrder, WriteBytesExt};
use bytes::{Buf, Bytes};
use integer_encoding::{VarIntReader, VarIntWriter};
use log::{debug, info};

use crate::iterator::{EqualFilter, ScanIter};
use crate::kv::superversion::Lifetime;
use crate::kv::{kv_entry_to_value, KvEntry};
use crate::value::Value;
use crate::KvIterator;
use byteorder::LE;
use std::fs::File;
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;

use super::{FileMetaData, SSTReader, SSTWriter};

const RAWSST_MAGIC: u32 = 0xA18C0001;

struct RawSSTIter<'a> {
    reader: &'a RawSSTReaderInner,
    beg: u64,
    end: u64,
    idx: u64,
}

impl<'a> Iterator for RawSSTIter<'a> {
    type Item = KvEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.end {
            return None;
        }
        let idx = self.idx;
        self.idx += 1;
        let val = self.reader.index(idx)?;
        Some(val.into())
    }
}

impl<'a> DoubleEndedIterator for RawSSTIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.idx <= self.beg {
            return None;
        }
        let idx = self.idx;
        self.idx -= 1;
        Some(self.reader.index(idx)?.into())
    }
}

impl<'a> KvIterator for RawSSTIter<'a> {
    fn prefetch(&mut self, _n: usize) {}
}

#[allow(unused)]
struct RawSSTReaderInner {
    mmap: memmap2::Mmap,
    file: File,
    size: u64,
    meta: RawSSTMetaInfo,
    seq: u64,
}

pub struct RawSSTReader {
    inner: Arc<RawSSTReaderInner>,
}

impl RawSSTReader {
    pub fn new(name: PathBuf) -> io::Result<Self> {
        let mut file = File::open(name)?;
        let size = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(0))?;

        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        let slice = mmap.get(0..size as usize).unwrap();

        let meta = RawSSTMetaInfo::read(slice)?;
        if meta.magic != RAWSST_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "magic invalid"));
        }
        log::info!("read meta {:?}", meta);

        Ok(Self {
            inner: RawSSTReaderInner {
                seq: meta.seq,
                mmap,
                file,
                meta,
                size,
            }
            .into(),
        })
    }

    pub fn meta(&self) -> RawSSTMetaInfo {
        self.inner.meta.clone()
    }

    pub fn get_index(&self, index: u64) -> Option<(Bytes, Value)> {
        let entry = self.inner.index(index)?;
        Some(kv_entry_to_value(entry.into()))
    }
}

impl RawSSTReaderInner {
    fn lower_bound(&self, key: &Bytes) -> u64 {
        let key = unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(key.as_ptr(), key.len()))
        };
        use std::cmp::Ordering::*;

        let mut size = self.meta.total_keys;
        if size == 0 {
            return 0;
        }
        let mut base = 0u64;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            let cmp = unsafe { self.index_key_unchecked(mid).cmp(key) };
            base = if cmp == Less { mid } else { base };
            size -= half;
        }
        let cmp = unsafe { self.index_key_unchecked(base).cmp(key) };
        base + (cmp == Less) as u64
    }

    fn upper_bound(&self, key: &Bytes) -> u64 {
        let key = unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(key.as_ptr(), key.len()))
        };
        use std::cmp::Ordering::*;

        let mut size = self.meta.total_keys;
        if size == 0 {
            return 0;
        }
        let mut base = 0u64;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            let cmp = unsafe { self.index_key_unchecked(mid).cmp(key) };
            base = if cmp == Greater { base } else { mid };
            size -= half;
        }
        let cmp = unsafe { self.index_key_unchecked(base).cmp(key) };
        base + (cmp != Greater) as u64
    }

    #[allow(unused)]
    fn offset(&self, offset: u64) -> RawSSTEntry {
        let slice = self.mmap.get(0..self.size as usize).unwrap();
        RawSSTEntry::read(&slice[offset as usize..])
    }

    fn index(&self, index: u64) -> Option<RawSSTEntry> {
        let slice = self.mmap.get(0..self.size as usize).unwrap();
        if index >= self.meta.total_keys {
            return None;
        }
        let offset = LE::read_u64(&slice[(self.meta.index_offset + index * 8) as usize..]);
        Some(RawSSTEntry::read(&slice[offset as usize..]))
    }

    #[allow(unused)]
    fn index_key(&self, index: u64) -> &str {
        let slice = self.mmap.get(0..self.size as usize).unwrap();
        let offset = LE::read_u64(&slice[(self.meta.index_offset + index * 8) as usize..]);
        RawSSTEntry::read_key(&slice[offset as usize..])
    }
    unsafe fn index_key_unchecked(&self, index: u64) -> &str {
        let slice = self.mmap.get(0..self.size as usize).unwrap_unchecked();
        let offset = LE::read_u64(&slice[(self.meta.index_offset + index * 8) as usize..]);
        RawSSTEntry::read_key(&slice[offset as usize..])
    }
}

impl SSTReader for RawSSTReader {
    fn get<'a>(
        &self,
        opt: &crate::GetOption,
        key: Bytes,
        lifetime: &Lifetime<'a>,
    ) -> Option<Value> {
        let ver = opt.snapshot().map(|v| v.version()).unwrap_or(u64::MAX);
        let mut index = self.inner.lower_bound(&key);
        loop {
            let entry = match self.inner.index(index) {
                Some(e) => e,
                None => break,
            };
            if entry.key_bytes == key {
                if entry.version <= ver {
                    return Some(Value::from_entry(&entry.into()));
                }
            } else {
                break;
            }

            index += 1;
        }
        None
    }

    fn scan<'a>(
        &self,
        opt: &crate::GetOption,
        beg: std::ops::Bound<bytes::Bytes>,
        end: std::ops::Bound<bytes::Bytes>,
        _mark: &Lifetime<'a>,
    ) -> ScanIter<'a, (bytes::Bytes, crate::Value)> {
        let beg = match beg {
            std::ops::Bound::Included(val) => self.inner.lower_bound(&val),
            std::ops::Bound::Excluded(val) => self.inner.upper_bound(&val),
            std::ops::Bound::Unbounded => 0,
        };
        let end = match end {
            std::ops::Bound::Included(val) => self.inner.upper_bound(&val),
            std::ops::Bound::Excluded(val) => self.inner.lower_bound(&val),
            std::ops::Bound::Unbounded => self.inner.meta.total_keys,
        };

        let reader = unsafe {
            core::mem::transmute::<_, &'static RawSSTReaderInner>(self.inner.clone().as_ref())
        };

        let iter = RawSSTIter {
            reader,
            beg,
            end,
            idx: beg,
        };

        if let Some(snapshot) = opt.snapshot() {
            let snapshot_ver = snapshot.version();
            let iter = iter.filter(move |entry| entry.version() <= snapshot_ver);
            ScanIter::new(EqualFilter::new(iter).map(kv_entry_to_value))
        } else {
            ScanIter::new(EqualFilter::new(iter).map(kv_entry_to_value))
        }
    }

    fn raw_scan<'a>(&self, lifetime: &Lifetime<'a>) -> ScanIter<'a, KvEntry> {
        let beg = 0;
        let end = self.inner.meta.total_keys;
        let reader = unsafe {
            core::mem::transmute::<_, &'static RawSSTReaderInner>(self.inner.clone().as_ref())
        };

        let iter = RawSSTIter {
            reader,
            beg,
            end,
            idx: beg,
        };

        ScanIter::new(EqualFilter::new(iter))
    }
}

// raw sst entry
//

#[allow(unused)]
struct RawSSTEntry {
    flags: u64,
    version: u64,
    key_len: u32,
    key_bytes: Bytes,
    value_len: u32,
    value_bytes: Bytes,
}

impl RawSSTEntry {
    fn write<W: Write>(entry: &KvEntry, mut w: W) -> usize {
        let mut total = 0;
        let buf = entry.flags().to_le_bytes();
        total += buf.len();
        w.write_all(&buf).unwrap();

        let buf = entry.version().to_le_bytes();
        total += buf.len();
        w.write_all(&buf).unwrap();

        // key
        let buf = (entry.key().len() as u32).to_le_bytes();
        total += buf.len();
        w.write_all(&buf).unwrap();
        total += entry.key.len();
        w.write_all(&entry.key()).unwrap();

        // value
        let buf = (entry.value().len() as u32).to_le_bytes();
        total += buf.len();
        w.write_all(&buf).unwrap();
        total += entry.value.len();
        w.write_all(&entry.value()).unwrap();

        total
    }

    fn read(buf: &[u8]) -> Self {
        let mut offset = 0;
        let flags = LE::read_u64(&buf[offset..]);
        offset += 8;
        let version = LE::read_u64(&buf[offset..]);
        offset += 8;
        let key_len = LE::read_u32(&buf[offset..]);
        offset += 4;

        let key_bytes = Bytes::copy_from_slice(&buf[offset..(offset + key_len as usize)]);
        offset += key_len as usize;
        let value_len = LE::read_u32(&buf[offset..]);
        offset += 4;
        let value_bytes = Bytes::copy_from_slice(&buf[offset..(offset + value_len as usize)]);

        Self {
            flags,
            version,
            key_len,
            key_bytes,
            value_len,
            value_bytes,
        }
    }

    fn read_key(buf: &[u8]) -> &str {
        let mut offset = 0;
        let _flags = LE::read_u64(&buf[offset..]);
        offset += 8;
        let _version = LE::read_u64(&buf[offset..]);
        offset += 8;
        let key_len = LE::read_u32(&buf[offset..]);
        offset += 4;
        // offset += key_len as usize;

        unsafe { std::str::from_utf8_unchecked(&buf[offset..(offset + key_len as usize)]) }
    }
}

impl Into<KvEntry> for RawSSTEntry {
    fn into(self) -> KvEntry {
        KvEntry {
            key: self.key_bytes,
            flags: self.flags,
            ver: self.version,
            value: self.value_bytes,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct RawSSTMetaInfo {
    pub seq: u64,
    pub level: u32,
    pub total_keys: u64,
    pub index_offset: u64,

    pub version: u32,
    pub meta_size: u32,
    pub magic: u32,
}

impl RawSSTMetaInfo {
    pub fn write<W: Write>(&mut self, mut w: W) -> io::Result<()> {
        let mut bytes = 0;
        bytes += w.write_varint(self.seq)?;
        bytes += w.write_varint(self.level)?;
        bytes += w.write_varint(self.total_keys)?;
        bytes += w.write_varint(self.index_offset)?;

        let meta_size = bytes as u32 + 12;
        self.meta_size = meta_size;

        w.write_u32::<LE>(self.version)?;
        w.write_u32::<LE>(meta_size)?;
        w.write_u32::<LE>(self.magic)?;

        Ok(())
    }

    pub fn read(slice: &[u8]) -> io::Result<Self> {
        // read tail first
        let len = slice.len();
        if len < 12 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid header"));
        }

        let magic = LE::read_u32(&slice[(len - 4)..]);
        let meta_size = LE::read_u32(&slice[(len - 8)..(len - 4)]);
        let version = LE::read_u32(&slice[(len - 12)..(len - 8)]);

        if len < meta_size as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid header meta size",
            ));
        }

        let mut reader = slice[(len - meta_size as usize)..].reader();
        let seq: u64 = reader.read_varint()?;
        let level: u32 = reader.read_varint()?;
        let total_keys: u64 = reader.read_varint()?;
        let index_offset: u64 = reader.read_varint()?;

        Ok(Self {
            seq,
            total_keys,
            index_offset,
            level,
            version,
            meta_size,
            magic,
        })
    }
}

pub struct RawSSTWriter {
    file: File,
}

impl RawSSTWriter {
    pub fn new(name: PathBuf) -> Self {
        let file = File::create(name).unwrap();
        Self { file }
    }
}

impl RawSSTWriter {}

impl SSTWriter for RawSSTWriter {
    fn write<I>(&mut self, level: u32, seq: u64, iter: I) -> FileMetaData
    where
        I: Iterator<Item = KvEntry>,
    {
        let mut w = BufWriter::new(&mut self.file);

        let mut min_key = Bytes::new();
        let mut max_key = Bytes::new();
        let mut last_entry = None;
        let mut keys_offset = Vec::new();

        let mut min_ver = u64::MAX;
        let mut max_ver = u64::MIN;

        let mut cur = 0;
        for entry in iter {
            // write key value entry
            if min_key.is_empty() {
                min_key = entry.key();
            }
            min_ver = min_ver.min(entry.version());
            max_ver = max_ver.max(entry.version());

            let bytes = RawSSTEntry::write(&entry, &mut w);
            last_entry = Some(entry);

            keys_offset.push(cur);
            cur += bytes;
        }
        if let Some(entry) = last_entry {
            max_key = entry.key();
        }
        let key_offset_begin = w.seek(SeekFrom::Current(0)).unwrap();

        // write key offset

        let keys = keys_offset.len() as u64;

        for key_offset in keys_offset {
            let k = key_offset as u64;
            let buf = k.to_le_bytes();
            w.write_all(&buf).unwrap();
        }

        let mut meta_info = RawSSTMetaInfo {
            seq,
            total_keys: keys,
            index_offset: key_offset_begin,
            level,
            version: 0,
            meta_size: 0,
            magic: RAWSST_MAGIC,
        };

        meta_info.write(&mut w).unwrap();
        w.flush().unwrap();

        debug!("write raw sst meta info {:?}", meta_info);

        FileMetaData::new(seq, min_key, max_key, min_ver, max_ver, keys, level)
    }
}
