use byteorder::{ByteOrder, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, Bytes, BytesMut};
use integer_encoding::{VarIntReader, VarIntWriter};
use log::debug;
use positioned_io::ReadBytesAtExt;

use crate::backend::fs::{ExtReader, ReadablePersist, WriteablePersist};
use crate::backend::Backend;
use crate::iterator::{EqualFilter, KvIteratorItem, ScanIter};
use crate::key::{InternalKey, Value};
use crate::kv::superversion::Lifetime;
use crate::{err::*, ConfigRef};
use crate::{Config, KvIterator};
use byteorder::LE;
use std::borrow::Borrow;
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
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
    type Item = (InternalKey, Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.end {
            return None;
        }
        let idx = self.idx;
        self.idx += 1;
        let res = match self.reader.index(idx) {
            Ok(v) => v.into(),
            Err(e) => {
                log::error!("error {:?} in iterator", e);
                return None;
            }
        };

        Some(res)
    }
}

impl<'a> DoubleEndedIterator for RawSSTIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.idx <= self.beg {
            return None;
        }
        let idx = self.idx;
        self.idx -= 1;
        let res = match self.reader.index(idx) {
            Ok(v) => v.into(),
            Err(e) => {
                log::error!("error {:?} in iterator", e);
                return None;
            }
        };

        Some(res)
    }
}

impl<'a> KvIterator for RawSSTIter<'a> {
    fn prefetch(&mut self, _n: usize) {}
}

pub struct RawSSTReaderFactory {}

#[allow(unused)]
struct RawSSTReaderInner {
    file: Box<dyn ReadablePersist>,
    size: u64,
    meta: RawSSTMetaInfo,
    seq: u64,
}

pub struct RawSSTReader {
    inner: Arc<RawSSTReaderInner>,
}

impl RawSSTReader {
    pub fn new(name: &Path, backend: &Backend, enable_mmap: bool) -> Result<Self> {
        let file = backend.fs.open(name, enable_mmap)?;
        let meta = RawSSTMetaInfo::read(file.borrow() as &dyn ReadablePersist, file.size())?;
        let size = file.size();

        if meta.magic != RAWSST_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "magic invalid").into());
        }
        log::info!("read meta {:?}", meta);

        Ok(Self {
            inner: RawSSTReaderInner {
                seq: meta.number,
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

    pub fn get_index(&self, index: u64) -> Result<(InternalKey, Value)> {
        let entry = self.inner.index(index)?;
        Ok(entry.into())
    }
}

impl RawSSTReaderInner {
    fn lower_bound(&self, key: &Bytes) -> Result<u64> {
        let key = unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(key.as_ptr(), key.len()))
        };
        use std::cmp::Ordering::*;

        let mut size = self.meta.total_keys;
        if size == 0 {
            return Ok(0);
        }
        let mut tmp = Vec::with_capacity(128);

        let mut base = 0u64;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            let cmp = unsafe { self.index_key_unchecked(mid, &mut tmp)?.cmp(key) };
            base = if cmp == Less { mid } else { base };
            size -= half;
        }
        let cmp = unsafe { self.index_key_unchecked(base, &mut tmp)?.cmp(key) };
        Ok(base + (cmp == Less) as u64)
    }

    fn upper_bound(&self, key: &Bytes) -> Result<u64> {
        let key = unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(key.as_ptr(), key.len()))
        };
        use std::cmp::Ordering::*;

        let mut size = self.meta.total_keys;
        if size == 0 {
            return Ok(0);
        }
        let mut tmp = Vec::with_capacity(128);

        let mut base = 0u64;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            let cmp = unsafe { self.index_key_unchecked(mid, &mut tmp)?.cmp(key) };
            base = if cmp == Greater { base } else { mid };
            size -= half;
        }
        let cmp = unsafe { self.index_key_unchecked(base, &mut tmp)?.cmp(key) };
        Ok(base + (cmp != Greater) as u64)
    }

    fn index(&self, index: u64) -> Result<RawSSTEntry> {
        if index >= self.meta.total_keys {
            return Err(StorageError::DataCorrupt);
        }
        let f = self.file.borrow() as &dyn ReadablePersist;
        let offset = f.read_u64_at::<LE>(self.meta.index_offset + index * 8)?;
        RawSSTEntry::read(ExtReader::new(f, offset, self.size))
    }

    unsafe fn index_key_unchecked<'a>(&self, index: u64, tmp: &'a mut Vec<u8>) -> Result<&'a str> {
        let f = self.file.borrow() as &dyn ReadablePersist;
        let offset = f.read_u64_at::<LE>(self.meta.index_offset + index * 8)?;
        RawSSTEntry::read_user_key(ExtReader::new(f, offset, self.size), tmp)
    }
}

impl SSTReader for RawSSTReader {
    fn get<'a>(
        &self,
        opt: &crate::GetOption,
        key: Bytes,
        _lifetime: &Lifetime<'a>,
    ) -> Result<(InternalKey, Value)> {
        let ver = opt.snapshot().map(|v| v.sequence()).unwrap_or(u64::MAX);
        let mut index = self.inner.lower_bound(&key)?;
        while index < self.inner.meta.total_keys {
            let (internal_key, value) = match self.inner.index(index) {
                Ok(e) => e.into(),
                Err(e) => return Err(e),
            };
            if internal_key.user_key() == key {
                if internal_key.seq() <= ver {
                    return Ok((internal_key, value));
                }
            } else {
                break;
            }

            index += 1;
        }
        Err(StorageError::KeyNotExist)
    }

    fn scan<'a>(
        &self,
        opt: &crate::GetOption,
        beg: std::ops::Bound<bytes::Bytes>,
        end: std::ops::Bound<bytes::Bytes>,
        _mark: &Lifetime<'a>,
    ) -> ScanIter<'a, (InternalKey, Value)> {
        let beg = match beg {
            std::ops::Bound::Included(val) => self.inner.lower_bound(&val).unwrap(),
            std::ops::Bound::Excluded(val) => self.inner.upper_bound(&val).unwrap(),
            std::ops::Bound::Unbounded => 0,
        };
        let end = match end {
            std::ops::Bound::Included(val) => self.inner.upper_bound(&val).unwrap(),
            std::ops::Bound::Excluded(val) => self.inner.lower_bound(&val).unwrap(),
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
            let snapshot_ver = snapshot.sequence();
            let iter = iter.filter(move |entry| entry.seq() <= snapshot_ver);
            ScanIter::new(EqualFilter::new(iter))
        } else {
            ScanIter::new(EqualFilter::new(iter))
        }
    }

    fn raw_scan<'a>(&self, _lifetime: &Lifetime<'a>) -> ScanIter<'a, (InternalKey, Value)> {
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
    key: InternalKey,
    value: Bytes,
}

impl Into<(InternalKey, Value)> for RawSSTEntry {
    fn into(self) -> (InternalKey, Value) {
        (self.key.into(), self.value.into())
    }
}

impl RawSSTEntry {
    fn write<W: Write>(key: &InternalKey, value: &Value, mut w: W) -> io::Result<u64> {
        let mut len = 0usize;
        len += w.write_varint(key.data().len())?;
        len += w.write_varint(value.data().len())?;
        w.write_all(key.data())?;
        len += key.data().len();
        w.write_all(value.data())?;
        len += value.data().len();
        Ok(len as u64)
    }

    fn read<R: Read>(mut r: R) -> Result<Self> {
        // read key length
        let key_len = r.read_varint::<usize>()?;
        let value_len = r.read_varint::<usize>()?;

        let mut key = BytesMut::new();
        key.resize(key_len, 0);
        r.read_exact(&mut key)?;
        let key = key.freeze();

        let mut value = BytesMut::new();
        value.resize(value_len, 0);
        r.read_exact(&mut value)?;
        let value = value.freeze();

        Ok(Self {
            key: key.into(),
            value,
        })
    }

    fn read_user_key<R: Read>(mut r: R, tmp: &mut Vec<u8>) -> Result<&str> {
        let key_len = r.read_varint::<usize>()? - 8;
        let _ = r.read_varint::<usize>()?;
        tmp.resize(key_len, 0);
        r.read_exact(tmp)?;

        Ok(unsafe { std::str::from_utf8_unchecked(&tmp[..(key_len as usize)]) })
    }
}

#[derive(Debug, Default, Clone)]
pub struct RawSSTMetaInfo {
    pub number: u64,
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
        bytes += w.write_varint(self.number)?;
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

    pub fn read(r: &dyn ReadablePersist, size: u64) -> io::Result<Self> {
        // read tail first
        if size < 12 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid header"));
        }
        let offset = size - 12;
        let version = r.read_u32_at::<LE>(offset)?;
        let meta_size = r.read_u32_at::<LE>(offset + 4)?;
        let magic = r.read_u32_at::<LE>(offset + 8)?;

        if size < meta_size as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid header meta size",
            ));
        }

        let offset = size - meta_size as u64;
        let mut rr = ExtReader::new(r, offset, size);
        let seq: u64 = rr.read_varint()?;
        let level: u32 = rr.read_varint()?;
        let total_keys: u64 = rr.read_varint()?;
        let index_offset: u64 = rr.read_varint()?;

        Ok(Self {
            number: seq,
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
    file: Box<dyn WriteablePersist>,
    name: PathBuf,
    success: bool,
}

impl RawSSTWriter {
    pub fn new(backend: &Backend, name: PathBuf) -> Self {
        let file = backend.fs.create(&name, None).unwrap();
        Self {
            file,
            name,
            success: false,
        }
    }
}

impl Drop for RawSSTWriter {
    fn drop(&mut self) {
        if !self.success {
            let _ = self.file.delete();
        }
    }
}

impl SSTWriter for RawSSTWriter {
    fn write<I>(&mut self, level: u32, number: u64, iter: I) -> Result<FileMetaData>
    where
        I: Iterator<Item = (InternalKey, Value)>,
    {
        self.success = false;
        let mut w = BufWriter::new(&mut self.file);

        let mut min_key = Bytes::new();
        let mut max_key = Bytes::new();
        let mut last_entry = None;
        let mut keys_offset = Vec::new();

        let mut min_ver = u64::MAX;
        let mut max_ver = u64::MIN;
        keys_offset.push(0);

        let mut cur = 0;
        for (internal_key, value) in iter {
            // write key value entry
            if min_key.is_empty() {
                min_key = internal_key.user_key();
            }
            min_ver = min_ver.min(internal_key.seq());
            max_ver = max_ver.max(internal_key.seq());

            cur += RawSSTEntry::write(&internal_key, &value, &mut w)?;
            last_entry = Some(internal_key);

            keys_offset.push(cur);
        }
        if let Some(entry) = last_entry {
            max_key = entry.user_key();
        }
        let key_offset_begin = cur;
        // write key offset

        let keys = keys_offset.len() as u64 - 1;

        for key_offset in keys_offset {
            let k = key_offset as u64;
            w.write_u64::<LE>(k)?;
        }

        let mut meta_info = RawSSTMetaInfo {
            number,
            total_keys: keys,
            index_offset: key_offset_begin,
            level,
            version: 0,
            meta_size: 0,
            magic: RAWSST_MAGIC,
        };

        meta_info.write(&mut w)?;
        w.flush().unwrap();

        debug!("write raw sst meta info {:?}", meta_info);
        self.success = true;

        Ok(FileMetaData::new(
            number, min_key, max_key, min_ver, max_ver, keys, level,
        ))
    }
}
