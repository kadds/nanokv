use byteorder::ByteOrder;
use bytes::Bytes;

use crate::kv::{KVReader, KvEntry};
use crate::value::Value;
use byteorder::LE;
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};

use super::{FileMetaData, SSTWriter};

struct RawSSTIterator {}

impl Iterator for RawSSTIterator {
    type Item = KvEntry;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub struct RawSSTReader {
    mmap: memmap2::Mmap,
    file: File,
    size: u64,
    meta: RawSSTMetaInfo,
}

impl RawSSTReader {
    pub fn new(name: &str) -> Self {
        let mut file = File::open(name).unwrap();
        let size = file.seek(SeekFrom::End(0)).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();

        let mmap = unsafe { memmap2::Mmap::map(&file).unwrap() };
        let slice = mmap.get(0..size as usize).unwrap();

        let meta = RawSSTMetaInfo::read(slice);

        Self {
            mmap,
            file,
            meta,
            size,
        }
    }
}

impl RawSSTReader {
    fn lower_bound(&self, key: &str) -> u64 {
        let mut left = 0 as u64;
        let mut right = self.meta.total_keys;

        while left < right {
            let mid = left + (right - left) / 2;
            let mid_key = self.index_key(mid);
            if mid_key < key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        left
    }
    fn upper_bound(&self, key: &str) -> u64 {
        let mut left = 0 as u64;
        let mut right = self.meta.total_keys;

        while left < right {
            let mid = left + (right - left) / 2;
            let mid_key = self.index_key(mid);
            if mid_key < key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        right
    }

    fn offset(&self, offset: u64) -> RawSSTEntry {
        let slice = self.mmap.get(0..self.size as usize).unwrap();
        RawSSTEntry::read(&slice[offset as usize..])
    }

    fn index(&self, index: u64) -> RawSSTEntry {
        let slice = self.mmap.get(0..self.size as usize).unwrap();
        let offset = LE::read_u64(&slice[(self.meta.index_offset + index * 8) as usize..]);
        RawSSTEntry::read(&slice[offset as usize..])
    }
    fn index_key(&self, index: u64) -> &str {
        let slice = self.mmap.get(0..self.size as usize).unwrap();
        let offset = LE::read_u64(&slice[(self.meta.index_offset + index * 8) as usize..]);
        RawSSTEntry::read_key(&slice[offset as usize..])
    }
}

impl<'a> KVReader<'a> for RawSSTReader {
    fn get_ver(&self, key: &str, ver: u64) -> Option<Value> {
        todo!()
    }

    fn get(&self, key: &str) -> Option<Value> {
        todo!()
    }

    fn scan(&'a self, beg: &str, end: &str) -> Box<dyn Iterator<Item = (&'a str, Value)> + 'a> {
        todo!()
    }

    fn iter(&'a self) -> Box<dyn Iterator<Item = (&'a str, Value)> + 'a> {
        todo!()
    }
}

// raw sst entry
//

struct RawSSTEntry {
    flags: u64,
    version: u64,
    key_len: u32,
    key_bytes: Bytes,
    value_len: u32,
    value_bytes: Bytes,
}

impl RawSSTEntry {
    pub fn total_size(&self) -> u64 {
        self.key_len as u64 + self.value_len as u64 + 8 + 8
    }

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
        w.write_all(&entry.key().as_bytes()).unwrap();

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
        let flags = LE::read_u64(&buf[offset..]);
        offset += 8;
        let version = LE::read_u64(&buf[offset..]);
        offset += 8;
        let key_len = LE::read_u32(&buf[offset..]);
        // offset += 4;
        // offset += key_len as usize;

        unsafe { std::str::from_utf8_unchecked(&buf[offset..(offset + key_len as usize)]) }
    }
}

#[derive(Debug, Default)]
struct RawSSTMetaInfo {
    total_keys: u64,
    index_offset: u64,
    level: u32,
    version: u32,
    meta_offset: u64,
    magic: u32,
}

impl RawSSTMetaInfo {
    pub fn write<W: Write>(&self, mut w: W) {
        let buf = self.total_keys.to_le_bytes();
        w.write_all(&buf).unwrap();

        let buf = self.index_offset.to_le_bytes();
        w.write_all(&buf).unwrap();

        let buf = self.level.to_le_bytes();
        w.write_all(&buf).unwrap();

        let buf = self.version.to_le_bytes();
        w.write_all(&buf).unwrap();

        let buf = self.meta_offset.to_le_bytes();
        w.write_all(&buf).unwrap();

        let buf = self.magic.to_le_bytes();
        w.write_all(&buf).unwrap();
    }

    pub fn read(slice: &[u8]) -> Self {
        // read tail first
        let len = slice.len();
        assert!(len >= 12);
        let magic = LE::read_u32(&slice[(len - 4)..]);
        let meta_offset = LE::read_u64(&slice[(len - 12)..(len - 4)]);

        assert!(len as u64 >= meta_offset);
        let mut offset = meta_offset as usize;
        let total_keys = LE::read_u64(&slice[offset..]);
        offset += 8;
        let index_offset = LE::read_u64(&slice[offset..]);
        offset += 8;
        let level = LE::read_u32(&slice[offset..]);
        offset += 4;
        let version = LE::read_u32(&slice[offset..]);

        Self {
            total_keys,
            index_offset,
            level,
            version,
            meta_offset,
            magic,
        }
    }
}

pub struct RawSSTWriter {
    file: File,
}

impl RawSSTWriter {
    pub fn new(name: &str) -> Self {
        let file = File::create(name).unwrap();
        Self { file }
    }
}

impl RawSSTWriter {}

impl SSTWriter for RawSSTWriter {
    fn write<'a, I>(&'a mut self, level: u32, seq: u64, iter: I) -> FileMetaData
    where
        I: Iterator<Item = &'a KvEntry>,
    {
        let mut w = BufWriter::new(&mut self.file);

        let mut min_key = String::new();
        let mut max_key = String::new();
        let mut last_entry = None;
        let mut keys_offset = Vec::new();

        let mut min_ver = u64::MAX;
        let mut max_ver = u64::MIN;

        let mut cur = 0;
        for entry in iter {
            // write key value entry
            if min_key.len() == 0 {
                min_key = entry.key().to_owned();
            }
            min_ver = min_ver.min(entry.version());
            max_ver = max_ver.max(entry.version());

            let bytes = RawSSTEntry::write(&entry, &mut w);
            last_entry = Some(entry);

            keys_offset.push(cur);
            cur += bytes;
        }
        if let Some(entry) = last_entry {
            max_key = entry.key().to_owned();
        }
        let key_offset_begin = w.seek(SeekFrom::Current(0)).unwrap();

        // write key offset

        let keys = keys_offset.len() as u64;

        for key_offset in keys_offset {
            let k = key_offset as u64;
            let buf = k.to_le_bytes();
            w.write_all(&buf).unwrap();
        }

        // meta info
        let meta_offset_begin = w.seek(SeekFrom::Current(0)).unwrap();

        let meta_info = RawSSTMetaInfo {
            total_keys: keys,
            index_offset: key_offset_begin,
            level,
            version: 0,
            meta_offset: meta_offset_begin,
            magic: 0xA0230F00,
        };

        meta_info.write(&mut w);
        w.flush().unwrap();

        FileMetaData::new(seq, min_key, max_key, min_ver, max_ver, keys, level)
    }
}
