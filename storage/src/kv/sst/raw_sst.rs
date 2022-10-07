use bytes::Bytes;

use crate::kv::{KVReader, KvEntry};
use crate::value::Value;
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};

use super::SSTWriter;

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
}

impl RawSSTReader {
    pub fn new(name: &str) -> Self {
        let file = File::open(name).unwrap();
        let mmap = unsafe { memmap2::Mmap::map(&file).unwrap() };
        Self { mmap, file }
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
    flags: u8,
    ttl2_high: u8,
    ttl: u32,
    version: u64,
    key_len: u32,
    key_bytes: Bytes,
    value_len: u32,
    value_bytes: Bytes,
}

struct RawSSTMetaInfo {
    total_keys: u64,
    index_offset: u64,
    min_key: String,
    max_key: String,
    meta_offset: u64,
    level: u32,
    magic: u32,
}

impl RawSSTMetaInfo {
    // total_keys: u64
    // index_offset: u64
    // min_key_size: u32
    // max_key_size: u32
    // min_key
    // max_key
    // meta_offset: u64
    // magic: u32
    pub fn write<W: Write>(&self, mut w: W) {
        let buf = self.total_keys.to_le_bytes();
        w.write_all(&buf).unwrap();
        let buf = self.index_offset.to_le_bytes();
        w.write_all(&buf).unwrap();

        let size = self.min_key.len() as u32;
        let buf = size.to_le_bytes();
        w.write_all(&buf).unwrap();

        let size = self.max_key.len() as u32;
        let buf = size.to_le_bytes();
        w.write_all(&buf).unwrap();

        // min max key
        w.write_all(&self.min_key.as_bytes()).unwrap();
        w.write_all(&self.max_key.as_bytes()).unwrap();

        let buf = self.meta_offset.to_le_bytes();
        w.write_all(&buf).unwrap();

        let buf = self.level.to_le_bytes();
        w.write_all(&buf).unwrap();

        let buf = self.magic.to_le_bytes();
        w.write_all(&buf).unwrap();
    }

    pub fn read(&mut self) {}
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

impl RawSSTWriter {
    fn write_entry<W: Write>(entry: &KvEntry, mut w: W) -> usize {
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
}

impl SSTWriter for RawSSTWriter {
    fn write<'a, I>(&'a mut self, level: u32, iter: I)
    where
        I: Iterator<Item = &'a KvEntry>,
    {
        let mut w = BufWriter::new(&mut self.file);

        let mut min_key = String::new();
        let mut max_key = String::new();
        let mut last_entry = None;
        let mut keys_offset = Vec::new();

        for entry in iter {
            // write key value entry
            if min_key.len() == 0 {
                min_key = entry.key().to_owned();
            }

            let bytes = Self::write_entry(&entry, &mut w);
            last_entry = Some(entry);

            keys_offset.push(bytes);
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
            min_key: min_key,
            max_key: max_key,
            meta_offset: meta_offset_begin,
            level,
            magic: 0xA0230F00,
        };

        meta_info.write(&mut w);
        w.flush().unwrap();
    }
}
