use std::{mem::swap, rc::Rc, time::Duration};

use bytes::Bytes;

use crate::{
    coordinator::Coordinator,
    kv::{Imemtable, KVReader, KVWriter, KvEntry, Memtable, SetError, SetResult},
    log::LogWriter,
    value::Value,
};

pub struct Storage {
    memtable: Memtable,
    imemtables: Vec<Imemtable>,
    wal: LogWriter,
    coordinator: Rc<Coordinator>,
    seq: u64,
}

impl Storage {
    pub fn new(c: Rc<Coordinator>) -> Self {
        let seq = c.current_sst_file_seq();
        Self {
            memtable: Memtable::new(),
            imemtables: Vec::new(),
            wal: LogWriter::new(seq, c.config()),
            coordinator: c,
            seq,
        }
    }
}

impl Storage {
    fn slow_get(&self, key: &str) -> Option<Value> {
        // todo: level0

        // level x
        None
    }
    pub fn get(&self, key: &str) -> Option<Value> {
        // query from memtable
        if let Some(value) = self.memtable.get(key) {
            if value.deleted() {
                return None;
            }
            return Some(value);
        }

        // query from imemetable
        for table in &self.imemtables {
            if let Some(value) = table.get(key) {
                if value.deleted() {
                    return None;
                }
                return Some(value);
            }
        }

        // query from merged file
        if let Some(val) = self.slow_get(key) {
            return Some(val);
        }

        None
    }

    pub fn get_ver(&self, key: &str, ver: u64) -> Option<Value> {
        // query from memtable
        if let Some(value) = self.memtable.get_ver(key, ver) {
            return Some(value);
        }

        // query from imemetable
        for table in &self.imemtables {
            if let Some(value) = table.get_ver(key, ver) {
                return Some(value);
            }
        }

        // query from merged file
        // if let Some(val) = self.slow_get_snapshot(key, ver) {
        //     return Some(val)
        // }

        None
    }

    pub fn scan(&self, beg: &str, end: &str) -> Box<dyn Iterator<Item = Value>> {
        todo!()
    }

    pub fn set(&mut self, key: String, value: Bytes) -> SetResult<()> {
        self.set_by(key, value, None, None)
    }

    pub fn set_by(
        &mut self,
        key: String,
        value: Bytes,
        ver: Option<u64>,
        ttl: Option<Duration>,
    ) -> SetResult<()> {
        if let Some(ver) = ver {
            let latest_ver = self.get(&key).ok_or(SetError::KeyNotExist)?.version();
            if latest_ver != ver {
                return Err(SetError::VersionNotMatch(latest_ver));
            }
        };
        if value.len() > 1024 * 1024 * 10 {
            // 10MB value
            return Err(SetError::ValueTooLarge);
        }

        let cur_ver = self.coordinator.next_version();
        let entry = KvEntry::new(key, value, ttl.map(|d| d.as_secs()), cur_ver);
        self.wal.append(&entry);

        self.memtable.set(entry)?;
        if self.memtable.full() {
            let mut memtable = Memtable::new();
            swap(&mut memtable, &mut self.memtable);
            self.seq = self.coordinator.next_sst_file_seq();

            self.wal.rotate(self.seq);
            self.imemtables.push(memtable.into());
        }
        Ok(())
    }

    pub fn del<S: Into<String>>(&mut self, key: S) -> SetResult<()> {
        let cur_ver = self.coordinator.next_version();
        let entry = KvEntry::new_del(key.into(), cur_ver);
        self.memtable.set(entry)
    }
}
