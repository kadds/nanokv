use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    mem::swap,
    rc::Rc,
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle, Thread},
    time::Duration,
};

use bytes::Bytes;

use crate::{
    coordinator::Coordinator,
    kv::{
        sst::{self, sst_name, SSTWriter},
        Imemtable, KVReader, KVWriter, KvEntry, Memtable, SetError, SetResult,
    },
    log::{self, LogWriter},
    value::Value,
    ConfigRef,
};

#[derive(Debug)]
struct Imemtables {
    imemtables: UnsafeCell<VecDeque<Imemtable>>,
    cond: Condvar,
    write_offset: Mutex<usize>,
    config: ConfigRef,
}

unsafe impl Sync for Imemtables {}

struct ConsumeGuard<'a> {
    imemtables: &'a Imemtables,
    imemtable: &'a Imemtable,
}

impl<'a> ConsumeGuard<'a> {
    pub fn table(&self) -> &'a Imemtable {
        self.imemtable
    }
}

impl<'a> Drop for ConsumeGuard<'a> {
    fn drop(&mut self) {
        unsafe { self.imemtables.commit(self.imemtable.seq()) };
    }
}

impl Imemtables {
    pub fn new(config: ConfigRef) -> Self {
        let vec = VecDeque::with_capacity(20);
        Self {
            imemtables: UnsafeCell::new(vec),
            cond: Condvar::new(),
            write_offset: Mutex::new(0),
            config,
        }
    }
    pub fn push(&self, table: Imemtable) {
        {
            let mut guard = self.write_offset.lock().unwrap();
            while self.tables().len() >= 20 {
                guard = self.cond.wait(guard).unwrap();
                if *guard > 0 {
                    self.mut_tables().pop_front();
                    *guard -= 1;
                }
            }
        }
        self.mut_tables().push_back(table);
        self.cond.notify_one();
    }
    fn tables<'a>(&'a self) -> &'a VecDeque<Imemtable> {
        unsafe {
            let tables = &*self.imemtables.get();
            tables
        }
    }
    fn mut_tables<'a>(&'a self) -> &'a mut VecDeque<Imemtable> {
        unsafe {
            let tables = &mut *self.imemtables.get();
            tables
        }
    }

    pub fn slow(&self) -> bool {
        self.tables().len() >= 10
    }

    unsafe fn commit(&self, seq: u64) {
        *self.write_offset.lock().unwrap() += 1;
        self.cond.notify_one();
        log::wal::LogFile::remove(seq, self.config);
    }

    pub fn fetch<'a>(&'a self) -> ConsumeGuard<'a> {
        let mut guard = self.write_offset.lock().unwrap();
        while self.tables().len() == 0 || *guard >= self.tables().len() {
            guard = self.cond.wait(guard).unwrap();
        }
        let table = &self.tables()[*guard];
        ConsumeGuard {
            imemtables: self,
            imemtable: table,
        }
    }
}

impl Imemtables {
    pub fn get(&self, key: &str) -> Option<Value> {
        for table in self.tables().iter().rev() {
            if let Some(value) = table.get(key) {
                return Some(value);
            }
        }
        None
    }
    pub fn get_ver(&self, key: &str, ver: u64) -> Option<Value> {
        for table in self.tables().iter().rev() {
            if let Some(value) = table.get_ver(key, ver) {
                return Some(value);
            }
        }
        None
    }
}

type SharedImemtables = Arc<Imemtables>;

pub struct Storage {
    memtable: Memtable,
    imemtables: SharedImemtables,
    wal: LogWriter,
    coordinator: Arc<Coordinator>,
    compactor: Arc<StorageCompactor>,
}

struct StorageCompactor {
    imemtables: SharedImemtables,
    thread: UnsafeCell<Option<JoinHandle<()>>>,
}

impl StorageCompactor {
    pub fn new(imemtables: SharedImemtables, conf: ConfigRef) -> Arc<Self> {
        let this = Arc::new(Self {
            imemtables,
            thread: UnsafeCell::new(None),
        });
        let that = this.clone();
        unsafe {
            let mut_thread = &mut *this.thread.get();
            *mut_thread = Some(thread::spawn(move || {
                that.main(conf);
            }));
        }
        this
    }
    fn main(&self, conf: ConfigRef) {
        loop {
            let guard = self.imemtables.fetch();
            let table = guard.table();
            let iter = table.entry_iter().filter(|entry| !entry.deleted());
            let mut sst = sst::raw_sst::RawSSTWriter::new(&sst_name(&conf.path, table.seq()));
            sst.write(0, iter);
        }
    }
}

unsafe impl Sync for StorageCompactor {}

impl Storage {
    pub fn new(c: Arc<Coordinator>) -> Self {
        let seq = c.current_sst_file_seq();
        let imemtables = SharedImemtables::new(Imemtables::new(c.config()));
        let sf = Self {
            memtable: Memtable::new(seq),
            imemtables: imemtables.clone(),
            wal: LogWriter::new(seq, c.config()),
            compactor: StorageCompactor::new(imemtables, c.config()),
            coordinator: c,
        };

        sf
    }
}

impl Storage {
    pub fn shutdown(&self) {}

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
        if let Some(value) = self.imemtables.get(key) {
            if value.deleted() {
                return None;
            }
            return Some(value);
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
        if let Some(value) = self.imemtables.get_ver(key, ver) {
            return Some(value);
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
            let new_seq = self.coordinator.next_sst_file_seq();
            let mut memtable = Memtable::new(new_seq);
            swap(&mut memtable, &mut self.memtable);

            self.wal.rotate(new_seq);
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
