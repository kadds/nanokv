use std::{
    fs::{self, File},
    io::{self, Write},
};

use crate::{
    kv::{self, LogSerializer},
    Config, ConfigRef,
};

pub struct LogFile {
    file: File,
    name: String,
}

impl LogFile {
    pub fn new(sst_seq: u64, alloc_size: usize, config: ConfigRef) -> Self {
        let name = format!("{}/wal-{}.log", config.path, sst_seq);
        let file = File::create(&name).unwrap();
        file.set_len(alloc_size as u64).unwrap();
        Self { file, name }
    }

    pub fn open(sst_seq: u64, config: ConfigRef) -> Self {
        let name = format!("{}/wal-{}.log", config.path, sst_seq);
        let file = File::open(&name).unwrap();
        Self { file, name }
    }

    pub fn file(&mut self) -> &mut File {
        &mut self.file
    }

    pub fn remove(sst_seq: u64, config: ConfigRef) {
        let name = format!("{}/wal-{}.log", config.path, sst_seq);
        let _ = fs::remove_file(&name);
    }

    fn make_sure(&mut self, alloc_size: usize) {
        self.file.set_len(alloc_size as u64).unwrap();
    }
}

const ALLOC_SIZE: usize = 1024 * 1024 * 20;
pub struct LogWriter {
    current: LogFile,
    sst_seq: u64,
    config: ConfigRef,
}

impl LogWriter {
    pub fn new(sst_seq: u64, config: ConfigRef) -> Self {
        Self {
            current: LogFile::new(sst_seq, ALLOC_SIZE, config.clone()),
            sst_seq,
            config,
        }
    }
}

impl LogWriter {
    pub fn append(&mut self, entry: &kv::KvEntry) {
        {
            let file = self.current.file();
            let mut writer = io::BufWriter::new(file);
            entry.serialize_log(&mut writer);
            writer.flush().unwrap();
        }
        self.current.file().sync_data().unwrap();
    }

    pub fn rotate(&mut self, sst_seq: u64) {
        self.sst_seq = sst_seq;
        self.current = LogFile::new(self.sst_seq, ALLOC_SIZE, self.config.clone());
    }
}
