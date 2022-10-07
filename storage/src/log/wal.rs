use std::{
    io::{self, Write},
    marker::PhantomData,
};

use log::info;

use super::{LogEntrySerializer, LogFile};

const DEFAULT_ALLOC_SIZE: usize = 1024 * 1024 * 5;
pub struct LogWriter<E, S> {
    current: LogFile,
    seq: u64,
    path: String,
    last_size: usize,
    write_bytes: u64,

    serializer: S,
    _pd: PhantomData<E>,
}

impl<E, S> LogWriter<E, S>
where
    S: LogEntrySerializer<Entry = E>,
{
    pub fn new(serializer: S, seq: u64, path: String) -> Self {
        Self {
            current: LogFile::new(seq, DEFAULT_ALLOC_SIZE, &path),
            seq,
            path,
            last_size: DEFAULT_ALLOC_SIZE,
            write_bytes: 0,
            serializer,
            _pd: PhantomData::default(),
        }
    }
}

impl<E, S> LogWriter<E, S>
where
    S: LogEntrySerializer<Entry = E>,
{
    pub fn append(&mut self, entry: &E) {
        {
            let file = self.current.file();
            let mut writer = io::BufWriter::new(file);
            self.write_bytes += self.serializer.write(entry, &mut writer);
            writer.flush().unwrap();
        }
        self.current.file().sync_data().unwrap();
    }

    pub fn rotate(&mut self, seq: u64) {
        self.seq = seq;
        self.current = LogFile::new(self.seq, self.last_size, &self.path);
        info!("wal rotate {}/{}", self.path, seq);
    }

    pub fn remove(&mut self, seq: u64) {
        LogFile::remove(seq, &self.path);
    }

    pub fn bytes(&self) -> u64 {
        self.write_bytes
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }
}
