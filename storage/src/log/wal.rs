use std::{
    fs::{self, File},
    io::{self, Write},
    marker::PhantomData,
    path::PathBuf,
    sync::Mutex,
};

use byteorder::{WriteBytesExt, LE};
use bytes::BufMut;
use log::info;

use crate::{util::crc::crc_mask, ConfigRef};

use super::{LogEntrySerializer, LogSegmentFlags, SEGMENT_CONTENT_SIZE};

const DEFAULT_ALLOC_SIZE: u64 = 1024 * 1024 * 5; // 5MB
const DETAIL_ALLOC_SIZE: u64 = 1024 * 1024 * 5; // 5MB

pub const INVALID_SEQ: u64 = u64::MAX;

struct LogInner {
    current: Option<File>,
    seq: u64,
    write_bytes: u64,
}

pub struct LogWriter<E, S> {
    config: ConfigRef,
    inner: Mutex<LogInner>,

    name_fn: Box<dyn Fn(ConfigRef, u64) -> PathBuf + 'static + Send + Sync>,

    serializer: S,

    _pd: PhantomData<E>,
}

impl<E, S> LogWriter<E, S>
where
    S: LogEntrySerializer<Entry = E>,
{
    pub fn new(
        config: ConfigRef,
        serializer: S,
        seq: u64,
        name_fn: Box<dyn Fn(ConfigRef, u64) -> PathBuf + 'static + Send + Sync>,
    ) -> Self {
        let current = if seq == INVALID_SEQ {
            None
        } else {
            let current = File::create(name_fn(config, seq)).unwrap();
            current.set_len(DEFAULT_ALLOC_SIZE);
            Some(current)
        };

        Self {
            inner: Mutex::new(LogInner {
                current,
                seq,
                write_bytes: 0,
            }),
            config,
            name_fn,
            serializer,
            _pd: PhantomData::default(),
        }
    }
}

impl<E, S> LogWriter<E, S>
where
    S: LogEntrySerializer<Entry = E>,
{
    pub fn append(&self, entry: &E) {
        let mut inner = self.inner.lock().unwrap();
        let bytes;
        {
            if inner.current.is_none() {
                return;
            }
            let mut file_writer = io::BufWriter::new(inner.current.as_ref().unwrap());
            let mut w = Vec::with_capacity(SEGMENT_CONTENT_SIZE).writer();

            bytes = self.serializer.write(entry, &mut w);

            let mut do_write_segment = |item: &[u8], flags| {
                let mut crc_builder = crc32fast::Hasher::new();
                crc_builder.update(item);
                let crc_value = crc_mask(crc_builder.finalize());

                file_writer.write_u32::<LE>(crc_value).unwrap();
                file_writer.write_u16::<LE>(item.len() as u16).unwrap();
                file_writer.write_u8(flags).unwrap();

                file_writer.write_all(item).unwrap();
            };

            let mut chunks = w.get_ref().chunks(SEGMENT_CONTENT_SIZE);
            let chunks_len = chunks.len();
            if chunks_len == 0 {
                return;
            }
            if chunks_len == 1 {
                let flags = LogSegmentFlags::FULL_FRAGMENT.bits;
                do_write_segment(chunks.next().unwrap(), flags);
                return;
            }
            for (idx, item) in chunks.enumerate() {
                let flags = if idx == 0 {
                    LogSegmentFlags::BEGIN_FRAGMENT.bits
                } else if idx == chunks_len - 1 {
                    LogSegmentFlags::LAST_FRAGMENT.bits
                } else {
                    LogSegmentFlags::MIDDLE_FRAGMENT.bits
                };

                do_write_segment(item, flags);
            }

            file_writer.flush().unwrap();
        }
        inner.write_bytes = bytes;
    }

    pub fn sync(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.current.as_mut().map(|f| f.sync_data().unwrap());
    }

    pub fn rotate(&self, seq: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.current.as_mut().map(|f| f.sync_data().unwrap());

        inner.seq = seq;
        inner.current = Some(File::create((self.name_fn)(self.config, seq)).unwrap());
        inner.current.as_mut().unwrap().set_len(DEFAULT_ALLOC_SIZE);

        info!("wal rotate to {}", seq);
    }

    pub fn remove(&self, seq: u64) {
        let name = (self.name_fn)(self.config, seq);
        fs::remove_file(name);
    }

    pub fn seq(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.seq
    }
}
