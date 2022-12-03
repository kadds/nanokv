use std::{
    fs::{self, File},
    io::{self, Write},
    marker::PhantomData,
    path::PathBuf,
};

use byteorder::{WriteBytesExt, LE};
use bytes::BufMut;
use log::info;

use crate::{util::crc::crc_mask, ConfigRef};

use super::{LogEntrySerializer, LogSegmentFlags, SEGMENT_CONTENT_SIZE};

const DEFAULT_ALLOC_SIZE: u64 = 1024 * 1024 * 5; // 5MB
const DETAIL_ALLOC_SIZE: u64 = 1024 * 1024 * 5; // 5MB

pub struct LogWriter<E, S> {
    current: File,
    config: ConfigRef,

    seq: u64,
    write_bytes: u64,
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
        let current = File::create(name_fn(config, seq)).unwrap();
        current.set_len(DEFAULT_ALLOC_SIZE);

        Self {
            current,
            config,
            name_fn,
            seq,
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
        let mut file_writer = io::BufWriter::new(&self.current);
        {
            let mut w = Vec::with_capacity(SEGMENT_CONTENT_SIZE).writer();

            self.write_bytes += self.serializer.write(entry, &mut w);

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
    }

    pub fn sync(&mut self) {
        self.current.sync_data().unwrap();
    }

    pub fn rotate(&mut self, seq: u64) {
        self.sync();

        self.seq = seq;
        self.current = File::create((self.name_fn)(self.config, seq)).unwrap();
        self.current.set_len(DEFAULT_ALLOC_SIZE);

        info!("wal rotate to {}", seq);
    }

    pub fn remove(&self, seq: u64) {
        let name = (self.name_fn)(self.config, seq);
        fs::remove_file(name);
    }

    #[allow(unused)]
    pub fn bytes(&self) -> u64 {
        self.write_bytes
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }
}
