use std::{
    io::{self, Write},
    marker::PhantomData,
};

use byteorder::{WriteBytesExt, LE};
use bytes::BufMut;
use log::info;

use crate::util::crc_mask;

use super::{LogEntrySerializer, LogFile, LogSegmentFlags, SEGMENT_CONTENT_SIZE};

const DEFAULT_ALLOC_SIZE: usize = 1024 * 1024 * 5;
pub struct LogWriter<E, S> {
    current: LogFile,
    seq: u64,
    prefix: String,
    last_size: usize,
    write_bytes: u64,

    serializer: S,

    _pd: PhantomData<E>,
}

impl<E, S> LogWriter<E, S>
where
    S: LogEntrySerializer<Entry = E>,
{
    pub fn new(serializer: S, seq: u64, prefix: String) -> Self {
        Self {
            current: LogFile::new(seq, DEFAULT_ALLOC_SIZE, &prefix),
            seq,
            prefix,
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
        let file = self.current.file();
        let mut file_writer = io::BufWriter::new(file);
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
        self.current.file().sync_data().unwrap();
    }

    pub fn rotate(&mut self, seq: u64) {
        self.sync();

        self.seq = seq;
        self.current = LogFile::new(self.seq, self.last_size, &self.prefix);
        info!("wal rotate to {}{}", self.prefix, seq);
    }

    pub fn remove(&self, seq: u64) {
        LogFile::remove(seq, &self.prefix);
    }

    #[allow(unused)]
    pub fn bytes(&self) -> u64 {
        self.write_bytes
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }
}
