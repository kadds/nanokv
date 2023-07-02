use std::{
    fs::File,
    io::{self, BufReader, Read},
    marker::PhantomData,
    path::PathBuf,
    sync::Arc, fmt::Write,
};

use byteorder::{ReadBytesExt, LE};
use bytes::{
    buf::{Reader, Writer},
    Buf, BufMut, BytesMut,
};

use crate::{
    backend::{fs::ReadablePersist, Backend, BackendRef},
    err::Result,
    util::crc::crc_unmask,
};

use super::{LogEntrySerializer, LogSegmentFlags, SEGMENT_SIZE};

pub trait SegmentRead: Read {}

pub struct SegmentReader<'a, R> {
    r: R,
    cache: &'a mut [u8],
    done: bool,
    flags: u8,
    length: u16,
    pos: usize,
}

impl<'a, R> SegmentRead for SegmentReader<'a, R> where R: Read {}

impl<'a, R> SegmentReader<'a, R>
where
    R: Read,
{
    pub fn new(r: R, segment_size: u64, cache: &'a mut [u8]) -> Self {
        assert_eq!(segment_size, cache.len() as u64);
        Self {
            r,
            cache,
            done: false,
            flags: 0,
            length: 0,
            pos: 0,
        }
    }

    pub fn pre_read(&mut self) -> io::Result<()> {
        self.r.read_exact(self.cache)?;

        let mut r = self.cache.reader();
        let crc_value = crc_unmask(r.read_u32::<LE>().unwrap());
        let flags = r.read_u8()?;
        let _ = r.read_u8()?; // reserved
        let length = r.read_u16::<LE>()?;

        if flags == LogSegmentFlags::UNKNOWN.bits() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "get segment flags eof",
            ));
        }

        let mut crc_builder = crc32fast::Hasher::new();
        crc_builder.update(&[0; 4]); // crc placeholder
        crc_builder.update(&self.cache[4..(4 + length + 4) as usize]);
        let calculate_crc_value = crc_builder.finalize();
        if crc_value != calculate_crc_value {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "crc not match"));
        }

        if self.flags == LogSegmentFlags::FULL_FRAGMENT.bits() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "get flags not expect",
            ));
        }
        if (self.flags == LogSegmentFlags::BEGIN_FRAGMENT.bits()
            || self.flags == LogSegmentFlags::CONTINUE_FRAGMENT.bits())
            && (flags != LogSegmentFlags::LAST_FRAGMENT.bits()
                && flags != LogSegmentFlags::CONTINUE_FRAGMENT.bits())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "get flags not expect",
            ));
        }

        if flags == LogSegmentFlags::LAST_FRAGMENT.bits()
            || flags == LogSegmentFlags::FULL_FRAGMENT.bits()
        {
            self.done = true;
        }

        self.flags = flags;
        self.length = length;
        self.pos = 0;
        Ok(())
    }
}

impl<'a, R> Read for SegmentReader<'a, R>
where
    R: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut buf_offset = 0 as usize;
        loop {
            if buf_offset == buf.len() {
                break;
            }
            let mut cache_can_read = self.length as usize - self.pos;
            if cache_can_read == 0 {
                if self.done {
                    if buf_offset > 0 {
                        break;
                    }
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "get segment eof",
                    ));
                }
                self.pre_read()?;
                cache_can_read = self.length as usize - self.pos;
            }

            let buf_can_read = buf.len() - buf_offset;
            let target_read = buf_can_read.min(cache_can_read);

            self.cache[(8 + self.pos)..]
                .reader()
                .read_exact(&mut buf[buf_offset..(buf_offset + target_read)])?;

            buf_offset += target_read;
            self.pos += target_read;
        }
        Ok(buf_offset)
    }
}

pub struct LogReplayerIter<'a, S, E> {
    underlying: Box<dyn ReadablePersist>,
    cache: Vec<u8>,
    serializer: &'a S,
    _pd: PhantomData<E>,
}

impl<'a, S, E> Iterator for LogReplayerIter<'a, S, E>
where
    S: LogEntrySerializer<Entry = E>,
{
    type Item = Result<E>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut r = SegmentReader::new(
            self.underlying.as_mut(),
            SEGMENT_SIZE as u64,
            &mut self.cache,
        );
        let entry = match self.serializer.read(&mut r) {
            Ok(e) => e,
            Err(err) => {
                if err.kind() == io::ErrorKind::UnexpectedEof {
                    return None;
                } else {
                    return Some(Err(crate::err::StorageError::Io(err)));
                }
            }
        };
        Some(Ok(entry))
    }
}

pub struct LogReplayer<'a, S> {
    serializer: S,
    backend: &'a Backend,
}

impl<'a, E, S> LogReplayer<'a, S>
where
    S: LogEntrySerializer<Entry = E>,
{
    pub fn new(backend: &'a Backend, serializer: S) -> Self {
        Self {
            serializer,
            backend,
        }
    }

    pub fn iter<P>(&self, path: P) -> Result<LogReplayerIter<S, E>>
    where
        P: Into<PathBuf>,
    {
        let path = path.into();
        let underlying = self.backend.fs.open(&path, false)?;
        let mut cache = vec![];
        cache.resize(SEGMENT_SIZE, 0);
        Ok(LogReplayerIter {
            underlying,
            cache,
            serializer: &self.serializer,
            _pd: PhantomData::default(),
        })
    }
}

pub struct DummySegmentRead<R>(R) where R: Read;

impl<R> DummySegmentRead<R> where R: Read {
    pub fn new(r: R) -> Self {
        Self(r)
    }
}

impl<R> Read for DummySegmentRead<R> where R: Read {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl<R> SegmentRead  for DummySegmentRead<R> where R: Read {
}