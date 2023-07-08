use std::{
    io::{self, Write},
    path::PathBuf,
    sync::Mutex,
};

use byteorder::{WriteBytesExt, LE};
use bytes::BufMut;

use crate::{
    backend::{fs::WriteablePersist, Backend},
    err::Result,
    util::crc::crc_mask,
};

use super::{LogEntrySerializer, LogSegmentFlags, SEGMENT_SIZE};

const DEFAULT_ALLOC_SIZE: u64 = 1024 * 1024 * 5; // 5MB

pub trait SegmentWrite: Write {
    fn flush_all(self) -> io::Result<u64>;
}

pub struct SegmentWriter<'a, W> {
    w: W,
    segment_index: u64,
    segment_written: u64,
    cache: &'a mut [u8],
}

impl<'a, W> SegmentWriter<'a, W>
where
    W: Write,
{
    pub fn new(w: W, segment_size: u64, cache: &'a mut [u8]) -> Self {
        assert_eq!(segment_size, cache.len() as u64);
        Self {
            w,
            segment_index: 0,
            segment_written: 0,
            cache,
        }
    }

    pub fn crc_value(flags: u8, length: u16, payload: &[u8]) -> [u8; 8] {
        let mut buffer: [u8; 8] = [0; 8];
        let mut crc_builder = crc32fast::Hasher::new();
        let mut w = buffer.writer();
        let _ = w.write_u32::<LE>(0);
        let _ = w.write_u8(flags);
        let _ = w.write_u8(0);
        let _ = w.write_u16::<LE>(length);
        let _ = w.flush();

        crc_builder.update(&buffer);
        crc_builder.update(&payload[..length as usize]);

        let crc = crc_mask(crc_builder.finalize());
        let _ = buffer.writer().write_u32::<LE>(crc);
        buffer
    }

    fn flush_segment(&mut self, last: bool) -> io::Result<()> {
        let flags = if self.segment_index == 0 {
            if last {
                LogSegmentFlags::FULL_FRAGMENT.bits()
            } else {
                LogSegmentFlags::BEGIN_FRAGMENT.bits()
            }
        } else {
            if last {
                LogSegmentFlags::LAST_FRAGMENT.bits()
            } else {
                LogSegmentFlags::CONTINUE_FRAGMENT.bits()
            }
        };
        let length = self.segment_written as u16;

        // flush
        let crc_buffer = Self::crc_value(flags, length, &self.cache[8..]);
        self.cache.writer().write(&crc_buffer)?;

        self.w.write_all(&self.cache)?;

        self.segment_index += 1;
        self.segment_written = 0;
        Ok(())
    }
}

// crc u32 = 4
// flags u8 = 1
// reserved u8 = 1
// length u16 = 2
// payload
// -------
impl<'a, W> Write for SegmentWriter<'a, W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = buf.len() as u64;
        let mut beg = 0;

        while beg < len {
            let rest = len - beg;
            let mut write_size = self.cache.len() as u64 - self.segment_written - 8;
            if write_size == 0 {
                self.flush_segment(false)?;
                write_size = self.cache.len() as u64 - 8;
            }

            write_size = write_size.min(rest);
            let end = beg + write_size;
            let chunk = &buf[beg as usize..end as usize];

            self.cache[(8 + self.segment_written as usize)..]
                .writer()
                .write(&chunk)?;

            self.segment_written += write_size;
            beg = end;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a, W> SegmentWrite for SegmentWriter<'a, W>
where
    W: Write,
{
    fn flush_all(mut self) -> io::Result<u64> {
        self.flush()?;
        self.flush_segment(true)?;

        self.w.flush()?;
        Ok(self.cache.len() as u64 * self.segment_index)
    }
}

struct LogInner {
    current: Option<(Box<dyn WriteablePersist>, Vec<u8>)>,
    write_bytes: u64,
}

pub struct LogWriter<'a, S> {
    inner: Mutex<LogInner>,
    backend: &'a Backend,
    serializer: S,
}

impl<'a, E, S> LogWriter<'a, S>
where
    S: LogEntrySerializer<Entry = E>,
{
    pub fn new(backend: &'a Backend, serializer: S) -> Self {
        let mut write_buffer = vec![];
        write_buffer.resize(SEGMENT_SIZE, 0);

        Self {
            inner: Mutex::new(LogInner {
                current: None,
                write_bytes: 0,
            }),
            serializer,
            backend,
        }
    }
}

impl<'a, E, S> LogWriter<'a, S>
where
    S: LogEntrySerializer<Entry = E>,
{
    pub fn append(&self, entry: &E) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let bytes = {
            let (underlying, buf) = match &mut inner.current {
                Some(f) => f,
                None => {
                    return Err(io::Error::new(io::ErrorKind::Other, "empty underlying"));
                }
            };
            let mut w = SegmentWriter::new(underlying, SEGMENT_SIZE as u64, buf);

            self.serializer.write(entry, &mut w)?;
            w.flush_all()?
        };
        inner.write_bytes += bytes;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if let Some((cur, _)) = inner.current.as_mut() {
            cur.sync()?;
        }
        Ok(())
    }

    pub fn rotate<P>(&self, path: P) -> Result<()>
    where
        P: Into<PathBuf>,
    {
        let mut inner = self.inner.lock().unwrap();
        if let Some((mut cur, _)) = inner.current.take() {
            cur.sync()?;
        } else {
        }
        let path = path.into();

        let mut write_buffer = vec![];
        write_buffer.resize(SEGMENT_SIZE, 0);
        let file = self.backend.fs.create(&path, Some(DEFAULT_ALLOC_SIZE))?;
        inner.current = Some((file, write_buffer));
        inner.write_bytes = 0;

        Ok(())
    }
}

pub struct DummySegmentWrite<W> {
    w: W,
    len: usize,
}

impl<W> DummySegmentWrite<W>
where
    W: Write,
{
    pub fn new(w: W) -> Self {
        Self { w, len: 0 }
    }
}

impl<W> Write for DummySegmentWrite<W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let r = self.w.write(buf)?;
        self.len += r;
        Ok(r)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.w.flush()
    }
}

impl<W> SegmentWrite for DummySegmentWrite<W>
where
    W: Write,
{
    fn flush_all(self) -> io::Result<u64> {
        let r = self.len as u64;
        Ok(r)
    }
}
