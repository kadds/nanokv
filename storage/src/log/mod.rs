use std::io;

pub mod replayer;
pub mod wal;
use bitflags::bitflags;
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
pub use replayer::LogReplayer;
pub use wal::LogWriter;

use self::{replayer::SegmentRead, wal::SegmentWrite};

const SEGMENT_SIZE: usize = 1024 * 32; // 32k

bitflags! {
    struct LogSegmentFlags: u8 {
        const UNKNOWN = 0b000;
        const FULL_FRAGMENT = 0b001;
        const BEGIN_FRAGMENT = 0b010;
        const CONTINUE_FRAGMENT = 0b011;
        const LAST_FRAGMENT = 0b100;
    }
}

pub trait LogEntrySerializer {
    type Entry;
    fn write<W>(&self, entry: &Self::Entry, w: &mut W) -> io::Result<()>
    where
        W: SegmentWrite;
    fn read<R>(&self, r: &mut R) -> io::Result<Self::Entry>
    where
        R: SegmentRead;
}

// default impl

impl LogEntrySerializer for String {
    type Entry = String;

    fn write<W>(&self, entry: &Self::Entry, w: &mut W) -> io::Result<()>
    where
        W: SegmentWrite,
    {
        w.write_u32::<LE>(entry.len() as u32)?;
        w.write_all(entry.as_bytes())?;
        Ok(())
    }

    fn read<R>(&self, r: &mut R) -> io::Result<Self::Entry>
    where
        R: SegmentRead,
    {
        let len = r.read_u32::<LE>()?;
        let mut vec = Vec::new();
        vec.resize(len as usize, 0);
        r.read_exact(&mut vec)?;
        unsafe { Ok(String::from_utf8_unchecked(vec)) }
    }
}

#[cfg(test)]
mod test {
    use rand::{distributions::Alphanumeric, prelude::Distribution};

    use crate::{
        backend::{fs::memory::MemoryBasedPersistBackend, Backend},
        err::StorageError,
    };

    struct TestSerializer;
    impl LogEntrySerializer for TestSerializer {
        type Entry = String;

        fn write<W>(&self, entry: &Self::Entry, w: &mut W) -> io::Result<()>
        where
            W: SegmentWrite,
        {
            w.write_all(entry.as_bytes())?;
            Ok(())
        }

        fn read<R>(&self, r: &mut R) -> io::Result<Self::Entry>
        where
            R: SegmentRead,
        {
            let mut buf = Vec::new();
            let mut tmp: [u8; 128] = [0; 128];

            loop {
                let n = match r.read(&mut tmp) {
                    Ok(e) => e,
                    Err(e) => {
                        if e.kind() == io::ErrorKind::UnexpectedEof {
                            break;
                        } else {
                            return Err(e);
                        }
                    }
                };
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
            }
            if buf.len() == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "empty"));
            }
            Ok(String::from_utf8(buf).unwrap())
        }
    }

    use super::*;
    #[test]
    fn wal_test() {
        let mut test_strings = vec![];
        for i in 10..1000 {
            let s: String = Alphanumeric
                .sample_iter(&mut rand::thread_rng())
                .take(i)
                .map(char::from)
                .collect();
            test_strings.push(s);
        }
        let spec_size = [32761, 32760, 65535, 1024 * 1024, 1024 * 1024 * 20];
        for size in &spec_size {
            let s2: String = Alphanumeric
                .sample_iter(&mut rand::thread_rng())
                .take(*size)
                .map(char::from)
                .collect();
            test_strings.push(s2);
        }

        // write
        let backend = Backend::new(MemoryBasedPersistBackend::new());
        let wal = wal::LogWriter::new(&backend, TestSerializer);
        let path = "test";
        wal.rotate(path).unwrap();

        for str in &test_strings {
            wal.append(str).unwrap();
        }
        wal.sync().unwrap();
        drop(wal);
        // replay

        let replayer = replayer::LogReplayer::new(&backend, TestSerializer);

        for (idx, var) in replayer.iter(path).unwrap().enumerate() {
            assert_eq!(test_strings[idx], var.unwrap());
        }
    }
}
