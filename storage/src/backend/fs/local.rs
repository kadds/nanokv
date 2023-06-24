use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, Read, Seek},
    ops::Range,
    sync::Mutex,
};

use bytes::Bytes;

use crate::err::StorageError;

use super::*;

pub struct LocalFileBasedReadablePersist {
    f: File,
    mmap: Option<memmap2::Mmap>,
}

impl Read for LocalFileBasedReadablePersist {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.f.read(buf)
    }
}

impl Seek for LocalFileBasedReadablePersist {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.f.seek(pos)
    }
}
impl ReadablePersist for LocalFileBasedReadablePersist {
    fn addr(&self) -> Result<&[u8]> {
        if let Some(m) = &self.mmap {
            return Ok(&m[..]);
        }
        Err(StorageError::Io(io::Error::new(
            io::ErrorKind::Unsupported,
            "mmap file not enabled",
        )))
    }

    // fn read_slice<'a>(&'a self, range: Range<usize>, buffer: &'a mut Vec<u8>) -> Result<&'a [u8]> {
    //     if let Some(m) = &self.mmap {
    //         Ok(&m[range])
    //     } else {
    //         *buffer = Vec::with_capacity(range.len());
    //         unsafe {
    //             let b = buffer.spare_capacity_mut();
    //             self.f.read_exact( std::mem::transmute(&mut b[..]))?;
    //             buffer.set_len(range.len());
    //         }

    //         Ok(&buffer[..])
    //     }
    // }
}

pub struct LocalFileBasedWriteablePersist {
    f: File,
}

impl Write for LocalFileBasedWriteablePersist {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.f.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.f.flush()
    }
}

impl WriteablePersist for LocalFileBasedWriteablePersist {
    fn truncate(&mut self, size: u64) -> Result<()> {
        self.f.set_len(size)?;
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.f.sync_data()?;
        Ok(())
    }
}

#[derive(Default, Debug)]
pub struct LocalFileBasedPersistBackend;

impl PersistBackend for LocalFileBasedPersistBackend {
    fn open(&self, path: PathBuf, enable_mmap: bool) -> Result<Box<dyn ReadablePersist>> {
        let f = File::open(path)?;
        let mmap = if enable_mmap {
            let mmap = unsafe { memmap2::Mmap::map(&f)? };
            Some(mmap)
        } else {
            None
        };
        Ok(Box::new(LocalFileBasedReadablePersist { f, mmap }))
    }

    fn get_feature(&self) -> PersistFeature {
        PersistFeature {
            mmap_supported: true,
            seek_supported: true,
        }
    }

    fn create(&self, path: PathBuf, truncate: Option<u64>) -> Result<Box<dyn WriteablePersist>> {
        let file = File::create(path)?;
        if let Some(t) = truncate {
            file.set_len(t)?;
        }

        Ok(Box::new(LocalFileBasedWriteablePersist { f: file }))
    }

    fn remove(&self, path: PathBuf) -> Result<()> {
        fs::remove_file(path)?;
        Ok(())
    }

    fn usage_total(&self) -> UsageTotal {
        UsageTotal {
            usage: 0,
            total_limit: None,
        }
    }

    fn make_sure_dir(&self, path: PathBuf) -> Result<()> {
        fs::create_dir_all(path)?;
        Ok(())
    }
}
