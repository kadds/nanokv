use bytes::Buf;
use positioned_io::RandomAccessFile;

use super::*;
use crate::err::StorageError;
use std::{
    fs::{self, File},
    io::{self, Cursor, Seek},
    path::PathBuf,
};

pub struct LocalFileBasedReadablePersist {
    f: File,
    mmap: Option<memmap2::Mmap>,
    size: u64,
}

impl ReadAt for LocalFileBasedReadablePersist {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> io::Result<usize> {
        if let Some(m) = &self.mmap {
            (&m[pos as usize..]).reader().read(buf)
        } else {
            self.f.read_at(pos, buf)
        }
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

    fn size(&self) -> u64 {
        self.size
    }
}
pub struct LocalFileBasedWriteablePersist {
    f: File,
    path: PathBuf,
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

    fn delete(&mut self) -> Result<()> {
        fs::remove_file(&self.path)?;
        Ok(())
    }
}

#[derive(Default, Debug)]
pub struct LocalFileBasedPersistBackend;

impl PersistBackend for LocalFileBasedPersistBackend {
    fn open(&self, path: &Path, enable_mmap: bool) -> Result<Box<dyn ReadablePersist>> {
        let size = std::fs::metadata(path)?.len();

        let f = File::open(path)?;
        #[cfg(all(unix, target_os = "linux"))]
        {}

        let mmap = if enable_mmap {
            let mmap = unsafe { memmap2::Mmap::map(&f)? };
            Some(mmap)
        } else {
            None
        };
        Ok(Box::new(LocalFileBasedReadablePersist { f, mmap, size }))
    }

    fn get_feature(&self) -> PersistFeature {
        PersistFeature {
            mmap_supported: true,
            seek_supported: true,
        }
    }

    fn create(&self, path: &Path, truncate: Option<u64>) -> Result<Box<dyn WriteablePersist>> {
        let file = File::create(path)?;
        if let Some(t) = truncate {
            file.set_len(t)?;
        }

        Ok(Box::new(LocalFileBasedWriteablePersist {
            f: file,
            path: path.to_path_buf(),
        }))
    }

    fn remove(&self, path: &Path) -> Result<()> {
        fs::remove_file(path)?;
        Ok(())
    }

    fn usage_total(&self) -> UsageTotal {
        UsageTotal {
            usage: 0,
            total_limit: None,
        }
    }

    fn make_sure_dir(&self, path: &Path) -> Result<()> {
        fs::create_dir_all(path)?;
        Ok(())
    }

    fn rename(&self, src: &Path, dst: &Path) -> Result<()> {
        fs::rename(src, dst)?;
        Ok(())
    }
}
