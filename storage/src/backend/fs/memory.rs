use std::{
    collections::HashMap,
    io::{self, Cursor},
    sync::{Arc, Mutex},
};

use bytes::{buf::Writer, BufMut, Bytes, BytesMut};

use super::*;

#[derive(Default, Clone, Debug)]
pub struct MemoryBasedPersistBackend {
    files: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl MemoryBasedPersistBackend {
    pub fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

pub struct ReadableMemoryBasedPersist {
    cursor: Cursor<Bytes>,
    bytes: Bytes,
}

impl Read for ReadableMemoryBasedPersist {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.cursor.read(buf)
    }
}

impl Seek for ReadableMemoryBasedPersist {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.cursor.seek(pos)
    }
}

impl ReadablePersist for ReadableMemoryBasedPersist {
    fn addr(&self) -> Result<&[u8]> {
        Ok(&self.bytes)
    }
}

pub struct WriteableMemoryBasedPersist {
    bytes: Option<Writer<BytesMut>>,
    path: String,
    b: MemoryBasedPersistBackend,
}

impl Write for WriteableMemoryBasedPersist {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes.as_mut().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.bytes.as_mut().unwrap().flush()
    }
}

impl WriteablePersist for WriteableMemoryBasedPersist {
    fn truncate(&mut self, size: u64) -> Result<()> {
        // self.bytes.
        self.bytes
            .as_mut()
            .unwrap()
            .get_mut()
            .reserve(size as usize);
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Drop for WriteableMemoryBasedPersist {
    fn drop(&mut self) {
        let mut files = self.b.files.lock().unwrap();
        files.insert(
            self.path.clone(),
            self.bytes.take().unwrap().into_inner().freeze(),
        );
    }
}

impl PersistBackend for MemoryBasedPersistBackend {
    fn open(&self, path: &Path, _enable_mmap: bool) -> Result<Box<dyn ReadablePersist>> {
        let files = self.files.lock().unwrap();
        let bytes = files
            .get(path.to_str().unwrap())
            .ok_or(io::Error::new(io::ErrorKind::NotFound, "file not exist"))?;
        let cursor = Cursor::new(bytes.clone());

        Ok(Box::new(ReadableMemoryBasedPersist {
            cursor,
            bytes: bytes.clone(),
        }))
    }

    fn get_feature(&self) -> PersistFeature {
        PersistFeature {
            mmap_supported: true,
            seek_supported: true,
        }
    }

    fn create(&self, path: &Path, _truncate: Option<u64>) -> Result<Box<dyn WriteablePersist>> {
        Ok(Box::new(WriteableMemoryBasedPersist {
            bytes: Some(BytesMut::new().writer()),
            path: path.to_str().unwrap().to_owned(),
            b: MemoryBasedPersistBackend {
                files: self.files.clone(),
            },
        }))
    }

    fn remove(&self, path: &Path) -> Result<()> {
        let mut files = self.files.lock().unwrap();
        files.remove(path.to_str().unwrap());
        Ok(())
    }

    fn usage_total(&self) -> UsageTotal {
        UsageTotal {
            usage: 0,
            total_limit: None,
        }
    }

    fn make_sure_dir(&self, _path: &Path) -> Result<()> {
        Ok(())
    }

    fn rename(&self, src: &Path, dst: &Path) -> Result<()> {
        let mut files = self.files.lock().unwrap();
        let path_str = src.to_str().unwrap();
        let content = files.remove(path_str);
        if let Some(content) = content {
            files.insert(dst.to_str().unwrap().to_owned(), content);
        }

        Ok(())
    }
}
