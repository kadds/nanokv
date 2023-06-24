use std::sync::Arc;

use self::fs::PersistBackend;

pub mod fs;

#[derive(Debug)]
pub struct Backend {
    pub fs: Arc<dyn PersistBackend>,
}

impl Backend {
    pub fn new<B: PersistBackend + 'static>(fs: B) -> Self {
        Self { fs: Arc::new(fs) }
    }
}

pub type BackendRef = &'static Backend;
