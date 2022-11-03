use std::{fs, sync::Arc};

use crate::{config::ConfigRef, kv::manifest::Manifest, storage::Storage};

#[allow(unused)]
pub struct Instance {
    manifest: Arc<Manifest>,
    storage: Storage,
    conf: ConfigRef,
}

impl Instance {
    pub fn new(conf: ConfigRef) -> Self {
        fs::create_dir_all(&conf.path).unwrap();
        let manifest = Arc::new(Manifest::new(conf.clone()));
        Self {
            storage: Storage::new(conf, manifest.clone()),
            manifest,
            conf,
        }
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    pub fn mut_storage(&mut self) -> &mut Storage {
        &mut self.storage
    }
}
