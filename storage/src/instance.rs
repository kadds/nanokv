use std::{
    fs,
    path::{Component, PathBuf},
    sync::Arc,
};

use log::info;

use crate::{config::ConfigRef, kv::manifest::Manifest, storage::Storage, util::fname, backend::Backend};

#[allow(unused)]
pub struct Instance {
    manifest: Arc<Manifest>,
    storage: Storage,
    conf: ConfigRef,
    backend: Arc<Backend>,
}

impl Instance {
    pub fn new(conf: ConfigRef, backend: Arc<Backend>) -> Self {
        fname::make_sure(conf);
        fs::File::create(PathBuf::from(&conf.path).join("nanokv")).unwrap();

        let manifest = Arc::new(Manifest::new(conf, backend.clone()));
        Self {
            storage: Storage::new(conf, manifest.clone(), backend.clone()),
            manifest,
            conf,
            backend,
        }
    }

    pub fn clean(conf: ConfigRef) {
        let path = conf.path.clone();
        if !path.exists() {
            return;
        }

        let mut c = path.components();
        if c.next().unwrap() == Component::RootDir && c.next().is_none() {
            panic!("invalid config.path {:?}", path.as_os_str());
        }
        if fs::File::open(path.join("nanokv")).is_ok() {
            fs::remove_dir_all(&conf.path).unwrap();
        } else {
            panic!("invalid config.path, not a nanokv database");
        }
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    pub fn mut_storage(&mut self) -> &mut Storage {
        &mut self.storage
    }

    pub fn compact_range(&self) {}
}
