use std::{
    fs,
    path::{Component, PathBuf},
    sync::Arc,
};

use log::info;

use crate::{config::ConfigRef, kv::manifest::Manifest, storage::Storage, util::fname};

#[allow(unused)]
pub struct Instance {
    manifest: Arc<Manifest>,
    storage: Storage,
    conf: ConfigRef,
}

impl Instance {
    pub fn new(conf: ConfigRef) -> Self {
        let dir = std::env::current_dir().unwrap();
        info!("cwd {}", dir.to_str().unwrap());
        fname::make_sure(conf);
        fs::File::create(PathBuf::from(&conf.path).join("nanokv")).unwrap();

        let manifest = Arc::new(Manifest::new(conf));
        Self {
            storage: Storage::new(conf, manifest.clone()),
            manifest,
            conf,
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

    pub fn compact_config(&self, _io_limit: f32) {}
}
