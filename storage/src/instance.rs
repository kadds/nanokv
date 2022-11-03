use std::{fs, sync::Arc, path::{PathBuf, Component}};

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
        fs::File::create(PathBuf::from(&conf.path).join("nanokv")).unwrap();
        let manifest = Arc::new(Manifest::new(conf.clone()));
        Self {
            storage: Storage::new(conf, manifest.clone()),
            manifest,
            conf,
        }
    }

    pub fn clean(conf: ConfigRef) {
        let path = PathBuf::from(&conf.path);
        if !path.exists() {
            return;
        }

        let mut c = path.components();
        if c.next().unwrap() == Component::RootDir && c.next().is_none() {
            panic!("invalid config.path {}", conf.path);
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
}
