use std::{fs, rc::Rc, sync::Arc};

use crate::{config::ConfigRef, coordinator::Coordinator, storage::Storage};

pub struct Instance {
    coordinator: Arc<Coordinator>,
    storage: Storage,
    conf: ConfigRef,
}

impl Instance {
    pub fn new(conf: ConfigRef) -> Self {
        fs::create_dir_all(&conf.path).unwrap();
        let coordinator = Arc::new(Coordinator::load(conf.clone()));
        let tmp = coordinator.clone();
        Self {
            coordinator,
            storage: Storage::new(tmp),
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
