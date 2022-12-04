use std::sync::{Arc, Mutex};

use lru::LruCache;

use crate::{
    kv::sst::{raw_sst::RawSSTReader, SSTReader},
    util::fname,
    ConfigRef,
};

pub struct Cache {
    opened_sst: Mutex<LruCache<u64, Arc<dyn SSTReader>>>,
    config: ConfigRef,
}

impl Cache {
    pub fn new(config: ConfigRef) -> Self {
        Self {
            opened_sst: Mutex::new(LruCache::new(200.try_into().unwrap())),
            config,
        }
    }
}

impl Cache {
    pub fn get_opened_sst(&self, _level: u32, seq: u64) -> Arc<dyn SSTReader> {
        let sst_path = fname::sst_name(self.config, seq);

        self.opened_sst
            .lock()
            .unwrap()
            .get_or_insert(seq, || Arc::new(RawSSTReader::new(sst_path).unwrap()))
            .clone()
    }
}
