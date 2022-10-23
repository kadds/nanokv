use std::sync::{Arc, Mutex};

use lru::LruCache;

use crate::{
    kv::sst::{raw_sst::RawSSTReader, sst_name, SSTReader},
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
    pub fn get_opened_sst(&self, level: u32, seq: u64) -> Arc<dyn SSTReader> {
        self.opened_sst
            .lock()
            .unwrap()
            .get_or_insert(seq, || {
                Arc::new(RawSSTReader::new(
                    &sst_name(&self.config.path, level, seq),
                    seq,
                ))
            })
            .clone()
    }
}
