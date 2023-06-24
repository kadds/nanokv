use std::sync::{Arc, Mutex};

use lru::LruCache;

use crate::{
    kv::sst::{raw_sst::RawSSTReader, SSTReader},
    util::fname,
    Config,
};

pub struct Cache {
    opened_sst: Mutex<LruCache<u64, Arc<dyn SSTReader + Send + Sync>>>,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            opened_sst: Mutex::new(LruCache::new(200.try_into().unwrap())),
        }
    }
}

impl Cache {
    pub fn get_opened_sst(
        &self,
        config: &Config,
        _level: u32,
        seq: u64,
    ) -> Arc<dyn SSTReader + Send + Sync> {
        let sst_path = fname::sst_name(config, seq);

        self.opened_sst
            .lock()
            .unwrap()
            .get_or_insert(seq, || Arc::new(RawSSTReader::new(sst_path).unwrap()))
            .clone()
    }
}
