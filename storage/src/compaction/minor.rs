use std::{sync::Arc, time::Instant};

use log::info;
use threadpool::ThreadPool;

use crate::{
    kv::{
        manifest::{FileMetaData, Manifest},
        sst::{self, SSTWriter},
        Imemtable,
    },
    util::fname,
    ConfigRef,
};

use super::CompactSerializer;

pub struct MinorSerializer {
    pool: ThreadPool,

    #[allow(unused)]
    manifest: Arc<Manifest>,
    config: ConfigRef,

    f: Arc<dyn Fn(FileMetaData) + Sync + Send + 'static>,
}

fn minor_compaction(
    config: ConfigRef,
    table: Arc<Imemtable>,
    f: Arc<dyn Fn(FileMetaData) + Sync + Send + 'static>,
) {
    info!("do minor compaction {}", table.seq());
    let meta = {
        let beg = Instant::now();
        let seq = table.seq();
        let iter = table.entry_iter();
        let mut sst = sst::raw_sst::RawSSTWriter::new(fname::sst_name(config, table.seq()));

        let meta = sst.write(0, seq, iter);

        let end = Instant::now();
        info!("sst {} done, cost {}ms", seq, (end - beg).as_millis());
        meta
    };
    f(meta);
}

impl MinorSerializer {
    pub fn new<F: Fn(FileMetaData) + Send + Sync + 'static>(
        config: ConfigRef,
        manifest: Arc<Manifest>,
        f: F,
    ) -> Arc<Self> {
        Arc::new(Self {
            pool: threadpool::Builder::new()
                .num_threads(config.minor_compaction_threads as usize)
                .thread_name("minor compaction ".into())
                .build(),
            manifest,
            config,
            f: Arc::new(f),
        })
    }

    pub fn compact_async(self: &Arc<Self>, table: Arc<Imemtable>) {
        let this = self.clone();
        let config = this.config;
        let f = this.f.clone();
        this.clone()
            .pool
            .execute(|| minor_compaction(config, table, f));
    }
}

impl Drop for MinorSerializer {
    fn drop(&mut self) {
        self.pool.join();
    }
}

impl CompactSerializer for MinorSerializer {
    fn stop(&self) {
        self.pool.join();
    }
}
