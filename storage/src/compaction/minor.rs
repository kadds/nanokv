use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

use log::info;
use threadpool::ThreadPool;

use crate::{
    backend::Backend,
    kv::{
        manifest::FileMetaData,
        sst::{self, SSTWriter},
        Memtable,
    },
    util::fname,
    Config,
};

use super::CompactSerializer;

pub struct MinorCompactionTaskPool {
    pool: ThreadPool,
    stop: Arc<AtomicBool>,

    config: Arc<Config>,
    backend: &'static Backend,

    f: Arc<dyn Fn(FileMetaData) + Sync + Send + 'static>,
}

fn minor_compaction(
    config: Arc<Config>,
    table: Arc<Memtable>,
    backend: &'static Backend,
    f: Arc<dyn Fn(FileMetaData) + Sync + Send + 'static>,
) {
    info!("do minor compaction {}", table.number());
    let meta = {
        let beg = Instant::now();
        let number = table.number();
        let iter = table.iter();
        let mut sst =
            sst::raw_sst::RawSSTWriter::new(backend, fname::sst_name(&config, table.number()));

        let meta = match sst.write(0, number, iter.map(|v| (v.0.clone(), v.1.clone().into()))) {
            Ok(v) => v,
            Err(e) => {
                log::warn!("minor compaction fail {:?}", e);
                return;
            }
        };

        let end = Instant::now();
        info!("sst {} done, cost {}ms", number, (end - beg).as_millis());
        meta
    };
    f(meta);
}

impl MinorCompactionTaskPool {
    pub fn new<F: Fn(FileMetaData) + Send + Sync + 'static>(
        config: &Config,
        backend: &Backend,
        f: F,
    ) -> Arc<Self> {
        Arc::new(Self {
            pool: threadpool::Builder::new()
                .num_threads(config.minor_compaction_threads as usize)
                .thread_name("minor compaction ".into())
                .build(),
            config: Arc::new(config.clone()),
            f: Arc::new(f),
            backend: unsafe { std::mem::transmute(backend) },
            stop: AtomicBool::new(false).into(),
        })
    }

    pub fn compact_async(self: &Arc<Self>, table: Arc<Memtable>) {
        let this = self.clone();
        let config = this.config.clone();
        let f = this.f.clone();
        let _stop_flag = this.stop.clone();
        let backend = self.backend;
        this.clone()
            .pool
            .execute(move || minor_compaction(config, table, backend, f));
    }
}

impl Drop for MinorCompactionTaskPool {
    fn drop(&mut self) {
        self.stop();
    }
}

impl CompactSerializer for MinorCompactionTaskPool {
    fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        self.pool.join();
    }
}
