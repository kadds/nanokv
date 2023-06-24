use std::{
    sync::{
        self,
        atomic::{AtomicBool, Ordering},
        mpsc::Receiver,
        Arc,
    },
    time::Instant,
};

use log::info;
use threadpool::ThreadPool;

use crate::{
    kv::{
        manifest::FileMetaData,
        sst::{self, SSTWriter},
        Imemtable,
    },
    util::fname,
    Config, ConfigRef,
};

use super::CompactSerializer;

pub struct MinorCompactionTaskPool {
    pool: ThreadPool,
    stop: Arc<AtomicBool>,

    config: Arc<Config>,

    f: Arc<dyn Fn(FileMetaData) + Sync + Send + 'static>,
}

fn minor_compaction(
    config: Arc<Config>,
    table: Arc<Imemtable>,
    f: Arc<dyn Fn(FileMetaData) + Sync + Send + 'static>,
) {
    info!("do minor compaction {}", table.number());
    let meta = {
        let beg = Instant::now();
        let number = table.number();
        let iter = table.entry_iter();
        let mut sst = sst::raw_sst::RawSSTWriter::new(fname::sst_name(&config, table.number()));

        let meta = match sst.write(0, number, iter.map(|v| (v.0, v.1.into()))) {
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
    pub fn new<F: Fn(FileMetaData) + Send + Sync + 'static>(config: &Config, f: F) -> Arc<Self> {
        Arc::new(Self {
            pool: threadpool::Builder::new()
                .num_threads(config.minor_compaction_threads as usize)
                .thread_name("minor compaction ".into())
                .build(),
            config: Arc::new(config.clone()),
            f: Arc::new(f),
            stop: AtomicBool::new(false).into(),
        })
    }

    pub fn compact_async(self: &Arc<Self>, table: Arc<Imemtable>) {
        let this = self.clone();
        let config = this.config.clone();
        let f = this.f.clone();
        let stop_flag = this.stop.clone();
        this.clone()
            .pool
            .execute(move || minor_compaction(config, table, f));
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
