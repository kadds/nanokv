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
    stop: Arc<AtomicBool>,

    #[allow(unused)]
    manifest: Arc<Manifest>,
    config: ConfigRef,

    f: Arc<dyn Fn(FileMetaData) + Sync + Send + 'static>,
}

fn minor_compaction(
    config: ConfigRef,
    table: Arc<Imemtable>,
    f: Arc<dyn Fn(FileMetaData) + Sync + Send + 'static>,
    stop_flag: Arc<AtomicBool>,
) {
    if stop_flag.load(Ordering::SeqCst) {
        return;
    }

    info!("do minor compaction {}", table.seq());
    let meta = {
        let beg = Instant::now();
        let seq = table.seq();
        let iter = table.entry_iter();
        let mut sst = sst::raw_sst::RawSSTWriter::new(fname::sst_name(config, table.seq()));

        let meta = match sst.write(0, seq, iter, &stop_flag) {
            Some(v) => v,
            None => {
                return;
            }
        };

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
            stop: AtomicBool::new(false).into(),
        })
    }

    pub fn compact_async(self: &Arc<Self>, table: Arc<Imemtable>) {
        let this = self.clone();
        let config = this.config;
        let f = this.f.clone();
        let stop_flag = this.stop.clone();
        this.clone()
            .pool
            .execute(move || minor_compaction(config, table, f, stop_flag));
    }
}

impl Drop for MinorSerializer {
    fn drop(&mut self) {
        self.stop();
    }
}

impl CompactSerializer for MinorSerializer {
    fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        self.pool.join();
    }
}
