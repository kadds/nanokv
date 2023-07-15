use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};

use log::info;
use rand::RngCore;
use threadpool::ThreadPool;

use crate::{
    backend::Backend,
    iterator::{MergedIter, ScanIter},
    kv::{
        manifest::{FileMetaData, Version, MAX_LEVEL},
        sst::{self, SSTReader, SSTWriter},
        superversion::{Lifetime, SuperVersion},
    },
    util::fname::{self},
    Config,
};

use super::CompactSerializer;

pub type CompactSSTFiles = Vec<FileMetaData>;

pub struct MajorCompactionTaskPool {
    pool: ThreadPool,
    config: Arc<Config>,
    f: Arc<dyn Fn(CompactSSTFiles, Vec<u64>) + Sync + Send + 'static>,
    factor: AtomicU32,
    backend: &'static Backend,
    stop: Arc<AtomicBool>,
}

#[derive(Debug, Default)]
pub struct CompactInfo {
    level_bottom: u32,
    level_top: u32,
    compact_bottom: Vec<u64>,
    compact_top: Vec<u64>,
    number: u64,
}

fn major_compaction(
    info: CompactInfo,
    config: Arc<Config>,
    f: Arc<dyn Fn(CompactSSTFiles, Vec<u64>) + Sync + Send + 'static>,
    backend: &'static Backend,
    stop_flag: Arc<AtomicBool>,
) {
    if stop_flag.load(Ordering::SeqCst) {
        return;
    }

    info!("do major compaction {:?}", info);

    let removal = info
        .compact_bottom
        .iter()
        .chain(info.compact_top.iter())
        .cloned()
        .collect();
    let mut additional = Vec::new();

    let reader: Vec<_> = info
        .compact_bottom
        .iter()
        .chain(info.compact_top.iter())
        .cloned()
        .map(|seq| {
            sst::raw_sst::RawSSTReader::new(
                &fname::sst_name(&config, seq),
                backend,
                config.enable_mmap,
            )
            .unwrap()
        })
        .collect();

    let mut iters = Vec::new();
    let lifetime = Lifetime::default();

    for file_reader in &reader {
        iters.push(file_reader.raw_scan(&lifetime))
    }

    let mut writer =
        sst::raw_sst::RawSSTWriter::new(backend, fname::sst_name(&config, info.number));

    let iter = ScanIter::new(MergedIter::new(iters));

    let meta = match writer.write(info.level_top, info.number, iter) {
        Ok(v) => v,
        Err(e) => {
            log::warn!("major compact fail {:?}", e);
            return;
        }
    };

    additional.push(meta);

    f(additional, removal)
}

impl MajorCompactionTaskPool {
    pub fn new<F: Fn(CompactSSTFiles, Vec<u64>) + Sync + Send + 'static>(
        config: &Config,
        backend: &Backend,
        f: F,
    ) -> Arc<Self> {
        Arc::new(Self {
            pool: threadpool::Builder::new()
                .num_threads(config.major_compaction_threads as usize)
                .thread_name("major compaction ".into())
                .build(),
            config: Arc::new(config.clone()),
            f: Arc::new(f),
            factor: AtomicU32::new(10),
            backend: unsafe { std::mem::transmute(backend) },
            stop: AtomicBool::new(false).into(),
        })
    }

    fn compact_async(&self, info: CompactInfo) {
        let f = self.f.clone();
        let config = self.config.clone();
        let stop_flag = self.stop.clone();
        let backend = self.backend;
        self.pool
            .execute(|| major_compaction(info, config, f, backend, stop_flag))
    }

    pub fn notify(&self, sv: &SuperVersion) {
        let fac = self.factor.load(Ordering::Relaxed);
        let config = &self.config;
        if rand::thread_rng().next_u32() % fac == 0 {
            if let Some(info) = pick_compaction_info(config, &sv.sst_version) {
                self.compact_async(info);
                self.factor.store(2, Ordering::Relaxed);
            } else {
                self.factor.store(3, Ordering::Relaxed);
            }
        }
    }
}

impl Drop for MajorCompactionTaskPool {
    fn drop(&mut self) {
        self.stop();
    }
}

impl CompactSerializer for MajorCompactionTaskPool {
    fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        self.pool.join();
    }
}

fn pick_compaction_info(_config: &Config, version: &Version) -> Option<CompactInfo> {
    // check level 0
    let mut info = CompactInfo::default();
    let mut cancel = false;
    None

    // if version.level_size(0) >= 4 {
    //     let mut num = 0;
    //     for fs in version.level_n(0) {
    //         if fs.is_using_relaxed() {
    //             num += 1;
    //         }
    //     }

    //     if num > 4 {
    //         for fs in version.level_n(0) {
    //             if fs.set_picked() {
    //                 info.compact_bottom.push(fs.meta().number);
    //             }
    //         }
    //     }
    //     // level 1
    //     for fs in version.level_n(1) {
    //         if fs.set_picked() {
    //             info.compact_top.push(fs.meta().number);
    //             break;
    //         }
    //     }
    //     info.level_top = 1;

    //     if info.compact_bottom.len() < 4 {
    //         cancel = true;
    //     }
    // } else {
    //     for level in 1..MAX_LEVEL {
    //         let mut num = 0;
    //         for fs in version.level_n(level) {
    //             if fs.is_using_relaxed() {
    //                 num += 1;
    //             }
    //         }

    //         if num > 1 {
    //             for fs in version.level_n(0) {
    //                 if fs.set_picked() {
    //                     info.compact_bottom.push(fs.meta().number);
    //                 }
    //             }
    //         }
    //     }
    // }
    // if info.compact_bottom.is_empty() {
    //     cancel = true;
    // }

    // if cancel {
    //     for seq in info.compact_bottom {
    //         for fs in version.level_n(info.level_bottom) {
    //             if fs.meta().number == seq {
    //                 fs.set_using();
    //             }
    //         }
    //     }
    //     for seq in info.compact_top {
    //         for fs in version.level_n(info.level_top) {
    //             if fs.meta().number == seq {
    //                 fs.set_using();
    //             }
    //         }
    //     }
    //     None
    // } else {
    //     Some(info)
    // }
}
