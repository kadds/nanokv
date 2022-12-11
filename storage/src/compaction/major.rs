use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};

use log::info;
use rand::RngCore;
use threadpool::ThreadPool;

use crate::{
    iterator::{MergedIter, ScanIter},
    kv::{
        manifest::{FileMetaData, Manifest, Version, MAX_LEVEL},
        sst::{self, SSTReader, SSTWriter},
        superversion::{Lifetime, SuperVersion},
    },
    util::fname::{self},
    ConfigRef,
};

use super::CompactSerializer;

pub type CompactSSTFiles = Vec<FileMetaData>;

pub struct MajorSerializer {
    pool: ThreadPool,
    manifest: Arc<Manifest>,
    config: ConfigRef,
    f: Arc<dyn Fn(CompactSSTFiles, Vec<u64>) + Sync + Send + 'static>,
    factor: AtomicU32,
    stop: Arc<AtomicBool>,
}

#[derive(Debug, Default)]
pub struct CompactInfo {
    level_bottom: u32,
    level_top: u32,
    compact_bottom: Vec<u64>,
    compact_top: Vec<u64>,
}

fn major_compaction(
    info: CompactInfo,
    config: ConfigRef,
    manifest: Arc<Manifest>,
    f: Arc<dyn Fn(CompactSSTFiles, Vec<u64>) + Sync + Send + 'static>,
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
        .map(|seq| sst::raw_sst::RawSSTReader::new(fname::sst_name(config, seq)).unwrap())
        .collect();

    let mut iters = Vec::new();
    let lifetime = Lifetime::default();

    for file_reader in &reader {
        iters.push(file_reader.raw_scan(&lifetime))
    }
    let new_seq = manifest.allocate_sst_sequence();

    let mut writer = sst::raw_sst::RawSSTWriter::new(fname::sst_name(config, new_seq));

    let iter = ScanIter::new(MergedIter::new(iters));

    let meta = match writer.write(info.level_top, new_seq, iter, &stop_flag) {
        Some(v) => v,
        None => {
            return;
        }
    };

    info!("major compaction {} done", new_seq);
    additional.push(meta);

    f(additional, removal)
}

impl MajorSerializer {
    pub fn new<F: Fn(CompactSSTFiles, Vec<u64>) + Sync + Send + 'static>(
        config: ConfigRef,
        manifest: Arc<Manifest>,
        f: F,
    ) -> Arc<Self> {
        Arc::new(Self {
            pool: threadpool::Builder::new()
                .num_threads(config.major_compaction_threads as usize)
                .thread_name("major compaction ".into())
                .build(),
            manifest,
            config,
            f: Arc::new(f),
            factor: AtomicU32::new(10),
            stop: AtomicBool::new(false).into(),
        })
    }

    fn compact_async(&self, info: CompactInfo) {
        let f = self.f.clone();
        let config = self.config;
        let m = self.manifest.clone();
        let stop_flag = self.stop.clone();
        self.pool
            .execute(|| major_compaction(info, config, m, f, stop_flag))
    }

    pub fn notify(&self, sv: &SuperVersion) {
        let fac = self.factor.load(Ordering::Relaxed);
        let config = self.config;
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

impl Drop for MajorSerializer {
    fn drop(&mut self) {
        self.stop();
    }
}

impl CompactSerializer for MajorSerializer {
    fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        self.pool.join();
    }
}

fn pick_compaction_info(_config: ConfigRef, version: &Version) -> Option<CompactInfo> {
    // check level 0
    let mut info = CompactInfo::default();
    let mut cancel = false;

    if version.level_size(0) >= 4 {
        let mut num = 0;
        for fs in version.level_n(0) {
            if fs.is_using_relaxed() {
                num += 1;
            }
        }

        if num > 4 {
            for fs in version.level_n(0) {
                if fs.set_picked() {
                    info.compact_bottom.push(fs.meta().seq);
                }
            }
        }
        // level 1
        for fs in version.level_n(1) {
            if fs.set_picked() {
                info.compact_top.push(fs.meta().seq);
                break;
            }
        }
        info.level_top = 1;

        if info.compact_bottom.len() < 4 {
            cancel = true;
        }
    } else {
        for level in 1..MAX_LEVEL {
            let mut num = 0;
            for fs in version.level_n(level) {
                if fs.is_using_relaxed() {
                    num += 1;
                }
            }

            if num > 1 {
                for fs in version.level_n(0) {
                    if fs.set_picked() {
                        info.compact_bottom.push(fs.meta().seq);
                    }
                }
            }
        }
    }
    if info.compact_bottom.is_empty() {
        cancel = true;
    }

    if cancel {
        for seq in info.compact_bottom {
            for fs in version.level_n(info.level_bottom) {
                if fs.meta().seq == seq {
                    fs.set_using();
                }
            }
        }
        for seq in info.compact_top {
            for fs in version.level_n(info.level_top) {
                if fs.meta().seq == seq {
                    fs.set_using();
                }
            }
        }
        None
    } else {
        Some(info)
    }
}
