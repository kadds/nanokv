use std::{
    cell::UnsafeCell,
    sync::{mpsc, Arc},
    thread::{self, JoinHandle},
};

use crate::{kv::manifest::Manifest, ConfigRef};

use super::CompactSerializer;

pub struct MajorCompactTask {
    level_bottom: u32,
    level_top: u32,
    bottom_seqs: Vec<u64>,
    top_seqs: Vec<u64>,
}

pub struct MajorSerializer {
    thread: UnsafeCell<Option<JoinHandle<()>>>,
    rx: mpsc::Receiver<Option<u64>>,
    tx: mpsc::Sender<Option<u64>>,
    manifest: Arc<Manifest>,
}

impl MajorSerializer {
    pub fn new(conf: ConfigRef, manifest: Arc<Manifest>) -> Arc<Self> {
        let (tx, rx) = mpsc::channel();
        let this = Arc::new(Self {
            thread: UnsafeCell::new(None),
            tx,
            rx,
            manifest,
        });
        let that = this.clone();
        unsafe {
            let mut_thread = &mut *this.thread.get();
            *mut_thread = Some(thread::spawn(move || {
                that.main(conf);
            }));
        }
        this
    }

    fn main(&self, conf: ConfigRef) {
        loop {
            let sst_seq = match self.rx.recv().unwrap() {
                Some(sst) => sst,
                None => break,
            };
        }
    }

    pub fn compact_async(&self, sst_file: u64) {
        self.tx.send(Some(sst_file)).unwrap();
    }
}

impl Drop for MajorSerializer {
    fn drop(&mut self) {}
}

impl CompactSerializer for MajorSerializer {
    fn stop(&self) {}
}

unsafe impl Sync for MajorSerializer {}
