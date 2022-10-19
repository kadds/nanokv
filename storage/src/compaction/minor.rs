use std::{
    cell::UnsafeCell,
    sync::{mpsc, Arc},
    thread::{self, JoinHandle},
};

use log::info;

use crate::{
    kv::{
        manifest::Manifest,
        sst::{self, sst_name, SSTWriter},
        Imemtable,
    },
    ConfigRef,
};

use super::CompactSerializer;

pub trait TableGetter {
    fn get(&self) -> &Imemtable;
}

pub struct MinorSerializer {
    thread: UnsafeCell<Option<JoinHandle<()>>>,
    rx: mpsc::Receiver<Option<Arc<Imemtable>>>,
    tx: mpsc::Sender<Option<Arc<Imemtable>>>,
    commit_tx: mpsc::SyncSender<Arc<Imemtable>>,
    manifest: Arc<Manifest>,
}

impl MinorSerializer {
    pub fn new(
        conf: ConfigRef,
        manifest: Arc<Manifest>,
        commit_tx: mpsc::SyncSender<Arc<Imemtable>>,
    ) -> Arc<Self> {
        let (tx, rx) = mpsc::channel();
        let this = Arc::new(Self {
            thread: UnsafeCell::new(None),
            rx,
            tx,
            commit_tx,
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
            let table = match self.rx.recv().unwrap() {
                Some(table) => table,
                None => break,
            };
            {
                let seq = table.seq();
                let iter = table.entry_iter().filter(|entry| !entry.deleted());
                let mut sst =
                    sst::raw_sst::RawSSTWriter::new(&sst_name(&conf.path, 0, table.seq()));

                let meta = sst.write(0, seq, iter);
                self.manifest.add_sst(Arc::new(meta));

                info!("sst {} done", seq);
            }
            self.commit_tx.send(table).unwrap();
        }
    }

    pub fn compact_async(&self, table: Arc<Imemtable>) {
        self.tx.send(Some(table)).unwrap();
    }
}

impl Drop for MinorSerializer {
    fn drop(&mut self) {
        let _ = self.tx.send(None);
    }
}

impl CompactSerializer for MinorSerializer {
    fn stop(&self) {
        self.tx.send(None).unwrap();
    }
}

unsafe impl Sync for MinorSerializer {}
