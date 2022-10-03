use std::{
    fs::File,
    io::{Read, Write},
    sync::atomic::{AtomicU64, Ordering},
};

use crate::config::ConfigRef;

const VERSION_STEP: u64 = 100000;

pub struct Coordinator {
    ver: AtomicU64,
    conf: ConfigRef,
}

impl Coordinator {
    pub fn load(conf: ConfigRef) -> Self {
        let filename = format!("{}/VERSION", conf.path);
        let mut buf = String::new();
        // ignore error if file doesn't exist
        let _ = File::open(filename)
            .and_then(|mut f| f.read_to_string(&mut buf));
        let ver = if buf.len() == 0 {
            0
        } else {
            buf.parse::<u64>().expect("VERSION file invalid") + VERSION_STEP
        };

        Self {
            ver: AtomicU64::new(ver),
            conf,
        }
    }

    fn save(&self, ver: u64) {
        let filename = format!("{}/VERSION", self.conf.path);
        let mut file = File::create(filename).unwrap();
        write!(file, "{}", ver).unwrap();
    }

    pub fn next_version(&self) -> u64 {
        let val = self.ver.fetch_add(1, Ordering::AcqRel);
        if val % VERSION_STEP == 0 {
            self.save(val);
        }
        val
    }
}
