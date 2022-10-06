use std::{
    fs::File,
    io::{Read, Write},
    sync::Mutex,
};

use serde_derive::{Deserialize, Serialize};

use crate::config::ConfigRef;

const VERSION_STEP: u64 = 100000;
const SST_SEQ_STEP: u64 = 10;

#[derive(Debug, Serialize, Deserialize)]
struct GlobalData {
    #[serde(default)]
    version: u64,
    #[serde(default)]
    sst_seq: u64,
}

pub struct Coordinator {
    data: Mutex<GlobalData>,
    conf: ConfigRef,
}

impl Coordinator {
    pub fn load(conf: ConfigRef) -> Self {
        let filename = format!("{}/version.toml", conf.path);
        let mut buf = String::new();
        // ignore error if file doesn't exist
        let _ = File::open(filename).and_then(|mut f| f.read_to_string(&mut buf));
        let mut data: GlobalData = toml::from_str(&buf).unwrap();
        data.sst_seq += SST_SEQ_STEP;
        data.version += VERSION_STEP;
        Self {
            data: Mutex::new(data),
            conf,
        }
    }

    fn save(&self, data: &GlobalData) {
        let filename = format!("{}/version.toml", self.conf.path);
        let mut file = File::create(filename).unwrap();
        let buf = toml::to_string(data).unwrap();
        file.write_all(&buf.as_bytes()).unwrap();
        file.sync_all().unwrap();
    }

    pub fn current_sst_file_seq(&self) -> u64 {
        self.data.lock().unwrap().sst_seq
    }

    pub fn next_sst_file_seq(&self) -> u64 {
        let mut data = self.data.lock().unwrap();
        let val = data.sst_seq;
        if val % SST_SEQ_STEP == 0 {
            self.save(&data);
        }
        data.sst_seq += 1;
        val
    }

    pub fn next_version(&self) -> u64 {
        let mut data = self.data.lock().unwrap();
        let val = data.version;
        if val % VERSION_STEP == 0 {
            self.save(&data);
        }
        data.version += 1;
        val
    }

    pub fn config(&self) -> ConfigRef {
        self.conf.clone()
    }
}
