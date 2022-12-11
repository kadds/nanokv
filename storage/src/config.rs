use serde_derive::Deserialize;
use serde_derive::Serialize;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

#[derive(Deserialize, Serialize, Debug)]
pub struct Config {
    pub path: PathBuf,
    pub no_wal: bool,
    pub minor_compaction_threads: u32,
    pub major_compaction_threads: u32,
    pub l0_compaction_files: u32,
    pub lx_compaction_files: u32,
    pub leveled_compaction_level: u32,
    pub size_tried_radio: u32,
    pub level_data_radio: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: Default::default(),
            no_wal: false,
            minor_compaction_threads: 4,
            major_compaction_threads: 2,
            l0_compaction_files: 4,
            lx_compaction_files: 3,
            leveled_compaction_level: 2,
            size_tried_radio: 10,
            level_data_radio: 10,
        }
    }
}

impl Config {
    pub fn set_no_wal(&mut self, no_wal: bool) {
        self.no_wal = no_wal;
    }
}

pub type ConfigRef = &'static Config;

pub fn load_config() -> Box<Config> {
    load_config_from("config.toml")
}

pub fn current_config() -> Box<Config> {
    let cfg = Config {
        path: "nanokv_data/".into(),
        no_wal: false,
        ..Default::default()
    };
    cfg.into()
}

pub fn load_config_from(file: &str) -> Box<Config> {
    let mut buf = String::new();
    File::open(file)
        .and_then(|mut f| f.read_to_string(&mut buf))
        .unwrap();

    toml::from_str(&buf).unwrap()
}

pub fn test_config() -> Box<Config> {
    let path = std::env::temp_dir();
    let cfg = Config {
        path,
        no_wal: true,
        ..Default::default()
    };
    cfg.into()
}
