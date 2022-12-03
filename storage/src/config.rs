use serde_derive::Deserialize;
use serde_derive::Serialize;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

#[derive(Deserialize, Serialize, Debug)]
pub struct Config {
    pub path: PathBuf,
    pub no_wal: bool,
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

pub fn test_config() -> Box<Config> {
    let cfg = Config {
        path: "/tmp/nanokv/".into(),
        no_wal: false,
    };
    cfg.into()
}

pub fn current_config() -> Box<Config> {
    let cfg = Config {
        path: "nanokv_data/".into(),
        no_wal: false,
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
