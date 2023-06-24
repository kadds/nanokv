use std::{fs, path::PathBuf};

use crate::{Config, ConfigRef};

pub fn sst_name(config: &Config, seq: u64) -> PathBuf {
    let mut base = config.path.join("sst").join(seq.to_string());
    base.set_extension("sst");
    base
}

pub fn wal_name(config: &Config, seq: u64) -> PathBuf {
    let mut base = config.path.join(seq.to_string());
    base.set_extension("log");
    base
}

pub fn manifest_name(config: &Config, seq: u64) -> PathBuf {
    let mut base = config.path.join("manifest").join(seq.to_string());
    base.set_extension("log");
    base
}

pub fn manifest_current(config: &Config) -> PathBuf {
    config.path.join("manifest").join("current")
}

pub fn manifest_current_tmp(config: &Config) -> PathBuf {
    config.path.join("manifest").join("current_tmp")
}
