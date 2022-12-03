use std::{fs, path::PathBuf};

use crate::ConfigRef;

pub fn sst_name(config: ConfigRef, seq: u64) -> PathBuf {
    let mut base = config.path.join("sst").join(seq.to_string());
    base.set_extension("sst");
    base
}

pub fn wal_name(config: ConfigRef, seq: u64) -> PathBuf {
    let mut base = config.path.join(seq.to_string());
    base.set_extension("log");
    base
}

pub fn manifest_name(config: ConfigRef, seq: u64) -> PathBuf {
    let mut base = config.path.join("manifest").join(seq.to_string());
    base.set_extension("log");
    base
}

pub fn manifest_current(config: ConfigRef) -> PathBuf {
    let base = config.path.join("manifest").join("current");
    base
}

pub fn manifest_current_tmp(config: ConfigRef) -> PathBuf {
    let base = config.path.join("manifest").join("current_tmp");
    base
}

pub fn make_sure(config: ConfigRef) {
    let sst_name = sst_name(config, 0);
    let wal_name = wal_name(config, 0);
    let manifest_name = manifest_name(config, 0);

    let _ = fs::create_dir_all(sst_name.parent().unwrap());
    let _ = fs::create_dir_all(wal_name.parent().unwrap());
    let _ = fs::create_dir_all(manifest_name.parent().unwrap());
}
