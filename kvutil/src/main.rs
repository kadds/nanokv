use std::path::PathBuf;

use bytes::Bytes;
use clap::Parser;
use storage::{kv::sst::SSTReader, GetOption};
mod opt;

fn main() {
    let cli = opt::Cli::parse();
    let base = cli.path.unwrap_or_else(|| "./".to_owned());
    match cli.command {
        opt::Commands::Sst { subcommand, file } => sst(base, subcommand, file),
        opt::Commands::Manifest => manifest(),
        opt::Commands::Wal => wal(),
        opt::Commands::Db => db(),
    };
}

fn sst(base: String, command: opt::SSTCommands, file: String) {
    let path = PathBuf::from(base).join("sst").join(file);
    match command {
        opt::SSTCommands::Summary => sst_summary(path),
        opt::SSTCommands::Dump(opts) => sst_dump(path, opts.noval),
        opt::SSTCommands::Get(opts) => sst_get(path, opts.key.into(), opts.noval),
    }
}

fn sst_summary(path: PathBuf) {
    let reader = storage::kv::sst::raw_sst::RawSSTReader::new(path).unwrap();
    let meta = reader.meta();
    let min = reader.get_index(0).unwrap().0;
    let max = reader.get_index(meta.total_keys - 1).unwrap().0;

    println!(
        "seq:level {}:{}, keys {}, version {}, index offset {}\nmin {:?}\nmax {:?}",
        meta.seq, meta.level, meta.total_keys, meta.version, meta.index_offset, min, max
    )
}

fn sst_dump(path: PathBuf, noval: bool) {
    let reader = storage::kv::sst::raw_sst::RawSSTReader::new(path).unwrap();
    let iter = reader.scan(
        &GetOption::default(),
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
    );
    for (key, value) in iter {
        print_item(key, value, noval)
    }
}

fn sst_get(path: PathBuf, key: Bytes, noval: bool) {
    let reader = storage::kv::sst::raw_sst::RawSSTReader::new(path).unwrap();
    let iter = reader.scan(
        &GetOption::default(),
        std::ops::Bound::Included(key.clone()),
        std::ops::Bound::Included(key),
    );
    for (key, value) in iter {
        print_item(key, value, noval)
    }
}

fn print_item(key: Bytes, mut value: storage::Value, noval: bool) {
    if noval {
        if value.deleted() {
            println!("{}, {:?}", value.version(), key)
        } else {
            println!("{}, {:?}, exist", value.version(), key);
        }
    } else if value.deleted() {
        println!("{}, {:?} => {:?}", value.version(), key, value.value())
    } else {
        println!(
            "{}, {:?} => {:?}, exist",
            value.version(),
            key,
            value.value()
        );
    }
}

fn manifest() {}

fn wal() {}

fn db() {}
