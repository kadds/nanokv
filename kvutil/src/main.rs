use std::{
    borrow::Borrow,
    io::Read,
    path::{Path, PathBuf},
};

use bytes::Bytes;
use clap::Parser;
use opt::ManifestCommands;
use storage::{
    backend::{
        fs::{local::LocalFileBasedPersistBackend, ExtReader},
        Backend,
    },
    iterator::KvIteratorItem,
    key::{InternalKey, KeyType, Value},
    kv::{
        manifest::{ManifestLogSerializer, VersionSet},
        sst::SSTReader,
        superversion::Lifetime,
    },
    log::LogReplayer,
    GetOption,
};
mod opt;

fn main() {
    let cli = opt::Cli::parse();
    let base = cli.path.unwrap_or_else(|| "./".to_owned());
    let backend = Backend::new(LocalFileBasedPersistBackend::default());
    match cli.command {
        opt::Commands::Sst { subcommand, file } => sst(base, subcommand, file, &backend),
        opt::Commands::Manifest { subcommand } => manifest(base, subcommand, &backend),
        opt::Commands::Wal => wal(),
        opt::Commands::Db => db(),
    };
}

fn sst(base: String, command: opt::SSTCommands, file: String, backend: &Backend) {
    let path = PathBuf::from(base).join("sst").join(file);
    match command {
        opt::SSTCommands::Summary => sst_summary(&path, backend),
        opt::SSTCommands::Dump(opts) => sst_dump(&path, opts.noval, backend),
        opt::SSTCommands::Get(opts) => sst_get(&path, opts.key.into(), opts.noval, backend),
    }
}

fn sst_summary(path: &Path, backend: &Backend) {
    let reader = storage::kv::sst::raw_sst::RawSSTReader::new(path, backend, false).unwrap();
    let meta = reader.meta();
    let min = reader.get_index(0).unwrap().0;
    let max = reader.get_index(meta.total_keys - 1).unwrap().0;

    println!(
        "seq:level {}:{}, keys {}, version {}, index offset {}\nmin {:?}\nmax {:?}",
        meta.number, meta.level, meta.total_keys, meta.version, meta.index_offset, min, max
    )
}

fn sst_dump(path: &Path, noval: bool, backend: &Backend) {
    let reader = storage::kv::sst::raw_sst::RawSSTReader::new(path, backend, false).unwrap();
    let lifetime = Lifetime::default();
    let iter = reader.scan(
        &GetOption::default(),
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        &lifetime,
    );
    for (key, value) in iter {
        print_item(key, value, noval)
    }
}

fn sst_get(path: &Path, key: Bytes, noval: bool, backend: &Backend) {
    let reader = storage::kv::sst::raw_sst::RawSSTReader::new(path, backend, false).unwrap();
    let lifetime = Lifetime::default();
    let iter = reader.scan(
        &GetOption::default(),
        std::ops::Bound::Included(key.clone()),
        std::ops::Bound::Included(key),
        &lifetime,
    );
    for (key, value) in iter {
        print_item(key, value, noval)
    }
}

fn print_item(key: InternalKey, value: Value, noval: bool) {
    if noval {
        if key.key_type() == KeyType::Del {
            println!("del,{},{:?}", key.seq(), key.user_key_slice())
        } else {
            println!("exist,{},{:?}", key.seq(), key.user_key_slice());
        }
    } else if key.key_type() == KeyType::Del {
        println!(
            "del,{},{:?},{:?}",
            key.seq(),
            key.user_key_slice(),
            value.data()
        )
    } else {
        println!(
            "exist,{},{:?},{:?}",
            key.seq(),
            key.user_key_slice(),
            value.data()
        );
    }
}

fn manifest(base: String, command: opt::ManifestCommands, backend: &Backend) {
    match command {
        ManifestCommands::Current => {
            show_current(base, backend);
        }
        ManifestCommands::Dump { show_step } => dump_manifest(base, show_step, backend),
    }
}

fn show_current(base: String, backend: &Backend) -> u64 {
    let name = PathBuf::from(base).join("manifest").join("current");
    let mut buf = String::new();
    backend
        .fs
        .open(&name, false)
        .and_then(|f| Ok(ExtReader::new(f.borrow(), 0, f.size()).read_to_string(&mut buf)?))
        .unwrap();

    println!("current {}.log", buf);
    buf.parse().unwrap()
}

fn dump_manifest(base: String, show_step: bool, backend: &Backend) {
    let seq = show_current(base.clone(), backend);
    let mut name = PathBuf::from(base).join("manifest").join(seq.to_string());
    name.set_extension("log");

    let final_state = {
        let r = LogReplayer::new(&backend, ManifestLogSerializer);
        let mut state = VersionSet::default();
        for edit in r.iter(name).unwrap() {
            let edit = edit.unwrap();
            if show_step {
                println!("{:?}", edit);
            }
            state.add(&edit);
        }
        state
    };
    println!("{:?}", final_state);
}

fn wal() {}

fn db() {}
