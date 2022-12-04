use std::{fs::File, io::Read, path::PathBuf};

use bytes::Bytes;
use clap::Parser;
use opt::ManifestCommands;
use storage::{
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
    match cli.command {
        opt::Commands::Sst { subcommand, file } => sst(base, subcommand, file),
        opt::Commands::Manifest { subcommand } => manifest(base, subcommand),
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

fn sst_get(path: PathBuf, key: Bytes, noval: bool) {
    let reader = storage::kv::sst::raw_sst::RawSSTReader::new(path).unwrap();
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

fn print_item(key: Bytes, mut value: storage::Value, noval: bool) {
    if noval {
        if value.deleted() {
            println!("del,{},{:?}", value.version(), key)
        } else {
            println!("exist,{},{:?}", value.version(), key);
        }
    } else if value.deleted() {
        println!("del,{},{:?},{:?}", value.version(), key, value.value())
    } else {
        println!("exist,{},{:?},{:?}", value.version(), key, value.value());
    }
}

fn manifest(base: String, command: opt::ManifestCommands) {
    match command {
        ManifestCommands::Current => {
            show_current(base);
        }
        ManifestCommands::Dump { show_step } => dump_manifest(base, show_step),
    }
}

fn show_current(base: String) -> u64 {
    let name = PathBuf::from(base).join("manifest").join("current");
    let mut buf = String::new();
    File::open(name)
        .and_then(|mut f| f.read_to_string(&mut buf))
        .unwrap();

    println!("current {}.log", buf);
    buf.parse().unwrap()
}

fn dump_manifest(base: String, show_step: bool) {
    let seq = show_current(base.clone());
    let mut name = PathBuf::from(base).join("manifest").join(seq.to_string());
    name.set_extension("log");

    let mut r = LogReplayer::new(ManifestLogSerializer::default(), name);
    let final_state = r.execute(VersionSet::default(), |state, edit| {
        if show_step {
            println!("{:?}", edit);
        }
        state.add(&edit);
    });
    println!("{:?}", final_state);
}

fn wal() {}

fn db() {}
