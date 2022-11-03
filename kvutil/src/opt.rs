use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
pub struct SSTDumpOptions {
    /// Don't print value
    #[arg(long)]
    pub noval: bool,
}

#[derive(Parser, Debug)]
pub struct SSTGetOptions {
    /// Don't print value
    #[arg(long)]
    pub noval: bool,

    /// key searched from sst file
    #[arg(short, long)]
    pub key: String,
}

#[derive(Subcommand, Debug)]
pub enum SSTCommands {
    /// Print summary&meta data
    Summary,
    /// Print all keys/values
    Dump(SSTDumpOptions),
    /// Get single key
    Get(SSTGetOptions),
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Load summary/dump data from sst file
    Sst {
        #[command(subcommand)]
        subcommand: SSTCommands,
        #[arg(short, long)]
        file: String,
    },
    /// Load current version info from manifest log
    Manifest,
    /// Parse write ahead log
    Wal,
    /// Database operations like repair/summary/print
    Db,
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Main command
    #[command(subcommand)]
    pub command: Commands,

    /// Database basic path
    #[arg(short, long, value_name = "PATH", default_value = "./nanokv_data")]
    pub path: Option<String>,
}
