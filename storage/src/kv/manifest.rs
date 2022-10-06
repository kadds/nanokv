pub struct SSTFileInfo {
    name: String,
    meta: FileMetaData,
}

impl SSTFileInfo {}

pub struct LevelFiles {
    files: Vec<SSTFileInfo>,
}

pub struct VersionSet {}

struct Manifest {
    levels: Vec<LevelFiles>,
}

pub struct FileMetaData {
    min: String,
    max: String,
    left: usize,
    right: usize,
    ref_count: u64,
}

impl Manifest {
    pub fn new() {}
}
