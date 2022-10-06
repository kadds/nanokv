use super::KvEntry;

pub mod raw_sst;

pub trait SSTWriter {
    fn write_level_x(&mut self, level: u32, entry_iter: Box<dyn Iterator<Item = KvEntry>>);
}
