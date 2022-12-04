use std::{collections::LinkedList, marker::PhantomData, sync::Arc};

use arc_swap::ArcSwap;

use super::{imemtable::Imemtables, manifest::Version, ColumnFamilyTables, Imemtable, Memtable};

pub struct SuperVersion {
    pub cf_tables: Arc<ColumnFamilyTables>,
    pub sst_version: Arc<Version>,
    pub step_version: u64,
}

impl SuperVersion {
    pub fn lifetime<'a>(&'a self) -> Lifetime<'a> {
        Lifetime::default()
    }
}

#[derive(Debug, Default, Clone)]
pub struct Lifetime<'a> {
    _pd: PhantomData<&'a ()>,
}
