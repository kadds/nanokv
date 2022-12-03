use std::{collections::LinkedList, marker::PhantomData, sync::Arc};

use super::{imemtable::Imemtables, manifest::Version, Imemtable, Memtable};

pub struct SuperVersion {
    pub memtable: Arc<Memtable>,
    pub imemtables: Imemtables,
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
