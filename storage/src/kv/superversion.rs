use std::{marker::PhantomData, sync::Arc};

use super::{manifest::Version, ColumnFamilyTables};

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
