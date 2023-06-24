use std::sync::Arc;

use crate::GetOption;

pub mod imemtable;
pub mod manifest;
pub mod memtable;
pub mod sst;
pub use imemtable::Imemtable;
pub use imemtable::Imemtables;
pub use memtable::Memtable;
pub mod superversion;

pub struct ColumnFamilyTables {
    pub memtable: Arc<Memtable>,
    pub imemtables: Imemtables,
}
