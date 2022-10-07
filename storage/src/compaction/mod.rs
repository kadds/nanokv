use std::sync::Arc;

use crate::kv::Imemtable;

pub mod major;
pub mod minor;

pub trait CompactSerializer {
    fn stop(&self);
}
