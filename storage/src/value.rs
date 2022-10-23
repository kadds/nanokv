use bytes::Bytes;

use crate::{iterator::KvIteratorItem, kv::KvEntry};

enum LazyValueRef {
    Inplace(Bytes),
    Extern(u64),
    Del,
}

pub struct Value {
    value: LazyValueRef,
    ver: u64,
}

impl Value {
    pub(crate) fn new(value: Bytes, ver: u64) -> Self {
        Self {
            value: LazyValueRef::Inplace(value),
            ver,
        }
    }
    // pub fn new_extern_value(value: Bytes, ver: u64) -> Self {
    //     Self { value, ver, exist: true}
    // }
    pub(crate) fn new_deleted(ver: u64) -> Self {
        Self {
            value: LazyValueRef::Del,
            ver,
        }
    }
    pub(crate) fn from_entry(entry: &KvEntry) -> Self {
        if entry.big_value() {
            todo!();
        } else {
            if entry.deleted() {
                Self::new_deleted(entry.version())
            } else {
                Self::new(entry.value(), entry.version())
            }
        }
    }
}

impl Value {
    pub fn value(&mut self) -> Bytes {
        match &self.value {
            LazyValueRef::Del => Bytes::new(),
            LazyValueRef::Inplace(val) => val.clone(),
            LazyValueRef::Extern(offset) => {
                let data = Bytes::new();
                // TODO: load vlog value
                data
            }
        }
    }
    pub fn deleted(&self) -> bool {
        match self.value {
            LazyValueRef::Del => true,
            _ => false,
        }
    }
    pub fn version(&self) -> u64 {
        self.ver
    }
}

impl KvIteratorItem for (Bytes, Value) {
    fn key(&self) -> &Bytes {
        &self.0
    }

    fn version(&self) -> u64 {
        self.1.version()
    }
}
