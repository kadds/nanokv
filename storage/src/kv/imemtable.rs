use std::{
    collections::HashSet,
    ops::RangeBounds,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use bytes::Bytes;

use super::{superversion::Lifetime, GetOption, KvEntry, Memtable};
use crate::{
    iterator::{EqualFilter, MergedIter, ScanIter},
    kv::kv_entry_ref_to_value,
    value::Value,
};
use superslice::*;

#[derive(Debug)]
pub struct Imemtable {
    keys: Vec<KvEntry>,
    seq: u64,

    min_ver: u64,
    max_ver: u64,
    flushed: AtomicBool,
}

impl Imemtable {
    pub fn new(memtable: &Memtable, reserved_version: u64) -> Self {
        let seq = memtable.seq();
        let cap = memtable.len();
        let mut keys = Vec::with_capacity(cap);
        let mut min_ver = u64::MAX;
        let mut max_ver = u64::MIN;

        for item in memtable.iter() {
            min_ver = min_ver.min(item.version());
            max_ver = max_ver.max(item.version());
            keys.push(item.to_owned());
        }

        let mut map = HashSet::new();
        let new_keys = keys
            .into_iter()
            .filter(|item| {
                if item.version() < reserved_version {
                    match map.get(&item.key) {
                        Some(_) => false,
                        None => {
                            map.insert(item.key.clone());
                            true
                        }
                    }
                } else {
                    true
                }
            })
            .collect();
        keys = new_keys;

        Self {
            keys,
            seq,
            min_ver,
            max_ver,
            flushed: AtomicBool::new(false),
        }
    }
}

impl Imemtable {
    pub fn min_max(&self) -> Option<(Bytes, Bytes)> {
        if !self.keys.is_empty() {
            Some((
                self.keys.first().unwrap().key(),
                self.keys.last().unwrap().key(),
            ))
        } else {
            None
        }
    }
    pub fn min_max_ver(&self) -> Option<(u64, u64)> {
        if !self.keys.is_empty() {
            Some((self.min_ver, self.max_ver))
        } else {
            None
        }
    }
    pub fn entry_iter<'a>(&'a self) -> Box<dyn Iterator<Item = KvEntry> + 'a> {
        Box::new(self.keys.iter().cloned())
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn set_flush(&self) {
        self.flushed.store(true, Ordering::Relaxed);
    }

    pub fn is_flushed(&self) -> bool {
        self.flushed.load(Ordering::Relaxed)
    }
}

impl Imemtable {
    pub fn get<'a>(&self, opt: &GetOption, key: Bytes, _lifetime: &Lifetime<'a>) -> Option<Value> {
        let mut range = self.keys.equal_range_by(|entry| entry.key().cmp(&key));

        if range.is_empty() {
            return None;
        }

        if let Some(snapshot) = &opt.snapshot() {
            for idx in range {
                if self.keys[idx].version() <= snapshot.version() {
                    return Some(Value::from_entry(&self.keys[idx]));
                }
            }
        } else {
            let idx = unsafe { range.next().unwrap_unchecked() };
            return Some(Value::from_entry(&self.keys[idx]));
        }
        None
    }

    pub fn scan<'a, R: RangeBounds<Bytes> + Clone>(
        &self,
        opt: &GetOption,
        range: R,
        _mark: &Lifetime<'a>,
    ) -> ScanIter<'a, (Bytes, Value)> {
        use std::ops::Bound::*;
        let beg = match range.start_bound() {
            Included(val) => self.keys.lower_bound_by(|entry| entry.key().cmp(val)),
            Excluded(val) => self.keys.upper_bound_by(|entry| entry.key().cmp(val)),
            Unbounded => 0,
        };
        let end = match range.end_bound() {
            Included(val) => self.keys.upper_bound_by(|entry| entry.key().cmp(val)),
            Excluded(val) => self.keys.lower_bound_by(|entry| entry.key().cmp(val)),
            Unbounded => self.keys.len(),
        };
        let keys = self.keys.clone();
        let slice = unsafe { core::mem::transmute::<_, &'static [KvEntry]>(&keys[beg..end]) };
        if let Some(snapshot) = opt.snapshot() {
            let snapshot_ver = snapshot.version();
            ScanIter::new(
                EqualFilter::new(
                    slice
                        .iter()
                        .filter(move |entry| entry.version() <= snapshot_ver),
                )
                .map(kv_entry_ref_to_value),
            )
        } else {
            ScanIter::new(EqualFilter::new(slice.iter()).map(kv_entry_ref_to_value))
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Imemtables {
    pub imemtables: Vec<Arc<Imemtable>>,
}

impl Imemtables {
    pub fn iter(&self) -> impl Iterator<Item = &Arc<Imemtable>> {
        self.imemtables.iter()
    }

    pub fn empty(&self) -> bool {
        self.imemtables.is_empty()
    }

    pub fn push(&self, imemtable: Arc<Imemtable>) -> Self {
        let mut imemtables = self.imemtables.clone();
        imemtables.push(imemtable);

        Self { imemtables }
    }

    pub fn remove(&self, seq: u64) -> Self {
        let mut imemtables = self.imemtables.clone();
        if let Some(idx) = imemtables.iter().enumerate().find(|val| val.1.seq() == seq) {
            imemtables.remove(idx.0);
        }
        Self { imemtables }
    }
}

impl Imemtables {
    pub fn get<'a>(&self, opt: &GetOption, key: Bytes, lifetime: &Lifetime<'a>) -> Option<Value> {
        for table in self.imemtables.iter().rev() {
            if let Some(value) = table.get(opt, key.clone(), lifetime) {
                return Some(value);
            }
        }
        None
    }

    pub fn scan<'a, R: RangeBounds<Bytes> + Clone>(
        &self,
        opt: &GetOption,
        range: R,
        lifetime: &Lifetime<'a>,
    ) -> ScanIter<'a, (Bytes, Value)> {
        let mut iters = Vec::new();
        for table in self.imemtables.iter().rev() {
            iters.push(table.scan(opt, range.clone(), lifetime));
        }

        ScanIter::new(MergedIter::new(iters))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn get_imemtable() {
        let (sorted_input, table, ver) = crate::test::init_itable();

        let opt = GetOption::default();
        let lifetime = Lifetime::default();

        // basic get
        assert_eq!(
            table.get(&opt, "101".into(), &lifetime).unwrap().version(),
            sorted_input[1].1
        );

        // multi-version get
        let v3 = table.get(&opt, "133".into(), &lifetime);
        assert_eq!(v3.unwrap().version(), ver - 3);

        // get snapshot
        let v3 = table.get(
            &GetOption::with_snapshot(sorted_input[33].1 + 1),
            "133".into(),
            &lifetime,
        );
        assert_eq!(v3.unwrap().version(), sorted_input[33].1);
        let v3 = table.get(
            &GetOption::with_snapshot(sorted_input[33].1),
            "133".into(),
            &lifetime,
        );
        assert_eq!(v3.unwrap().version(), sorted_input[33].1);

        // get deleted
        let v4 = table.get(&opt, "144".into(), &lifetime);
        assert!(v4.unwrap().deleted());

        let v4 = table.get(&GetOption::with_snapshot(ver - 2), "144".into(), &lifetime);
        assert_eq!(v4.unwrap().version(), ver - 4);

        let v4 = table.get(&GetOption::with_snapshot(ver - 5), "144".into(), &lifetime);
        assert_eq!(v4.unwrap().version(), sorted_input[44].1);

        // set & delete & set

        let v5 = table.get(&GetOption::with_snapshot(ver - 2), "155".into(), &lifetime);
        assert!(v5.unwrap().deleted());

        // scan tables
        assert_eq!(
            table
                .scan(&opt, Bytes::from("100")..=Bytes::from("110"), &lifetime)
                .count(),
            11
        );

        // full scan
        assert_eq!(table.scan(&opt, .., &lifetime).count(), 200);
        // filter deleted
        assert_eq!(
            table
                .scan(&opt, .., &lifetime)
                .filter(|(_, v)| !v.deleted())
                .count(),
            199
        );

        // scan all
        for (idx, entry) in table.scan(&opt, .., &lifetime).enumerate() {
            assert_eq!(sorted_input[idx].0, entry.0);
        }
    }
}
