use std::{collections::HashSet, ops::RangeBounds, sync::Arc};

use bytes::Bytes;

use super::{GetOption, KvEntry, Memtable};
use crate::{
    iterator::{EqualFilter, IteratorContext, ScanIter},
    kv::kv_entry_ref_to_value,
    value::Value,
};
use superslice::*;

#[derive(Debug)]
pub struct Imemtable {
    keys: Arc<Vec<KvEntry>>,
    seq: u64,

    min_ver: u64,
    max_ver: u64,
}

impl IteratorContext for Vec<KvEntry> {
    fn release(&mut self) {}
}

impl Imemtable {
    pub fn new(memtable: Memtable, reserved_version: u64) -> Self {
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
            keys: Arc::new(keys),
            seq,
            min_ver,
            max_ver,
        }
    }
}

impl Imemtable {
    pub fn min_max(&self) -> Option<(Bytes, Bytes)> {
        if self.keys.len() > 0 {
            Some((
                self.keys.first().unwrap().key(),
                self.keys.last().unwrap().key(),
            ))
        } else {
            None
        }
    }
    pub fn min_max_ver(&self) -> Option<(u64, u64)> {
        if self.keys.len() > 0 {
            Some((self.min_ver, self.max_ver))
        } else {
            None
        }
    }
    pub fn entry_iter<'a>(&'a self) -> Box<dyn Iterator<Item = &KvEntry> + 'a> {
        Box::new(self.keys.iter())
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }
}

impl Imemtable {
    pub fn get(&self, opt: &GetOption, key: Bytes) -> Option<Value> {
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

    pub fn scan(
        &self,
        opt: &GetOption,
        range: impl RangeBounds<Bytes>,
    ) -> ScanIter<(Bytes, Value)> {
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
            .with(keys)
        } else {
            ScanIter::new(EqualFilter::new(slice.iter()).map(kv_entry_ref_to_value)).with(keys)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn get_imemtable() {
        let (sorted_input, table, ver) = crate::test::init_itable();

        let opt = GetOption::default();

        // basic get
        assert_eq!(
            table.get(&opt, "101".into()).unwrap().version(),
            sorted_input[1].1
        );

        // multi-version get
        let v3 = table.get(&opt, "133".into());
        assert_eq!(v3.unwrap().version(), ver - 3);

        // get snapshot
        let v3 = table.get(
            &GetOption::with_snapshot(sorted_input[33].1 + 1),
            "133".into(),
        );
        assert_eq!(v3.unwrap().version(), sorted_input[33].1);
        let v3 = table.get(&GetOption::with_snapshot(sorted_input[33].1), "133".into());
        assert_eq!(v3.unwrap().version(), sorted_input[33].1);

        // get deleted
        let v4 = table.get(&opt, "144".into());
        assert!(v4.unwrap().deleted());

        let v4 = table.get(&GetOption::with_snapshot(ver - 2), "144".into());
        assert_eq!(v4.unwrap().version(), ver - 4);

        let v4 = table.get(&GetOption::with_snapshot(ver - 5), "144".into());
        assert_eq!(v4.unwrap().version(), sorted_input[44].1);

        // set & delete & set

        let v5 = table.get(&GetOption::with_snapshot(ver - 2), "155".into());
        assert!(v5.unwrap().deleted());

        // scan tables
        assert_eq!(
            table
                .scan(&opt, Bytes::from("100")..=Bytes::from("110"))
                .count(),
            11
        );

        // full scan
        assert_eq!(table.scan(&opt, ..).count(), 200);
        // filter deleted
        assert_eq!(
            table.scan(&opt, ..).filter(|(_, v)| !v.deleted()).count(),
            199
        );

        // scan all
        for (idx, entry) in table.scan(&opt, ..).enumerate() {
            assert_eq!(sorted_input[idx].0, entry.0);
        }
    }
}
