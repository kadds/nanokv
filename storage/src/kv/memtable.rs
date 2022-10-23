use std::ops::{Bound, RangeBounds};

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use bytes::Bytes;

use super::{GetOption, KvEntry, SetResult, WriteOption};
use crate::iterator::{EqualFilter, IteratorContext, ScanIter};
use crate::kv::kv_entry_ref_to_value;
use crate::value::Value;

#[derive(Debug, Default)]
struct MeltableInner {
    list: skiplist::OrderedSkipList<KvEntry>,
}

impl IteratorContext for MeltableInner {
    fn release(&mut self) {}
}

pub struct Memtable {
    inner: Arc<MeltableInner>,

    seq: u64,

    total_bytes: AtomicU64,
    min_ver: AtomicU64,
}

impl Memtable {
    pub fn new(seq: u64) -> Self {
        Self {
            inner: Arc::new(MeltableInner::default()),
            seq,
            total_bytes: AtomicU64::new(0),
            min_ver: AtomicU64::new(0),
        }
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = &'a KvEntry> {
        unsafe {
            core::mem::transmute::<_, skiplist::ordered_skiplist::Iter<'a, _>>(
                self.inner.clone().list.iter(),
            )
        }
    }

    pub fn full(&self) -> bool {
        self.inner.list.len() >= 1024 * 16
            || self.total_bytes.load(std::sync::atomic::Ordering::Relaxed) > 1024 * 1024 * 10
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }
    pub fn len(&self) -> usize {
        self.inner.list.len()
    }
}

impl Memtable {
    pub fn get(&self, opt: &GetOption, key: Bytes) -> Option<Value> {
        use std::ops::Bound::Included;
        let mut iter = self.inner.list.range(
            Included(&KvEntry::from_search(key.to_owned(), u64::MAX)),
            Included(&KvEntry::from_search(key.to_owned(), 0)),
        );
        if let Some(snapshot) = opt.snapshot() {
            // snapshot get
            return iter
                .filter(|entry| entry.key == key)
                .find(|entry| entry.ver <= snapshot.version())
                .map(Value::from_entry);
        } else if let Some(entry) = iter.next() {
            if entry.key == key {
                return Some(Value::from_entry(entry));
            }
        }

        None
    }

    pub fn scan(
        &self,
        opt: &GetOption,
        range: impl RangeBounds<Bytes> + Clone,
    ) -> ScanIter<(Bytes, Value)> {
        use std::ops::Bound::*;
        let beg = match range.start_bound() {
            Included(val) => Included(KvEntry::from_search(val.to_owned(), u64::MAX)),
            Excluded(val) => Excluded(KvEntry::from_search(val.to_owned(), 0)),
            Unbounded => Unbounded,
        };
        let end = match range.end_bound() {
            Included(val) => Included(KvEntry::from_search(val.clone(), 0)),
            Excluded(val) => Excluded(KvEntry::from_search(val.clone(), u64::MAX)),
            Unbounded => Unbounded,
        };

        let beg = map_bound(&beg);
        let end = map_bound(&end);

        let inner = self.inner.clone();

        let iter = unsafe {
            core::mem::transmute::<_, skiplist::ordered_skiplist::Iter<'static, KvEntry>>(
                inner.list.range(beg, end),
            )
        };
        if let Some(snapshot) = opt.snapshot() {
            let snapshot_ver = snapshot.version();
            let iter = iter.filter(move |entry| entry.version() <= snapshot_ver);
            ScanIter::new(EqualFilter::new(iter).map(kv_entry_ref_to_value)).with(inner)
        } else {
            ScanIter::new(EqualFilter::new(iter).map(kv_entry_ref_to_value)).with(inner)
        }
    }
}

fn map_bound<'a, T>(b: &'a Bound<T>) -> Bound<&'a T> {
    use std::ops::Bound::*;
    unsafe {
        match b {
            Unbounded => Unbounded,
            Included(x) => Included((x as *const T).as_ref().unwrap()),
            Excluded(x) => Excluded((x as *const T).as_ref().unwrap()),
        }
    }
}

impl Memtable {
    pub fn set(&self, _opt: &WriteOption, entry: KvEntry) -> SetResult<()> {
        if self.inner.list.is_empty() {
            self.min_ver
                .store(entry.version(), std::sync::atomic::Ordering::Release);
        }
        self.total_bytes
            .fetch_add(entry.bytes(), std::sync::atomic::Ordering::AcqRel);
        let l = &self.inner.list as *const skiplist::OrderedSkipList<KvEntry>;
        unsafe {
            let l = (l as *mut skiplist::OrderedSkipList<KvEntry>)
                .as_mut()
                .unwrap();
            l.insert(entry);
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn get_memtable() {
        let (sorted_input, table, ver) = crate::test::init_table();

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

    #[test]
    pub fn write_memtable() {
        let (_, table, ver) = crate::test::init_table();
        let opt = GetOption::default();

        table
            .set(&WriteOption::default(), KvEntry::new("0", "1", None, ver))
            .unwrap();

        assert!(table.get(&opt, "0".into()).is_some());
    }
}
