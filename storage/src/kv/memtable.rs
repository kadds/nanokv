use std::io::Write;
use std::ops::{Bound, RangeBounds};

use std::sync::atomic::AtomicU64;

use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::superversion::Lifetime;
use super::GetOption;
use crate::err::{Result, StorageError};
use crate::iterator::{EqualFilter, KvIteratorItem, ScanIter};
use crate::key::{InternalKey, KeyType, Value, WriteBatch};

#[derive(Debug, Eq, Ord, Clone)]
struct LookupKeyValue {
    bytes: Bytes,
}

impl<'a> KvIteratorItem for &'a LookupKeyValue {
    fn user_key_slice(&self) -> &[u8] {
        unsafe { std::mem::transmute(self.internal_key().user_key_slice()) }
    }

    fn user_key(&self) -> Bytes {
        self.internal_key().user_key()
    }

    fn seq(&self) -> u64 {
        self.internal_key().seq()
    }

    fn deleted(&self) -> bool {
        self.internal_key().key_type() == KeyType::Del
    }
}

impl LookupKeyValue {
    pub fn new(key: InternalKey, value: &[u8]) -> Self {
        let mut bytes = BytesMut::default().writer();
        bytes.write_u32::<LE>(key.data().len() as u32);
        bytes.write(key.data());
        bytes.write(value).unwrap();
        Self {
            bytes: bytes.into_inner().freeze(),
        }
    }

    pub fn new_lookup(key: &[u8], seq: u64) -> Self {
        let mut bytes = BytesMut::default().writer();
        bytes.write_u32::<LE>((key.len() + 8) as u32);
        bytes.write(key);
        bytes.write_u64::<LE>(seq & 0xFFFF_FFFF_FFFF);
        Self {
            bytes: bytes.into_inner().freeze(),
        }
    }

    pub fn internal_key(&self) -> InternalKey {
        let len = self.bytes.clone().reader().read_u32::<LE>().unwrap() + 4;

        InternalKey::from(self.bytes.slice(4..len as usize))
    }

    pub fn user_key(&self) -> Bytes {
        self.internal_key().user_key()
    }

    pub fn seq(&self) -> u64 {
        self.internal_key().seq()
    }

    pub fn value(&self) -> Bytes {
        let len = self.bytes.clone().reader().read_u32::<LE>().unwrap() + 4;
        self.bytes.slice(len as usize..)
    }
}

impl PartialEq for LookupKeyValue {
    fn eq(&self, other: &Self) -> bool {
        self.internal_key() == other.internal_key()
    }
}

impl PartialOrd for LookupKeyValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.internal_key().partial_cmp(&other.internal_key())
    }
}

#[derive(Debug)]
pub struct Memtable {
    list: skiplist::OrderedSkipList<LookupKeyValue>,

    total_bytes: AtomicU64,
    min_seq: AtomicU64,
    max_seq: AtomicU64,
    number: u64,
}

impl Memtable {
    pub fn new(number: u64) -> Self {
        Self {
            list: skiplist::OrderedSkipList::new(),
            max_seq: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            min_seq: AtomicU64::new(0),
            number,
        }
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = &'a (InternalKey, Bytes)> {
        unsafe {
            core::mem::transmute::<_, skiplist::ordered_skiplist::Iter<'a, _>>(
                self.list.iter().map(|v| (v.internal_key(), v.value())),
            )
        }
    }

    pub fn full(&self) -> bool {
        self.list.len() >= 1024 * 16
            || self.total_bytes.load(std::sync::atomic::Ordering::Relaxed) > 1024 * 1024 * 10
    }

    pub fn max_seq(&self) -> u64 {
        self.max_seq.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn min_seq(&self) -> u64 {
        self.min_seq.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn number(&self) -> u64 {
        self.number
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }

    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    pub fn first_key(&self) -> InternalKey {
        self.list.front().unwrap().internal_key()
    }
    pub fn last_key(&self) -> InternalKey {
        self.list.back().unwrap().internal_key()
    }
}

impl Memtable {
    pub fn get<'a>(
        &self,
        opt: &GetOption,
        key: Bytes,
        _lifetime: &Lifetime<'a>,
    ) -> Result<(InternalKey, Value)> {
        use std::ops::Bound::Included;
        let mut iter = self.list.range(
            Included(&LookupKeyValue::new_lookup(&key, u64::MAX)),
            Included(&LookupKeyValue::new_lookup(&key, 0)),
        );
        if let Some(snapshot) = opt.snapshot() {
            // snapshot get
            if let Some(entry) = iter.find(|entry| entry.seq() <= snapshot.sequence()) {
                return Ok((entry.internal_key(), entry.value().into()));
            }
        } else if let Some(entry) = iter.next() {
            return Ok((entry.internal_key(), entry.value().into()));
        }

        Err(StorageError::KeyNotExist)
    }

    pub fn scan<'a, R: RangeBounds<Bytes> + Clone>(
        &self,
        opt: &GetOption,
        range: R,
        _lifetime: &Lifetime<'a>, // lifetime parameter
    ) -> ScanIter<'a, (InternalKey, Value)> {
        use std::ops::Bound::*;
        let beg = match range.start_bound() {
            Included(val) => Included(LookupKeyValue::new_lookup(val, u64::MAX)),
            Excluded(val) => Excluded(LookupKeyValue::new_lookup(val, 0)),
            Unbounded => Unbounded,
        };
        let end = match range.end_bound() {
            Included(val) => Included(LookupKeyValue::new_lookup(val, 0)),
            Excluded(val) => Excluded(LookupKeyValue::new_lookup(val, u64::MAX)),
            Unbounded => Unbounded,
        };

        let beg = map_bound(&beg);
        let end = map_bound(&end);

        let iter = unsafe {
            core::mem::transmute::<_, skiplist::ordered_skiplist::Iter<'static, LookupKeyValue>>(
                self.list.range(beg, end),
            )
        };
        if let Some(snapshot) = opt.snapshot() {
            let snapshot_seq = snapshot.sequence();
            let iter = iter.filter(move |entry| entry.seq() <= snapshot_seq);
            ScanIter::new(EqualFilter::new(iter).map(|v| (v.internal_key(), v.value().into())))
        } else {
            ScanIter::new(EqualFilter::new(iter).map(|v| (v.internal_key(), v.value().into())))
        }
    }
}

fn map_bound<T>(b: &Bound<T>) -> Bound<&T> {
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
    pub fn set<B: AsRef<[u8]>>(&self, key: InternalKey, value: B) -> Result<()> {
        let value = value.as_ref();
        let value_len = value.len();
        if self.list.is_empty() {
            self.min_seq
                .store(key.seq(), std::sync::atomic::Ordering::Release);
        }
        let kv = LookupKeyValue::new(key, value);

        self.total_bytes
            .fetch_add(value_len as u64, std::sync::atomic::Ordering::AcqRel);
        let l = &self.list as *const skiplist::OrderedSkipList<LookupKeyValue>;
        unsafe {
            let l = (l as *mut skiplist::OrderedSkipList<LookupKeyValue>)
                .as_mut()
                .unwrap();
            l.insert(kv);
        }

        Ok(())
    }

    pub fn set_batch(&self, b: WriteBatch) -> Result<()> {
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
        let lifetime = Lifetime::default();

        // basic get
        assert_eq!(
            table.get(&opt, "101".into(), &lifetime).unwrap().seq(),
            sorted_input[1].1
        );

        // multi-version get
        let v3 = table.get(&opt, "133".into(), &lifetime);
        assert_eq!(v3.unwrap().seq(), ver - 3);

        // get snapshot
        let v3 = table.get(
            &GetOption::with_snapshot(sorted_input[33].1 + 1),
            "133".into(),
            &lifetime,
        );
        assert_eq!(v3.unwrap().seq(), sorted_input[33].1);
        let v3 = table.get(
            &GetOption::with_snapshot(sorted_input[33].1),
            "133".into(),
            &lifetime,
        );
        assert_eq!(v3.unwrap().seq(), sorted_input[33].1);

        // get deleted
        let v4 = table.get(&opt, "144".into(), &lifetime);
        assert!(v4.unwrap().deleted());

        let v4 = table.get(&GetOption::with_snapshot(ver - 2), "144".into(), &lifetime);
        assert_eq!(v4.unwrap().seq(), ver - 4);

        let v4 = table.get(&GetOption::with_snapshot(ver - 5), "144".into(), &lifetime);
        assert_eq!(v4.unwrap().seq(), sorted_input[44].1);

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
                .filter(|v| !v.deleted())
                .count(),
            199
        );

        // scan all
        for (idx, entry) in table.scan(&opt, .., &lifetime).enumerate() {
            assert_eq!(sorted_input[idx].0, entry.0.user_key());
        }
    }

    #[test]
    pub fn write_memtable() {
        let (_, table, ver) = crate::test::init_table();
        let opt = GetOption::default();

        table
            .set(InternalKey::new("0", ver, crate::key::KeyType::Set), "1")
            .unwrap();

        let lifetime = Lifetime::default();
        assert!(table.get(&opt, "0".into(), &lifetime).is_ok());
    }
}
