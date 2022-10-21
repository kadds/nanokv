use bytes::Bytes;

use super::{GetOption, KVReader, KVWriter, KvEntry, SetResult, WriteOption};
use crate::iterator::Iter;
use crate::util::eq_filter::EqualFilter;
use crate::value::Value;
use crate::KvIterator;

pub struct Memtable {
    list: skiplist::OrderedSkipList<KvEntry>,
    seq: u64,

    total_bytes: u64,
    min_ver: u64,
}

impl Memtable {
    pub fn new(seq: u64) -> Self {
        Self {
            list: skiplist::OrderedSkipList::new(),
            total_bytes: 0,
            seq,
            min_ver: 0,
        }
    }

    pub fn into_iter<'a>(self) -> Box<dyn Iterator<Item = KvEntry>> {
        Box::new(EqualFilter::new(
            self.list.into_iter(),
            |a: &KvEntry, b: &KvEntry| !a.equal_key(b),
        ))
    }

    pub fn full(&self) -> bool {
        self.list.len() >= 1024 * 16 || self.total_bytes > 1024 * 1024 * 10
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }
    pub fn len(&self) -> usize {
        self.list.len()
    }
}

impl<'a> KVReader<'a> for Memtable {
    fn get<K: Into<Bytes>>(&self, opt: &GetOption, key: K) -> Option<Value> {
        let key = key.into();

        use std::ops::Bound::Included;
        let mut iter = self.list.range(
            Included(&KvEntry::from_search(key.to_owned(), u64::MAX)),
            Included(&KvEntry::from_search(key.to_owned(), 0)),
        );
        if let Some(snapshot) = opt.snapshot() {
            // snapshot get
            return iter
                .find(|entry| entry.ver <= snapshot.version())
                .map(Value::from_entry);
        }

        if let Some(entry) = iter.next() {
            return Some(Value::from_entry(entry));
        }
        None
    }

    fn scan<K: Into<Bytes>>(
        &'a self,
        opt: &GetOption,
        beg: K,
        end: K,
    ) -> Box<dyn KvIterator<Item = (Bytes, Value)> + 'a> {
        use std::ops::Bound::*;
        let beg = beg.into();
        let end = end.into();
        let beg_len = beg.len();
        let end_len = end.len();
        let beg_entry = KvEntry::from_search(beg, u64::MAX);
        let end_entry = KvEntry::from_search(end, 0);

        let iter = if beg_len == 0 && end_len == 0 {
            self.list.iter()
        } else {
            self.list.range(
                if beg_len == 0 {
                    Unbounded
                } else {
                    Included(&beg_entry)
                },
                if end_len == 0 {
                    Unbounded
                } else {
                    Included(&end_entry)
                },
            )
        };
        Box::new(Iter::from(
            EqualFilter::new(iter, |a: &&KvEntry, b: &&KvEntry| !a.equal_key(b))
                .map(|entry| (entry.key(), Value::from_entry(entry))),
        ))
    }
}

impl KVWriter for Memtable {
    fn set(&mut self, opt: &WriteOption, entry: KvEntry) -> SetResult<()> {
        if self.list.len() == 0 {
            self.min_ver = entry.version();
        }
        self.total_bytes += entry.bytes();
        self.list.insert(entry);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use rand::seq::SliceRandom;

    use super::*;

    fn load_test_data() -> (Vec<String>, Vec<String>) {
        let mut input: Vec<String> = (0..100).into_iter().map(|k| k.to_string()).collect();
        input.shuffle(&mut rand::thread_rng());
        let mut sorted_input = input.clone();
        sorted_input.sort();
        (input, sorted_input)
    }

    fn init_table() -> (Vec<String>, Memtable, u64) {
        let (input, sorted_input) = load_test_data();
        let mut ver = 0;

        let mut table = Memtable::new(0);
        let opt = WriteOption::default();
        for key in input {
            let entry = KvEntry::new(key, "abc", None, ver);
            table.set(&opt, entry).unwrap();
            ver += 1;
        }
        (sorted_input, table, ver)
    }

    #[test]
    pub fn get_memtable() {
        let (_, mut table, mut ver) = init_table();
        let opt = GetOption::default();
        let sopt = WriteOption::default();

        let v3 = table.get(&opt, "13");
        assert!(table.get(&opt, "1").is_some());
        assert!(v3.is_some());

        assert_eq!(table.scan(&opt, "2", "22").count(), 4); // 2, 20, 21, 22

        // insert snapshot
        table.set(&sopt, KvEntry::new_del("13", ver)).unwrap();
        ver += 1;

        table
            .set(&sopt, KvEntry::new("13", "value", None, ver))
            .unwrap();
        ver += 1;
        // get snapshot

        assert!(table
            .get(&GetOption::with_snapshot(ver - 2), "13")
            .unwrap()
            .deleted()); // deleted key in ver-2

        let v3_new = table.get(&GetOption::with_snapshot(v3.unwrap().version()), "13");
        assert!(v3_new.is_some());
        assert!(!v3_new.unwrap().deleted());

        assert!(table.get(&GetOption::with_snapshot(ver), "13").map(|v| v.version()).unwrap_or_default() == ver - 1);
    }

    #[test]
    pub fn write_memtable() {
        let (sorted_input, table, _) = init_table();
        let opt = GetOption::default();

        assert_eq!(sorted_input.len(), table.scan(&opt, "", "").count());
        // scan all
        for (idx, entry) in table.into_iter().enumerate() {
            assert_eq!(sorted_input[idx], entry.key);
        }
    }

    #[test]
    pub fn del() {
        let (mut sorted_input, mut table, ver) = init_table();
        let opt = WriteOption::default();

        table.set(&opt, KvEntry::new_del("58", ver)).unwrap();

        assert_eq!(
            sorted_input.len(),
            table.scan(&GetOption::default(), "", "").count()
        );

        // scan all
        for (idx, (key, value)) in table.scan(&GetOption::default(), "", "").enumerate() {
            if key == "58" {
                assert!(value.deleted());
            }
            assert_eq!(sorted_input[idx], key);
        }

        sorted_input.remove(sorted_input.iter().position(|r| r == "58").unwrap());

        // range get
        let mut n = 0;
        // 55, 56, 57, 59, 6, 60
        for (idx, (key, _)) in table
            .scan(&GetOption::default(), "55", "60")
            .filter(|(_, v)| !v.deleted())
            .enumerate()
        {
            assert_ne!("58", key);
            assert_eq!(sorted_input[idx + 51], key);
            n += 1;
        }
        assert_eq!(n, 6);
    }
}
