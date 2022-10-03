use super::{KVReader, KVWriter, KvEntry, SetResult};
use crate::util::eq_filter::EqualFilter;
use crate::value::Value;

pub struct Memtable {
    list: skiplist::OrderedSkipList<KvEntry>,
    total_bytes: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            list: skiplist::OrderedSkipList::new(),
            total_bytes: 0,
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
}

impl<'a> KVReader<'a> for Memtable {
    fn get_ver(&self, key: &str, ver: u64) -> Option<Value> {
        use std::ops::Bound::Included;
        let mut iter = self.list.range(
            Included(&KvEntry::from_search(key.to_owned(), u64::MAX)),
            Included(&KvEntry::from_search(key.to_owned(), 0)),
        );
        iter.find(|entry| entry.ver == ver).map(Value::from_entry)
    }

    fn get(&self, key: &str) -> Option<Value> {
        use std::ops::Bound::Included;
        let mut iter = self.list.range(
            Included(&KvEntry::from_search(key.to_owned(), u64::MAX)),
            Included(&KvEntry::from_search(key.to_owned(), 0)),
        );
        if let Some(entry) = iter.next() {
            return Some(Value::from_entry(entry));
        }
        return None;
    }

    fn scan(&'a self, beg: &str, end: &str) -> Box<dyn Iterator<Item = (&'a str, Value)> + 'a> {
        use std::ops::Bound::Included;
        let iter = self.list.range(
            Included(&KvEntry::from_search(beg.to_owned(), u64::MAX)),
            Included(&KvEntry::from_search(end.to_owned(), 0)),
        );
        Box::new(
            EqualFilter::new(iter, |a: &&KvEntry, b: &&KvEntry| !a.equal_key(b))
                .map(|entry| (entry.key(), Value::from_entry(entry))),
        )
    }

    fn iter(&'a self) -> Box<dyn Iterator<Item = (&'a str, Value)> + 'a> {
        Box::new(
            EqualFilter::new(self.list.iter(), |a: &&KvEntry, b: &&KvEntry| {
                !a.equal_key(b)
            })
            .map(|entry| (entry.key(), Value::from_entry(entry))),
        )
    }
}

impl KVWriter for Memtable {
    fn set(&mut self, entry: KvEntry) -> SetResult<()> {
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

        let mut table = Memtable::new();
        for key in input {
            let entry = KvEntry::new(key, "abc".into(), None, ver);
            table.set(entry).unwrap();
            ver += 1;
        }
        (sorted_input, table, ver)
    }

    #[test]
    pub fn get_memtable() {
        let (_, mut table, mut ver) = init_table();

        let v3 = table.get("13");
        assert!(table.get("1").is_some());
        assert!(v3.is_some());

        assert_eq!(table.scan("2", "22").count(), 4); // 2, 20, 21, 22

        // insert snapshot
        table.set(KvEntry::new_del("13".to_owned(), ver)).unwrap();
        ver += 1;

        table
            .set(KvEntry::new("13".to_owned(), "value".into(), None, ver))
            .unwrap();
        ver += 1;
        // get snapshot

        assert!(table.get_ver("13", ver - 2).unwrap().deleted()); // deleted key in ver-2

        let v3_new = table.get_ver("13", v3.unwrap().version());
        assert!(v3_new.is_some());
        assert!(!v3_new.unwrap().deleted());

        assert!(table.get_ver("13", ver).is_none());
    }

    #[test]
    pub fn write_memtable() {
        let (sorted_input, table, _) = init_table();

        assert_eq!(sorted_input.len(), table.iter().count());
        // scan all
        for (idx, entry) in table.into_iter().enumerate() {
            assert_eq!(sorted_input[idx], entry.key);
        }
    }

    #[test]
    pub fn del() {
        let (mut sorted_input, mut table, ver) = init_table();

        table.set(KvEntry::new_del("58".to_owned(), ver)).unwrap();

        assert_eq!(sorted_input.len(), table.iter().count());

        // scan all
        for (idx, (key, value)) in table.iter().enumerate() {
            if key == "58" {
                assert!(value.deleted());
            }
            assert_eq!(sorted_input[idx], key);
        }

        sorted_input.remove(sorted_input.iter().position(|r| r == "58").unwrap());

        // range get
        let mut n = 0;
        // 55, 56, 57, 59, 6, 60
        for (idx, (key, _)) in table.scan("55", "60").filter(|(_, v)| !v.deleted() ).enumerate() {
            assert_ne!("58", key);
            assert_eq!(sorted_input[idx + 51], key);
            n += 1;
        }
        assert_eq!(n, 6);
    }
}
