use bytes::Bytes;

use super::{GetOption, KVReader, KvEntry, Memtable};
use crate::{iterator::Iter, value::Value, KvIterator};

#[derive(Debug)]
pub struct Imemtable {
    keys: Vec<KvEntry>,
    seq: u64,

    min_ver: u64,
    max_ver: u64,
}

impl From<Memtable> for Imemtable {
    fn from(memtable: Memtable) -> Self {
        let seq = memtable.seq();
        let cap = memtable.len();
        let mut keys = Vec::with_capacity(cap);
        let mut min_ver = u64::MAX;
        let mut max_ver = u64::MIN;
        for item in memtable.into_iter() {
            if item.deleted() {
                continue;
            }
            min_ver = min_ver.min(item.version());
            max_ver = max_ver.max(item.version());
            keys.push(item);
        }

        Self {
            keys,
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
                self.keys.first().unwrap().key().clone(),
                self.keys.last().unwrap().key().clone(),
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

impl<'a> KVReader<'a> for Imemtable {
    fn get<K: Into<Bytes>>(&self, opt: &GetOption, key: K) -> Option<Value> {
        let key = key.into();

        let mut idx = self
            .keys
            .binary_search_by(|entry| entry.key().cmp(&key))
            .ok()?;

        if let Some(snapshot) = &opt.snapshot() {
            while idx < self.keys.len() {
                if self.keys[idx].version() <= snapshot.version() {
                    return Some(Value::from_entry(&self.keys[idx]));
                }
                idx += 1;
            }
        } else {
            return Some(Value::from_entry(&self.keys[idx]));
        }
        None
    }

    fn scan<K: Into<Bytes>>(
        &'a self,
        opt: &GetOption,
        beg: K,
        end: K,
    ) -> Box<dyn KvIterator<Item = (Bytes, Value)> + 'a> {
        let beg = beg.into();
        let end = end.into();
        let beg_index = if beg.len() == 0 {
            0
        } else {
            self.keys.partition_point(|entry| entry.key() < beg)
        };
        let end_index = if end.len() == 0 {
            self.keys.len()
        } else {
            self.keys.partition_point(|entry| entry.key() < end)
        };

        Box::new(Iter::from(
            self.keys[beg_index..end_index]
                .iter()
                .map(|entry| (entry.key(), Value::from_entry(entry))),
        ))
    }
}

#[cfg(test)]
mod test {
    use crate::kv::{KVWriter, WriteOption};

    use super::*;
    use rand::seq::SliceRandom;

    fn load_test_data() -> (Vec<String>, Vec<String>) {
        let mut input: Vec<String> = (0..100).into_iter().map(|k| k.to_string()).collect();
        input.shuffle(&mut rand::thread_rng());
        let mut sorted_input = input.clone();
        sorted_input.sort();
        (input, sorted_input)
    }

    fn init_table() -> (Vec<String>, Imemtable, u64) {
        let (input, mut sorted_input) = load_test_data();
        let mut ver = 0;

        let mut table = Memtable::new(0);
        for key in input {
            let entry = KvEntry::new(key, "abc", None, ver);
            table.set(&WriteOption::default(), entry).unwrap();
            ver += 1;
        }
        // remove 0
        sorted_input.remove(0);

        table
            .set(&WriteOption::default(), KvEntry::new_del("0", ver))
            .unwrap();

        (sorted_input, table.into(), ver)
    }

    #[test]
    pub fn test_imemtable() {
        let (sorted_input, itable, _) = init_table();

        for (idx, (key, _)) in itable.scan(&GetOption::default(), "", "").enumerate() {
            assert_eq!(sorted_input[idx], key);
        }
        assert_eq!(itable.scan(&GetOption::default(), "", "").count(), 99);
        assert_eq!(
            itable.min_max().unwrap(),
            (
                Bytes::from(sorted_input.first().unwrap().clone()),
                Bytes::from(sorted_input.last().unwrap().clone())
            )
        );
    }
}
