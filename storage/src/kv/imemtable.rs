use super::{KVReader, KvEntry, Memtable};
use crate::value::Value;

pub struct Imemtable {
    keys: Vec<KvEntry>,
}

impl From<Memtable> for Imemtable {
    fn from(memtable: Memtable) -> Self {
        let mut keys = Vec::new();
        for item in memtable.into_iter() {
            if item.deleted() {
                continue;
            }
            keys.push(item);
        }

        Self {
            keys,
        }
    }
}

impl Imemtable {
    pub fn min_max(&self) -> Option<(&str, &str)> {
        if self.keys.len() > 0 {
            Some((self.keys.first().unwrap().key(), self.keys.last().unwrap().key()))
        } else {
            None
        }
    }
    pub fn full(&self) -> bool {
        self.keys.len() > 12
    }
    pub fn half_full(&self) -> bool {
        self.keys.len() > 8
    }
}

impl<'a> KVReader<'a> for Imemtable {
    fn get_ver(&self, key: &str, ver: u64) -> Option<Value> {
        let idx = self.keys.binary_search_by(|entry| entry.key().cmp(key)).ok()?;
        while idx < self.keys.len() {
            if self.keys[idx].version() == ver {
                return Some(Value::from_entry(&self.keys[idx]));
            }
        }
        None
    }

    fn get(&self, key: &str) -> Option<Value> {
        self.keys.binary_search_by(|entry| entry.key().cmp(key)).ok().map(|idx| Value::from_entry(&self.keys[idx]))
    }

    fn scan(&'a self, beg: &str, end: &str) -> Box<dyn Iterator<Item = (&'a str, Value)> + 'a> {
        let beg_index = self.keys.partition_point(|entry| entry.key() < beg);
        let end_index = self.keys.partition_point(|entry| entry.key() < end);

        Box::new(self.keys[beg_index..end_index].iter().map(|entry| (entry.key(), Value::from_entry(entry))))
    }

    fn iter(&'a self) -> Box<dyn Iterator<Item = (&'a str, Value)> + 'a> {
        Box::new(self.keys.iter().map(|entry| (entry.key(), Value::from_entry(entry))))
    }
}

#[cfg(test)]
mod test {
    use crate::kv::KVWriter;

    use super::*;
    use rand::{seq::SliceRandom};

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


        let mut table = Memtable::new();
        for key in input {
            let entry = KvEntry::new(key, "abc".into(), None, ver);
            table.set(entry).unwrap();
            ver += 1;
        }
        // remove 0
        sorted_input.remove(0);

        table.set(KvEntry::new_del("0".to_owned(), ver)).unwrap();

        (sorted_input, table.into(), ver)
    }

    #[test]
    pub fn test_imemtable() {
        let (sorted_input, itable, _) = init_table();

        for (idx, (key, _)) in itable.iter().enumerate() {
            assert_eq!(sorted_input[idx], key);
        }
        assert_eq!(itable.iter().count(), 99);
        assert_eq!(
            itable.min_max().unwrap(),
            (
                sorted_input.first().unwrap().as_str(),
                sorted_input.last().unwrap().as_str()
            )
        );
    }
}
