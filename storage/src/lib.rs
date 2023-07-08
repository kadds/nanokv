pub mod backend;
mod cache;
pub mod compaction;
pub mod config;
pub mod err;
pub mod iterator;
pub mod key;
pub mod kv;
pub mod log;
pub mod option;
pub mod snapshot;
pub mod storage;
pub mod util;

pub use crate::storage::Storage;
pub use config::Config;
pub use config::ConfigRef;

pub use iterator::KvIterator;
pub use option::GetOption;
pub use option::WriteOption;

mod test {
    use rand::seq::SliceRandom;

    use crate::{
        key::{InternalKey, KeyType},
        kv::Memtable,
        WriteOption,
    };

    pub fn load_test_data() -> Vec<String> {
        let mut input: Vec<String> = (100..300).into_iter().map(|k| k.to_string()).collect();
        input.shuffle(&mut rand::thread_rng());

        input
    }

    #[allow(unused)]
    pub fn init_table() -> (Vec<(String, u64)>, Memtable, u64) {
        let input = load_test_data();
        let mut ver = 0;

        let table = Memtable::new(0);
        let opt = WriteOption::default();
        let mut sorted_input = Vec::new();

        for key in input {
            let internal_key = InternalKey::new(key.clone(), ver, KeyType::Set);
            let value = key.to_string();
            table.set(internal_key, value).unwrap();

            sorted_input.push((key, ver));
            ver += 1;
        }

        table
            .set(InternalKey::new("144", ver, KeyType::Del), "")
            .unwrap();
        ver += 1;

        table
            .set(InternalKey::new("133", ver, KeyType::Set), "")
            .unwrap();
        ver += 1;

        table
            .set(InternalKey::new("155", ver, KeyType::Del), "")
            .unwrap();
        ver += 1;

        table
            .set(InternalKey::new("155", ver, KeyType::Set), "")
            .unwrap();
        ver += 1;

        sorted_input.sort();

        (sorted_input, table, ver)
    }

    #[allow(unused)]
    pub fn init_itable() -> (Vec<(String, u64)>, Memtable, u64) {
        let (sorted_input, table, ver) = crate::test::init_table();
        (sorted_input, table, ver)
    }
}
