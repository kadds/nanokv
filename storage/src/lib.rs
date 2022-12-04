mod cache;
pub mod compaction;
pub mod config;
pub mod instance;
pub mod iterator;
pub mod kv;
pub mod log;
pub mod option;
pub mod snapshot;
pub mod storage;
pub mod util;

pub mod value;
pub use config::Config;
pub use config::ConfigRef;
pub use instance::Instance;
pub use storage::Storage;

pub use iterator::KvIterator;
pub use option::GetOption;
pub use option::WriteOption;
pub use value::Value;

mod test {
    use rand::seq::SliceRandom;

    use crate::{
        kv::{Imemtable, KvEntry, Memtable},
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
            let entry = KvEntry::new(key.clone(), "", None, ver);
            table.set(&opt, entry).unwrap();
            sorted_input.push((key, ver));
            ver += 1;
        }

        table.set(&opt, KvEntry::new_del("144", ver)).unwrap();
        ver += 1;

        table.set(&opt, KvEntry::new("133", "", None, ver)).unwrap();
        ver += 1;

        table.set(&opt, KvEntry::new_del("155", ver)).unwrap();
        ver += 1;

        table.set(&opt, KvEntry::new("155", "", None, ver)).unwrap();
        ver += 1;

        sorted_input.sort();

        (sorted_input, table, ver)
    }

    #[allow(unused)]
    pub fn init_itable() -> (Vec<(String, u64)>, Imemtable, u64) {
        let (sorted_input, table, ver) = crate::test::init_table();
        (sorted_input, Imemtable::new(&table, 0), ver)
    }
}
