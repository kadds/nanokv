use std::collections::HashSet;

use ::storage::*;
use bytes::Bytes;
use log::info;
use rand::distributions::Alphanumeric;
use rand::{Rng, RngCore};
fn rand_key() -> String {
    let mut rng = rand::thread_rng();
    let len = rng.gen_range(2..15);
    rng.sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn rand_value() -> Bytes {
    let mut rng = rand::thread_rng();
    let len = rng.gen_range(1..8);
    let mut vec = Vec::new();
    vec.resize(len, 0);

    rng.fill_bytes(&mut vec[..]);
    vec.into()
}

fn is_deleted() -> bool {
    let mut rng = rand::thread_rng();
    rng.gen_bool(0.07)
}

fn repeated() -> bool {
    let mut rng = rand::thread_rng();
    rng.gen_bool(0.12)
}

fn main() {
    env_logger::init();
    let mut config = config::current_config();
    config.set_no_wal(true);

    let config = Box::leak(config);

    let mut ins = Instance::new(config);

    let total_test: usize = 100_000;

    let mut exist_keys = HashSet::<String>::new();
    let mut del_keys = HashSet::<String>::new();

    for i in 0..total_test {
        if is_deleted() && !exist_keys.is_empty() {
            let del_key = exist_keys.iter().next().unwrap().clone();

            // make sure del_key exist
            ins.mut_storage()
                .get(&GetOption::default(), del_key.clone())
                .unwrap();

            ins.mut_storage()
                .del(&WriteOption::default(), del_key.clone())
                .unwrap();

            exist_keys.remove(&del_key);

            del_keys.insert(del_key.clone());
        } else {
            let key = loop {
                let key = rand_key();
                if del_keys.get(&key).is_none() && exist_keys.insert(key.clone()) {
                    break key;
                }
            };

            let value = rand_value();

            ins.mut_storage()
                .set(&WriteOption::default(), key.clone(), value.clone())
                .unwrap();

            if repeated() {
                let value = rand_value();
                ins.mut_storage()
                    .set(&WriteOption::default(), key, value)
                    .unwrap();
            }
        }

        let p = i / (total_test / 100);

        if i % (total_test / 100) == 0 {
            info!("finish {}%", p)
        }
    }

    for key in &exist_keys {
        assert!(ins
            .mut_storage()
            .get(&GetOption::default(), key.clone())
            .is_some())
    }

    let iter = ins.mut_storage().scan(&GetOption::default(), ..);

    assert_eq!(
        iter.count(),
        exist_keys.len(),
        "db key count != expected key count"
    );

    let iter = ins.mut_storage().scan(&GetOption::default(), ..);
    let mut v: Vec<String> = exist_keys.into_iter().collect();
    v.sort();

    for (idx, val) in iter.enumerate() {
        let val = unsafe { String::from_utf8_unchecked(val.0.into()) };
        assert_eq!(val, v[idx], "corruption at index {}", idx);
    }
}
