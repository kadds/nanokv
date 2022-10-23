use std::collections::{HashMap, HashSet};

use ::storage::*;
use bytes::Bytes;
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
    rng.gen_bool(0.1)
}

fn main() {
    env_logger::init();
    let config = config::current_config();
    let config = Box::leak(config);

    let mut ins = Instance::new(config);

    let total_test: usize = 1_000_00;

    let mut exist_key = HashSet::<String>::new();

    for i in 0..total_test {
        if is_deleted() && exist_key.len() > 0 {
            let del_key = exist_key.iter().next().unwrap().clone();

            // make sure del_key exist
            ins.mut_storage()
                .get(&GetOption::default(), del_key.clone())
                .unwrap();

            ins.mut_storage()
                .del(&WriteOption::default(), del_key.clone())
                .unwrap();

            exist_key.remove(&del_key);
        } else {
            let key = loop {
                let key = rand_key();
                if exist_key.insert(key.clone()) {
                    break key;
                }
            };

            let value = rand_value();

            ins.mut_storage()
                .set(&WriteOption::default(), key, value)
                .unwrap();
        }

        let p = i / (total_test / 100);

        if i % (total_test / 100) == 0 {
            println!("finish {}%", p)
        }
    }

    let iter = ins.mut_storage().scan(&GetOption::default(), ..);

    println!(
        "scan all keys in db {} should be {}",
        iter.count(),
        exist_key.len()
    );
}
