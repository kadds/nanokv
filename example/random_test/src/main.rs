use std::collections::HashSet;
use std::time::Instant;

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
    rng.gen_bool(0.07)
}

fn repeated() -> bool {
    let mut rng = rand::thread_rng();
    rng.gen_bool(0.12)
}

struct TestCase<F> {
    r: Option<F>,
    ops: u64,
    name: String,
}

impl<F, R> TestCase<F>
where
    F: FnOnce() -> R,
{
    pub fn new(f: F, name: String, ops: u64) -> Self {
        Self {
            r: Some(f),
            name,
            ops,
        }
    }
    pub fn run(&mut self) -> R {
        println!("{}", self.name);
        let beg = Instant::now();
        let f = self.r.take();
        let r = (f.unwrap())();
        let end = Instant::now();
        let cost = end - beg;
        println!(
            "{} {:.2}ns/op, tps {:.1}/s",
            self.name,
            cost.as_nanos() as f64 / self.ops as f64,
            self.ops as f64 / (cost.as_nanos() as f64 / 1000_000_000f64)
        );
        r
    }
}

fn main() {
    env_logger::init();
    let mut config = config::current_config();
    config.set_no_wal(true);

    let config = Box::leak(config);

    // Instance::clean(config);
    let mut ins = Instance::new(config);

    let total_test: usize = 100_000;

    let mut exist_keys = HashSet::<String>::new();
    let mut del_keys = HashSet::<String>::new();

    let mut case = TestCase::new(
        || {
            for i in 0..total_test {
                if is_deleted() && !exist_keys.is_empty() {
                    let del_key = exist_keys.iter().next().unwrap().clone();

                    // make sure del_key exist
                    if ins
                        .mut_storage()
                        .get(&GetOption::default(), del_key.clone())
                        .is_none()
                    {
                        panic!("not found key {}", del_key);
                    }

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
            }
        },
        "insert keys".to_owned(),
        total_test as u64,
    );
    case.run();
    drop(case);

    let mut case = TestCase::new(
        || {
            for key in &exist_keys {
                if ins
                    .mut_storage()
                    .get(&GetOption::default(), key.clone())
                    .is_none()
                {
                    let _ = ins.mut_storage().get(&GetOption::with_debug(), key.clone());

                    assert!(false, "{} should be existed", key.clone(),)
                }
            }
        },
        "check exist keys".to_owned(),
        exist_keys.len() as u64,
    );
    case.run();
    drop(case);

    let mut case = TestCase::new(
        || {
            for key in &del_keys {
                if ins
                    .mut_storage()
                    .get(&GetOption::default(), key.clone())
                    .is_some()
                {
                    let value = ins
                        .mut_storage()
                        .get(&GetOption::with_debug(), key.clone())
                        .unwrap();

                    assert!(
                        false,
                        "{} ver {} should be deleted",
                        key.clone(),
                        value.version()
                    )
                }
            }
        },
        "check del keys".to_owned(),
        del_keys.len() as u64,
    );
    case.run();
    drop(case);

    let keys = exist_keys.len() as u64;

    let su_version = ins.storage().super_version();

    let mut case = TestCase::new(
        || {
            let iter = ins
                .mut_storage()
                .scan(&GetOption::default(), .., &su_version);

            assert_eq!(
                iter.count(),
                exist_keys.len(),
                "db key count != expected key count"
            );
        },
        "check scan keys".to_owned(),
        keys,
    );
    case.run();
    drop(case);

    let mut case = TestCase::new(
        || {
            let su_version = ins.storage().super_version();
            let iter = ins
                .mut_storage()
                .scan(&GetOption::default(), .., &su_version);
            let mut v: Vec<String> = exist_keys.into_iter().collect();
            v.sort();

            for (idx, val) in iter.enumerate() {
                let val = unsafe { String::from_utf8_unchecked(val.0.into()) };
                assert_eq!(val, v[idx], "corruption at index {}", idx);
            }
        },
        "scan validation".to_owned(),
        keys,
    );
    case.run();
    drop(case);
}
