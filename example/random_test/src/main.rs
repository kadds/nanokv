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

    // rng.fill_bytes(&mut vec[..]);
    vec.into()
}

fn is_deleted() -> bool {
    let mut rng = rand::thread_rng();
    rng.gen_bool(0.05)
}

fn main() {
    let config = config::current_config();
    let config = Box::leak(config);

    let mut ins = Instance::new(config);

    let mut deleted_keys = Vec::new();

    let total_test: usize = 1000_00;

    for i in 0..total_test {
        let key = rand_key();
        let value = rand_value();
        if is_deleted() {
            deleted_keys.push(key.to_owned());
        }

        if is_deleted() && deleted_keys.len() > 0 {
            ins.mut_storage()
                .del(deleted_keys.first().unwrap())
                .unwrap();
            deleted_keys.remove(0);
        }

        ins.mut_storage().set(key, value).unwrap();

        let p = i / (total_test / 100);

        if i % (total_test / 100) == 0 {
            println!("finish {}%", p)
        }
    }
}
