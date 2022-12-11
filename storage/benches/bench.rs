use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::{distributions::Alphanumeric, Rng, RngCore};
use storage::{config, Config, Instance, WriteOption};

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

fn kv_read(c: &mut Criterion) {}

fn kv_write(c: &mut Criterion) {
    c.bench_function("write", |b| {
        let config = config::test_config();
        let config = Box::leak(config);
        let ins = Instance::new(config);

        b.iter(|| {
            let key = rand_key();
            let value = rand_value();
            ins.storage()
                .set(&WriteOption::default(), key, value)
                .unwrap();
        });
    });
}

criterion_group!(benches, kv_read, kv_write);
criterion_main!(benches);
