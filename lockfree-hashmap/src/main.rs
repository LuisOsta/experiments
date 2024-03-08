use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use rand::Rng;

const NUM_BUCKETS: usize = 100;

struct BucketItem<K, V> {
    key: K,
    value: Arc<V>,
}
#[derive(Clone)]
pub struct LockFreeConcurrentHashMap<K, V>
where
    K: ToString + Eq + PartialEq,
{
    buckets: Vec<Arc<RwLock<Vec<BucketItem<K, V>>>>>,
}

impl<K, V> LockFreeConcurrentHashMap<K, V>
where
    K: ToString + Eq + PartialEq,
{
    pub fn new() -> Self {
        let mut buckets = Vec::with_capacity(NUM_BUCKETS);

        for _ in 0..NUM_BUCKETS {
            buckets.push(Arc::new(RwLock::new(Vec::new())));
        }

        Self { buckets }
    }
    fn get_bucket_slot(&self, key: &K) -> usize {
        format!("{:?}", key.to_string()).len() % NUM_BUCKETS
    }

    // Insert a key-value pair into the map
    // No way to making sure there aren't duplicate keys
    fn insert(&self, key: K, value: V) {
        let hash = self.get_bucket_slot(&key);
        let mut bucket = self.buckets[hash].write().unwrap();
        bucket.push(BucketItem {
            key,
            value: Arc::new(value),
        });
    }

    // Get the value associated with a key
    fn get(&self, key: &K) -> Option<Arc<V>> {
        let hash = self.get_bucket_slot(key);
        let bucket = self.buckets[hash].read().unwrap();
        bucket
            .iter()
            .find(|bucket| &bucket.key == key)
            .map(|b| b.value.clone())
    }
}

fn main() {
    let concurrent_map = LockFreeConcurrentHashMap::new();
    // Create a random number generator

    // Generate a random integer in the range [0, 100)
    // Spawn multiple threads to insert values concurrently
    let max_key = 10;
    let map_ref = concurrent_map.clone();

    let read_handle = thread::spawn(move || {
        // Print the contents of the map
        let mut rng = rand::thread_rng();

        loop {
            let random_key = rng.gen_range(0..max_key);

            if let Some(value) = map_ref.get(&random_key) {
                println!("Key: {}, Value: {}", random_key, value);
            } else {
                println!("Did not find value for key {}", random_key);
            }
            thread::sleep(Duration::from_millis(100));
        }
    });

    let insert_handles: Vec<_> = (0..max_key)
        .map(|i| {
            let map_ref = concurrent_map.clone();
            thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let random_key = rng.gen_range(0..1000);

                thread::sleep(Duration::from_millis(random_key));
                map_ref.insert(i, i * i);
                println!("Inserted key: {}", i);
            })
        })
        .collect();

    // Wait for all threads to finish
    for handle in insert_handles {
        handle.join().unwrap();
    }

    read_handle.join().unwrap();
}
