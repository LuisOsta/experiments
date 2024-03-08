use std::hash::Hasher;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

const INITIAL_CAPACITY: usize = 16;

struct Node<K, V> {
    key: K,
    value: V,
    next: AtomicPtr<Node<K, V>>,
}

pub struct ConcurrentHashMap<K, V> {
    buckets: Vec<AtomicPtr<Node<K, V>>>,
    capacity: usize,
}

impl<K, V> ConcurrentHashMap<K, V>
where
    K: Eq + std::hash::Hash,
{
    pub fn new() -> Self {
        let mut buckets = Vec::with_capacity(INITIAL_CAPACITY);
        for _ in 0..INITIAL_CAPACITY {
            buckets.push(AtomicPtr::new(ptr::null_mut()));
        }

        ConcurrentHashMap {
            buckets,
            capacity: INITIAL_CAPACITY,
        }
    }

    pub fn insert(&self, key: K, value: V) {
        let hash = self.hash(&key);
        let new_node = Box::into_raw(Box::new(Node {
            key,
            value,
            next: AtomicPtr::new(ptr::null_mut()),
        }));

        loop {
            let mut head = self.buckets[hash % self.capacity].load(Ordering::Acquire);
            unsafe { (*new_node).next.store(head, Ordering::Relaxed) };

            match self.buckets[hash % self.capacity].compare_exchange(
                head,
                new_node,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break, // Successfully inserted
                Err(actual) => {
                    head = actual;
                    // Collision, retry
                }
            }
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let hash = self.hash(&key);
        let mut current = self.buckets[hash % self.capacity].load(Ordering::Acquire);

        while !current.is_null() {
            let node = unsafe { &*current };

            if node.key == *key {
                return Some(&node.value);
            }

            current = node.next.load(Ordering::Acquire);
        }

        None
    }

    fn hash(&self, key: &K) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize
    }
}

fn main() {
    let map = ConcurrentHashMap::new();

    map.insert("key1", "value1");
    map.insert("key2", "value2");

    println!("{:?}", map.get(&"key1")); // Some("value1")
    println!("{:?}", map.get(&"key3")); // None
}
