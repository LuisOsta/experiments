use std::collections::HashSet;
use std::hash::Hasher;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, Mutex};
use std::{ptr, thread};

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

/**
 * Things we need to support:
 * * Resizing (to deal with bucket lengths getting too long and which will cause hash collisions)
 * * Garbage collection
 * * Unique enforcement
 *
 * All of this will require keeping track of writers and readers
 */
impl<K, V> ConcurrentHashMap<K, V>
where
    K: Eq + std::hash::Hash,
    V: Clone,
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
        let index: usize = hash % self.capacity;
        let new_node = Box::into_raw(Box::new(Node {
            key,
            value,
            next: AtomicPtr::new(ptr::null_mut()),
        }));

        let mut head = self.buckets[index].load(Ordering::SeqCst);
        loop {
            unsafe { (*new_node).next.store(head, Ordering::SeqCst) };

            //new node: k: 3, value 5
            //head: k 2, value 5, next: k: 3, value 10
            //k=3, k=2, k=3
            // How do you enforce uniqueness
            // Keep track of head pointer & iterate through the list to see if your key value is already there
            // If found then compare_exchange that pointer, otherwise compare_exchange head pointer as is
            // If fail to compare exchange, repeat algo
            match self.buckets[index].compare_exchange(
                head,
                new_node,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break, // Successfully inserted
                Err(new_head) => {
                    // The value in buckets[index] was updated in between the load and compare_exchange and you must restart
                    head = new_head;
                    // Collision, retry

                    // Optimization
                    // Only check the distance between the new head and the old head since you've already checked everything after the old head
                    // Only do this if you know there aren't any duplicates in the old head list

                    // Consider simply rejecting duplicate wries (two conflicting writes on the same key)
                    // You'll know cause you've iterated the whole list finding no duplicates and then the new head will be of the same key
                }
            }
        }
    }

    /**
     * We really don't want to return a reference as that's error prone and will make garbage collection much harder in the future
     * This is intentional append-only but we may need to add garbage collection in the future
     */
    pub fn get(&self, key: &K) -> Option<V> {
        let hash = self.hash(&key);
        let index = hash % self.capacity;
        let mut current = self.buckets[index].load(Ordering::SeqCst);

        while !current.is_null() {
            let node = unsafe { &*current };

            if node.key.eq(key) {
                return Some(node.value.clone());
            }

            current = node.next.load(Ordering::SeqCst);
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
    // relacy - concurrency testing
    // Thread pinning may be necessary in order to see true race conditions for test

    // Create a shared ConcurrentHashMap
    let map = Arc::new(ConcurrentHashMap::new());

    // Create a HashSet to store results
    let results: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    // Clone references for threads
    let map_clone = Arc::clone(&map);
    let results_clone = Arc::clone(&results);

    // Thread 1: Inserts values into the map
    let r = results_clone.clone();
    let thread1 = thread::spawn(move || {
        for i in 0..1000 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            map_clone.insert(key.clone(), value);
            r.lock().unwrap().insert(key);
        }
    });

    // Thread 2: Retrieves values from the map
    let r = results_clone.clone();
    let thread2 = thread::spawn(move || {
        for i in 0..1000 {
            let key = format!("key{}", i);
            if let Some(value) = map.get(&key) {
                let v = value.clone();
                r.lock().unwrap().insert(key);
            }
        }
    });

    // Wait for threads to finish
    thread1.join().unwrap();
    thread2.join().unwrap();

    // Check if all keys were processed without deadlocks
    let results = results.lock().unwrap();
    assert_eq!(results.len(), 1000);
    println!("Test passed!");
}
