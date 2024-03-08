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
    K: Eq + std::hash::Hash + Clone,
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
        let index = self.get_bucket_slot(&key);
        let new_node = Box::into_raw(Box::new(Node {
            key: key.clone(),
            value,
            next: AtomicPtr::new(ptr::null_mut()),
        }));

        if self.update_existing_key(new_node, index).is_err() {
            self.insert_new_key(new_node, index);
        }
    }

    /**
     *
     */
    fn insert_new_key(&self, new_node: *mut Node<K, V>, index: usize) {
        let mut head = self.buckets[index].load(Ordering::SeqCst);
        loop {
            //new node: k: 3, value 5
            //head: k 2, value 5, next: k: 3, value 10
            //k=3, k=2, k=3

            // In the case that the key value does not already exist in the linkedlist in the bucket at `index`.
            // Then add the new_node to the front of the linkedlist and update the pointer at buckets[index]
            unsafe { (*new_node).next.store(head, Ordering::SeqCst) };
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

    // How do you enforce uniqueness
    // Keep track of head pointer & iterate through the list to see if your key value is already there
    // If found then compare_exchange that pointer, otherwise compare_exchange head pointer as is
    // If fail to compare exchange, repeat algo
    /**
     *
     */
    fn update_existing_key(&self, new_node: *mut Node<K, V>, index: usize) -> Result<(), ()> {
        let key = unsafe { &(*new_node).key };
        let head = self.buckets[index].load(Ordering::SeqCst);
        let mut prev = ptr::null_mut::<Node<K, V>>();
        let mut current = head;

        while !current.is_null() {
            let node = unsafe { &*current };
            let next_node = node.next.load(Ordering::SeqCst);
            if node.key.eq(&key) {
                // Change the next value of new node
                unsafe { (*new_node).next.store(next_node, Ordering::SeqCst) };

                // If prev is equal to null it means that the key of `head` is the same as the key of new_node
                if prev.is_null() {
                    match self.buckets[index].compare_exchange(
                        head,
                        new_node,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => return Ok(()), // Successfully inserted
                        Err(new_head) => {
                            // The value in buckets[index] was updated in between the load and compare_exchange and you must restart
                            current = new_head;
                            continue;
                        }
                    }
                } else {
                    // Set prev.next to new_node if prev is not null.
                    let prev_node = unsafe { &*prev };

                    match prev_node.next.compare_exchange(
                        current,
                        new_node,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => return Ok(()), // Successfully inserted
                        Err(new_head) => {
                            // The value in buckets[index] was updated in between the load and compare_exchange and you must restart
                            current = new_head;
                            continue;
                        }
                    }
                }
            }

            prev = current;
            current = node.next.load(Ordering::SeqCst);
        }
        Err(())
    }

    /**
     * We really don't want to return a reference as that's error prone and will make garbage collection much harder in the future
     * This is intentional append-only but we may need to add garbage collection in the future
     */
    pub fn get(&self, key: &K) -> Option<V> {
        let index = self.get_bucket_slot(key);
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

    fn get_bucket_slot(&self, key: &K) -> usize {
        let hash = self.hash(&key);
        return hash % self.capacity;
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
                let _v = value.clone();
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
