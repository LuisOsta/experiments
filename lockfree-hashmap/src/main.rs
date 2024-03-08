use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hasher;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::{ptr, thread};

/**
 * Things we need to support:
 * * Resizing (to deal with bucket lengths getting too long and which will cause hash collisions)
 * * Garbage collection ( This and the above will require keeping track of writers and readers)
 * * Unique enforcement - Done
 * * Iterable support - Done
 * * Internal mutability support - In Progress
 *
 */

const INITIAL_CAPACITY: usize = 16;

type BucketValue<V> = Arc<RwLock<V>>;
struct AtomicBucketItem<K, V>(AtomicPtr<BucketItem<K, V>>);

#[derive(Debug)]
struct BucketItem<K, V> {
    key: K,
    value: BucketValue<V>,
    next: AtomicPtr<BucketItem<K, V>>,
}

pub struct ExternalBucketItem<K, V> {
    key: K,
    value: BucketValue<V>,
}

pub struct ConcurrentHashMapIter<'a, K, V> {
    current: Option<&'a AtomicPtr<BucketItem<K, V>>>,
}

pub struct ConcurrentHashMap<K, V>
where
    K: Eq + std::hash::Hash + Clone + Debug,
{
    buckets: Vec<AtomicBucketItem<K, V>>,
    capacity: usize,
}

impl<'a, K, V> Iterator for ConcurrentHashMapIter<'a, K, V>
where
    K: Eq + std::hash::Hash + Clone + Debug,
{
    type Item = ExternalBucketItem<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        let current_node = self.current?;
        let node = current_node.load(Ordering::SeqCst);
        if node.is_null() {
            return None;
        }

        let node = unsafe { &*node };
        let key = node.key.clone();
        let value = node.value.clone();
        let item = ExternalBucketItem { key, value };
        self.current = Some(&node.next);

        Some(item)
    }
}

impl<K, V> AtomicBucketItem<K, V> {
    pub fn iter(&self) -> ConcurrentHashMapIter<K, V> {
        let bucket_item = &self.0;
        ConcurrentHashMapIter {
            current: Some(bucket_item),
        }
    }
}

impl<'a, K, V> ConcurrentHashMap<K, V>
where
    K: Eq + std::hash::Hash + Clone + Debug,
{
    pub fn iter(&'a self) -> impl Iterator<Item = ExternalBucketItem<K, V>> + 'a {
        self.buckets.iter().flat_map(|bucket| {
            bucket.iter().map(|item| ExternalBucketItem {
                key: item.key.clone(),
                value: Arc::clone(&item.value),
            })
        })
    }
}

impl<K, V> ConcurrentHashMap<K, V>
where
    K: Eq + std::hash::Hash + Clone + Debug,
    V: Debug,
{
    pub fn new() -> Self {
        let mut buckets = Vec::with_capacity(INITIAL_CAPACITY);
        for _ in 0..INITIAL_CAPACITY {
            buckets.push(AtomicBucketItem(AtomicPtr::new(ptr::null_mut())));
        }

        ConcurrentHashMap {
            buckets,
            capacity: INITIAL_CAPACITY,
        }
    }

    pub fn insert(&self, key: K, value: V) {
        let index = self.get_bucket_slot(&key);
        let new_bucket_item = Box::into_raw(Box::new(BucketItem {
            key: key,
            value: Arc::new(RwLock::new(value)),
            next: AtomicPtr::new(ptr::null_mut()),
        }));

        if self.update_existing_key(new_bucket_item, index).is_err() {
            // no current bucket item has the new_bucket_item key, so add new key via new_bucket_item
            self.insert_new_key(new_bucket_item, index);
        }
    }

    pub fn remove(&self, _key: K) {
        todo!()
    }

    pub fn get_mut(&self, _key: K) -> Option<BucketValue<V>> {
        todo!()
    }

    /**
     * `insert_new_key` assumes that there are no bucket items in the bucket at `index` with the same key as `new_bucket_item`.
     * It will attempt to append to the front of the LinkedList at bucket `index` the bucket item `new_bucket_item` until it succeeds.
     * Utilizes the atomic operation `compare_exchange` to handle the list mutations. If the `compare_exchange` operation fails it will continue to retry until it succeeds
     */
    fn insert_new_key(&self, new_bucket_item: *mut BucketItem<K, V>, index: usize) {
        let mut head = self.buckets[index].0.load(Ordering::SeqCst);
        loop {
            unsafe { (*new_bucket_item).next.store(head, Ordering::SeqCst) };
            match self.buckets[index].0.compare_exchange(
                head,
                new_bucket_item,
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
     * Returns Ok if a bucket item with the same key as new_bucket_item is found, otherwise returns an error if no bucket item with the same key is found.
     *
     * Gets the bucket at `index`, then iterates through the LinkedIn starting at the head of the bucket.
     * Starts by setting a current node to `head` and then iterating to the list, trying to find any bucket item that has the same key as `new_bucket_item`
     * If it finds a bucket item with the same key, it will use `prev` and `current` as needed to replace that bucket item with the `new_bucket_item` pointer.
     * Utilize the atomic operation `compare_exchange` to handle the list mutations. If the `compare_exchange` operation fails it will continue to retry until it succeeds
     */
    fn update_existing_key(
        &self,
        new_bucket_item: *mut BucketItem<K, V>,
        index: usize,
    ) -> Result<(), ()> {
        let key = unsafe { &(*new_bucket_item).key };
        let head = self.buckets[index].0.load(Ordering::SeqCst);
        let mut prev = ptr::null_mut::<BucketItem<K, V>>();
        let mut current = head;

        while !current.is_null() {
            let node = unsafe { &*current };
            let next_node = node.next.load(Ordering::SeqCst);
            if node.key.eq(&key) {
                // Change the next value of new node
                unsafe { (*new_bucket_item).next.store(next_node, Ordering::SeqCst) };

                // If prev is equal to null it means that the key of `head` is the same as the key of new_bucket_item
                if prev.is_null() {
                    match self.buckets[index].0.compare_exchange(
                        head,
                        new_bucket_item,
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
                    // There was an existing BucketItem with a matching key but it was not the first item in the bucket. So it's somewhere within the list.

                    // Set prev.next to new_bucket_item if prev is not null.
                    let raw_prev = unsafe { &*prev };

                    match raw_prev.next.compare_exchange(
                        current,
                        new_bucket_item,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => return Ok(()), // Successfully inserted
                        Err(actual_next) => {
                            // actual_next is what the pointer for the element after the previous bucket item is in reality (`current` was incorrect)
                            // The value in buckets[index] was updated in between the load and compare_exchange and you must restart
                            current = actual_next;
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
    pub fn get(&self, key: &K) -> Option<BucketValue<V>> {
        let index = self.get_bucket_slot(key);
        let mut current = self.buckets[index].0.load(Ordering::SeqCst);

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
    let map_clone = map.clone();
    let thread2 = thread::spawn(move || {
        for i in 0..1000 {
            let key = format!("key{}", i);
            if let Some(value) = map_clone.get(&key) {
                let _v = value.write().unwrap();
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

    let map_two = Arc::new(ConcurrentHashMap::new());

    let key = "test".to_string();
    map_two.insert(key.clone(), "hello".to_string());
    map_two.insert("test2".to_string(), "world".to_string());

    for b in map_two.iter() {
        println!("Node Key: {:#?}. Node Value: {:#?}", b.key, b.value);
        let value = b.value.read().unwrap();
        map_two.insert("test3".to_string(), value.clone());
    }
    println!("\n\n");
    for b in map_two.iter() {
        println!("Node Key: {:#?}. Node Value: {:#?}", b.key, b.value);
    }
}
