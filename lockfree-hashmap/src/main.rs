mod lockfree_hashmap;

use lockfree_hashmap::ConcurrentHashMap;
use std::collections::HashSet;

use std::sync::{Arc, Mutex};
use std::thread;

fn main() {
    // relacy - concurrency testing
    // Thread pinning may be necessary in order to see true race conditions for test

    // Create a shared ConcurrentHashMap
    let map = Arc::new(ConcurrentHashMap::default());
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

    let map_two = Arc::new(ConcurrentHashMap::default());

    let key = "test".to_string();
    map_two.insert(key.clone(), "hello".to_string());
    map_two.insert("test2".to_string(), "world".to_string());

    for b in map_two.iter() {
        println!("Node Key: {:#?}. Node Value: {:#?}", b.key, b.value);
        let mut value = b.value.write().unwrap();
        value.push('m');
        map_two.insert("test3".to_string(), value.clone());
    }
    println!("\n\n");
    for b in map_two.iter() {
        println!("Node Key: {:#?}. Node Value: {:#?}", b.key, b.value);
    }

    map_two.remove("test2".to_string());

    assert!(map_two.get(&"test2".to_string()).is_none());

    println!("\n\n");
    for b in map_two.iter() {
        println!("Node Key: {:#?}. Node Value: {:#?}", b.key, b.value);
    }
}
