use std::fmt::Debug;
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicPtr, Arc, RwLock};

pub type BucketValue<V> = Arc<RwLock<V>>;
pub struct AtomicBucketItem<K, V>(pub AtomicPtr<BucketItem<K, V>>);

#[derive(Debug)]
pub struct BucketItem<K, V> {
    pub key: K,
    pub value: BucketValue<V>,
    pub next: AtomicPtr<BucketItem<K, V>>,
}

pub struct ExternalBucketItem<K, V> {
    pub key: K,
    pub value: BucketValue<V>,
}

pub struct ConcurrentHashMapIter<'a, K, V> {
    pub current: Option<&'a AtomicPtr<BucketItem<K, V>>>,
}

impl<'a, K, V> Iterator for ConcurrentHashMapIter<'a, K, V>
where
    K: Clone,
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
