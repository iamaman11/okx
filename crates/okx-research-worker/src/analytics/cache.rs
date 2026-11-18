use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceKey {
    pub source: String,
    pub instrument: String,
    pub timeframe_ms: u64,
    pub start_ms: u64,
    pub end_ms: u64,
    pub closed_only: bool,
    pub schema: u32,
}
struct Entry {
    value: Arc<Vec<okx_research::strategy_registry::Bar>>,
    bytes: usize,
}
#[derive(Clone)]
pub struct SourceCache {
    entries: Arc<Mutex<BTreeMap<SourceKey, Entry>>>,
    locks: Arc<Mutex<BTreeMap<SourceKey, Weak<AsyncMutex<()>>>>>,
    global: Arc<Semaphore>,
}
impl Default for SourceCache {
    fn default() -> Self {
        Self {
            entries: Default::default(),
            locks: Default::default(),
            global: Arc::new(Semaphore::new(8)),
        }
    }
}
impl SourceCache {
    pub async fn load<F, Fut>(
        &self,
        key: SourceKey,
        fetch: F,
    ) -> Result<Arc<Vec<okx_research::strategy_registry::Bar>>, String>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<okx_research::strategy_registry::Bar>, String>>,
    {
        let lock = {
            let mut locks = self.locks.lock().unwrap();
            locks.retain(|_, v| v.strong_count() > 0);
            if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(AsyncMutex::new(()));
                locks.insert(key.clone(), Arc::downgrade(&lock));
                lock
            }
        };
        let _guard = lock.lock().await;
        if let Some(e) = self.entries.lock().unwrap().get(&key) {
            return Ok(e.value.clone());
        }
        let _permit = self.global.acquire().await.map_err(|e| e.to_string())?;
        let value = Arc::new(fetch().await?);
        let bytes = value.capacity() * std::mem::size_of::<okx_research::strategy_registry::Bar>();
        if bytes > 256 * 1024 * 1024 {
            return Err("CACHE_MEMORY_LIMIT".into());
        }
        let mut entries = self.entries.lock().unwrap();
        while entries.values().map(|e| e.bytes).sum::<usize>() + bytes > 256 * 1024 * 1024 {
            let k = entries.keys().next().cloned().unwrap();
            entries.remove(&k);
        }
        entries.insert(
            key,
            Entry {
                value: value.clone(),
                bytes,
            },
        );
        Ok(value)
    }
}

impl SourceCache {
    pub async fn permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, String> {
        self.global
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn combined_fields_share_source_fetch() {
        let c = SourceCache::default();
        let key = SourceKey {
            source: "candles".into(),
            instrument: "BTC".into(),
            timeframe_ms: 1,
            start_ms: 0,
            end_ms: 1,
            closed_only: true,
            schema: 1,
        };
        let n = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let f = || {
            let n = n.clone();
            async move {
                n.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(Vec::new())
            }
        };
        let (a, b) = tokio::join!(c.load(key.clone(), f), c.load(key, f));
        a.unwrap();
        b.unwrap();
        assert_eq!(n.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
    #[test]
    fn cache_key_includes_range_and_version() {
        let a = SourceKey {
            source: "candles".into(),
            instrument: "BTC".into(),
            timeframe_ms: 1,
            start_ms: 0,
            end_ms: 1,
            closed_only: true,
            schema: 1,
        };
        let mut b = a.clone();
        b.schema = 2;
        assert_ne!(a, b);
        b = a.clone();
        b.end_ms = 2;
        assert_ne!(a, b);
    }
}
