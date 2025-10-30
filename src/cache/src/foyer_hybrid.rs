use std::{io::Bytes, sync::Arc};

use foyer::HybridCache;

struct LruResultCache {
    pub inner: Arc<HybridCache<u64, Bytes>>,
}
