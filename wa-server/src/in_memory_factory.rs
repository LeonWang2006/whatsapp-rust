//! In-memory [`StorageFactory`] implementation.
//!
//! Backs each JID with a fresh [`InMemoryBackend`]. All state lives in RAM and
//! is lost when the process exits — this exists to get the server framework +
//! API running without a database. The PostgreSQL factory replaces it later
//! with zero changes to the session/dispatcher code paths.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use log::warn;
use tokio::sync::Mutex;
use wacore::store::in_memory::InMemoryBackend;
use wacore::store::traits::Backend;

use crate::storage_factory::StorageFactory;

/// Per-JID entry: `(device_id, backend)`.
type BackendEntry = (i32, Arc<dyn Backend>);

/// Per-JID in-memory backends. Not shared across pods; use only for local
/// development / single-pod smoke tests.
#[derive(Clone, Default)]
pub struct InMemoryStorageFactory {
    backends: Arc<Mutex<HashMap<String, BackendEntry>>>,
    next_device_id: Arc<std::sync::atomic::AtomicI32>,
}

impl InMemoryStorageFactory {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StorageFactory for InMemoryStorageFactory {
    async fn for_jid(&self, jid: &str) -> Option<Arc<dyn Backend>> {
        let backends = self.backends.lock().await;
        backends.get(jid).map(|(_, b)| b.clone())
    }

    async fn for_device_id(&self, device_id: i32) -> Option<Arc<dyn Backend>> {
        let backends = self.backends.lock().await;
        backends
            .values()
            .find(|(id, _)| *id == device_id)
            .map(|(_, b)| b.clone())
    }

    async fn create_for_jid(&self, jid: &str) -> Result<(i32, Arc<dyn Backend>)> {
        let mut backends = self.backends.lock().await;
        if let Some((id, b)) = backends.get(jid) {
            return Ok((*id, b.clone()));
        }
        let device_id = self
            .next_device_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let backend: Arc<dyn Backend> = Arc::new(InMemoryBackend::new());
        backends.insert(jid.to_string(), (device_id, backend.clone()));
        Ok((device_id, backend))
    }

    async fn delete_for_jid(&self, jid: &str) -> Result<()> {
        let mut backends = self.backends.lock().await;
        if backends.remove(jid).is_some() {
            Ok(())
        } else {
            warn!("delete_for_jid({jid}) - no backend found");
            Ok(())
        }
    }
}
