pub mod commands;
pub mod error;
pub mod persistence_manager;
#[cfg(feature = "postgres-storage")]
pub mod redis_registry;
pub mod signal;
pub mod signal_adapter;
pub mod signal_cache;
pub mod traits;

// Re-export from the sqlite-storage crate when the feature is enabled
#[cfg(feature = "sqlite-storage")]
pub use whatsapp_rust_sqlite_storage::SqliteStore;

// Re-export from the postgres-storage crate when the feature is enabled
#[cfg(feature = "postgres-storage")]
pub use whatsapp_rust_postgres_storage::PostgresStore;

pub use crate::store::traits::*;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
pub use wacore::store::StorageFactory;

#[derive(Clone)]
pub struct Device {
    pub core: wacore::store::Device,
    pub backend: Arc<dyn Backend>,
}

impl Deref for Device {
    type Target = wacore::store::Device;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl DerefMut for Device {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.core
    }
}

impl Device {
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        let core = wacore::store::Device::new();
        Self { core, backend }
    }

    pub fn to_serializable(&self) -> wacore::store::Device {
        self.core.clone()
    }

    pub fn load_from_serializable(&mut self, loaded: wacore::store::Device) {
        self.core = loaded;
    }
}
