//! `StorageFactory` trait: resolves a JID to a per-account `Backend`.
//!
//! Defined here in `wa-server` (rather than `wacore`) so future storage backend
//! crates can implement it without a circular dependency. The trait itself is
//! platform-agnostic and cheap to clone (wrap internals in `Arc`).

use std::sync::Arc;

use async_trait::async_trait;
use wacore::store::traits::Backend;

/// Factory that produces per-account storage backends.
///
/// Implementations decide how `jid` maps to a `device_id` and whether a new
/// device row should be created on first sight.
#[async_trait]
pub trait StorageFactory: Send + Sync {
    /// Return the backend for an existing session, or `None` if the JID has no
    /// device row yet. Does NOT create a new device.
    async fn for_jid(&self, jid: &str) -> Option<Arc<dyn Backend>>;

    /// Return the backend for an existing `device_id`, or `None` if absent.
    async fn for_device_id(&self, device_id: i32) -> Option<Arc<dyn Backend>>;

    /// Create a new device row for `jid` and return its backend.
    ///
    /// Implementations are responsible for persisting the `jid -> device_id`
    /// mapping so that subsequent `for_jid` calls resolve without a second
    /// insert. Returns the newly assigned `device_id` alongside the backend.
    async fn create_for_jid(&self, jid: &str) -> anyhow::Result<(i32, Arc<dyn Backend>)>;

    /// Drop the device row and all cascading account data for `jid`.
    async fn delete_for_jid(&self, jid: &str) -> anyhow::Result<()>;
}
