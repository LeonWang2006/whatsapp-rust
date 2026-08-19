//! `StorageFactory` trait: resolves a JID to a per-account `Backend`.
//!
//! Defined here in `wa-server` (rather than `wacore`) so future storage backend
//! crates can implement it without a circular dependency. The trait itself is
//! platform-agnostic and cheap to clone (wrap internals in `Arc`).

use std::sync::Arc;

use async_trait::async_trait;
use wacore::store::traits::Backend;

use crate::task::PresenceEvent;

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

    /// Enumerate every JID that already has a device row, for startup restore.
    ///
    /// Returns an empty vec for factories that cannot enumerate (e.g.
    /// in-memory backends lose state on restart anyway). Default returns an
    /// empty vec so in-memory/test factories do not need to implement it.
    async fn all_jids(&self) -> anyhow::Result<Vec<String>> {
        Ok(Vec::new())
    }

    /// Look up the business user id owning `phone`, if any.
    ///
    /// Used after a session connects to fetch the contacts to auto-subscribe
    /// presence for. Default returns `None` so in-memory/test factories opt out.
    async fn biz_user_id_by_phone(&self, phone: &str) -> anyhow::Result<Option<i64>> {
        let _ = phone;
        Ok(None)
    }

    /// Contact phone numbers for a business user, in insertion order.
    ///
    /// Default returns an empty vec so in-memory/test factories opt out.
    async fn biz_contacts_for_user(&self, user_id: i64) -> anyhow::Result<Vec<String>> {
        let _ = user_id;
        Ok(Vec::new())
    }

    /// Persist a contact's online/offline presence event. The session worker
    /// records every `Event::Presence` here so the API can answer range queries.
    ///
    /// Default is a no-op so in-memory/test factories (no PG) do nothing.
    async fn record_presence_event(
        &self,
        owner_phone: &str,
        contact_phone: &str,
        event_type: &str,
        ts: i64,
        last_seen: Option<i64>,
    ) -> anyhow::Result<()> {
        let _ = (owner_phone, contact_phone, event_type, ts, last_seen);
        Ok(())
    }

    /// Query presence events for one owner + contact in a `[start, end]` window.
    ///
    /// Default returns an empty vec so in-memory/test factories report nothing.
    async fn query_presence_events(
        &self,
        owner_phone: &str,
        contact_phone: &str,
        start: i64,
        end: i64,
    ) -> anyhow::Result<Vec<PresenceEvent>> {
        let _ = (owner_phone, contact_phone, start, end);
        Ok(Vec::new())
    }
}
