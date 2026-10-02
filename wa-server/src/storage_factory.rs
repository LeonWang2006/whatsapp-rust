//! `StorageFactory` trait: resolves a JID to a per-account `Backend`.
//!
//! Defined here in `wa-server` (rather than `wacore`) so future storage backend
//! crates can implement it without a circular dependency. The trait itself is
//! platform-agnostic and cheap to clone (wrap internals in `Arc`).

use std::sync::Arc;

use async_trait::async_trait;
use wacore::store::traits::Backend;

use crate::task::PresenceEvent;

/// A `biz.wa_user` row as returned by the init path. Storage-agnostic view so
/// the API can detect a phone-number change without depending on a backend's
/// row type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BizUserRecord {
    pub id: i64,
    pub device_uuid: String,
    pub phone_number: String,
    pub wa_device_id: Option<i32>,
}

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

    /// Look up a `biz.wa_user` by its `device_uuid`, including the infra-side
    /// `wa_device_id`. Used by the init path to tell a new user from a phone-
    /// number change.
    ///
    /// Default returns `None` so in-memory/test factories opt out.
    async fn biz_user_by_device_uuid(
        &self,
        device_uuid: &str,
    ) -> anyhow::Result<Option<BizUserRecord>> {
        let _ = device_uuid;
        Ok(None)
    }

    /// Upsert a `biz.wa_user` keyed by `device_uuid`. Returns the resolved row
    /// so the caller can detect a phone-number change. `clear_wa_device_id`
    /// signals the init path that a stale pairing reference must be dropped
    /// (换卡); the backend never sets it, only clears.
    ///
    /// Default is a no-op that reports no user so in-memory/test factories opt
    /// out.
    #[allow(clippy::too_many_arguments)]
    async fn upsert_biz_user(
        &self,
        device_uuid: &str,
        phone_number: &str,
        platform: Option<i16>,
        os_version: Option<&str>,
        manufacturer: Option<&str>,
        device_model: Option<&str>,
        os_build_number: Option<&str>,
        locale_language: Option<&str>,
        locale_country: Option<&str>,
        device_info_raw: Option<&str>,
        firebase_token: Option<&str>,
        apns_token: Option<&str>,
        notification: bool,
        clear_wa_device_id: bool,
    ) -> anyhow::Result<BizUserRecord> {
        let _ = (
            device_uuid,
            phone_number,
            platform,
            os_version,
            manufacturer,
            device_model,
            os_build_number,
            locale_language,
            locale_country,
            device_info_raw,
            firebase_token,
            apns_token,
            notification,
            clear_wa_device_id,
        );
        Ok(BizUserRecord {
            id: 0,
            device_uuid: device_uuid.to_string(),
            phone_number: phone_number.to_string(),
            wa_device_id: None,
        })
    }

    /// Record a pairing-flow lifecycle event in `biz.pair_history`.
    ///
    /// Default is a no-op so in-memory/test factories opt out.
    async fn record_pair_history(
        &self,
        user_id: i64,
        phone_number: Option<&str>,
        wa_device_id: Option<i32>,
        action: &str,
        pair_code: Option<&str>,
        detail: Option<&str>,
    ) -> anyhow::Result<()> {
        let _ = (
            user_id,
            phone_number,
            wa_device_id,
            action,
            pair_code,
            detail,
        );
        Ok(())
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
