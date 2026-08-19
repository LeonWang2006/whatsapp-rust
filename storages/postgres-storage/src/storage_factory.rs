//! `StorageFactory` for PostgreSQL.
//!
//! Resolves `jid -> device_id` via the `jid_device_map` table. Creating a new
//! device inserts both a `device` row and a `jid_device_map` row in one
//! transaction so a partial failure cannot leave an orphaned device. Deleting
//! a device relies on the FK `ON DELETE CASCADE` to purge all account tables.
//!
//! The factory exposes *inherent* methods only; the server crate (`wa-server`)
//! implements its own `StorageFactory` trait over these methods so this crate
//! never has to depend on the server crate (which would be a dependency cycle).

use std::sync::Arc;

use diesel::prelude::*;
use log::warn;
use wacore::store::error::{Result as StoreResult, StoreError};
use wacore::store::traits::Backend;

use crate::postgres_store::PostgresStore;
use crate::schema::{device, jid_device_map};

/// Factory producing [`PostgresStore`] backends per JID. Cheap to clone — all
/// state lives behind the r2d2 pool.
#[derive(Clone)]
pub struct PostgresStorageFactory {
    database_url: String,
}

impl PostgresStorageFactory {
    pub fn new(database_url: String) -> Self {
        Self { database_url }
    }

    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    /// Run pending migrations once at startup.
    pub async fn run_migrations(&self) -> StoreResult<()> {
        let store = PostgresStore::new(&self.database_url)?;
        store.run_migrations().await
    }

    /// Resolve the `device_id` mapped to `jid`, or `None` if no row exists.
    pub async fn device_id_for_jid(&self, jid: &str) -> StoreResult<Option<i32>> {
        let url = self.database_url.clone();
        let jid = jid.to_string();
        tokio::task::spawn_blocking(move || -> StoreResult<Option<i32>> {
            let mut conn =
                PgConnection::establish(&url).map_err(|e| StoreError::Connection(Box::new(e)))?;
            let row: Option<i32> = jid_device_map::table
                .select(jid_device_map::device_id)
                .filter(jid_device_map::jid.eq(&jid))
                .first::<i32>(&mut conn)
                .optional()
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(row)
        })
        .await
        .map_err(|e| StoreError::Database(Box::new(e)))?
    }

    /// Build a backend for an existing `device_id` (no existence check).
    pub fn backend_for_device_id(&self, device_id: i32) -> Option<Arc<dyn Backend>> {
        match PostgresStore::new_for_device(&self.database_url, device_id) {
            Ok(store) => Some(Arc::new(store) as Arc<dyn Backend>),
            Err(e) => {
                warn!("failed to open store for device_id={device_id}: {e}");
                None
            }
        }
    }

    /// Return the backend for an existing session, or `None` if the JID has no
    /// device row yet. Does NOT create a new device.
    pub async fn for_jid(&self, jid: &str) -> Option<Arc<dyn Backend>> {
        match self.device_id_for_jid(jid).await {
            Ok(Some(device_id)) => self.backend_for_device_id(device_id),
            Ok(None) => None,
            Err(e) => {
                warn!("jid lookup failed for jid={jid}: {e}");
                None
            }
        }
    }

    /// Create a new device row for `jid` and return its backend. Idempotent:
    /// an existing mapping is reused. The `jid -> device_id` mapping is
    /// persisted so subsequent `for_jid` calls resolve without a second insert.
    pub async fn create_for_jid(&self, jid: &str) -> anyhow::Result<(i32, Arc<dyn Backend>)> {
        let url = self.database_url.clone();
        let jid = jid.to_string();
        let device_id = tokio::task::spawn_blocking(move || -> anyhow::Result<i32> {
            let mut conn =
                PgConnection::establish(&url).map_err(|e| anyhow::anyhow!("connect: {e}"))?;
            conn.transaction(|conn| -> anyhow::Result<i32> {
                // Reuse an existing mapping if one exists (idempotent create).
                if let Some(existing) = jid_device_map::table
                    .select(jid_device_map::device_id)
                    .filter(jid_device_map::jid.eq(&jid))
                    .first::<i32>(conn)
                    .optional()
                    .map_err(|e| anyhow::anyhow!("lookup: {e}"))?
                {
                    return Ok(existing);
                }
                // Insert a new device row, mirroring PostgresStore::create_new_device
                // inline so we can keep the transaction with the map insert.
                let new_device = wacore::store::Device::new();
                let noise_key_data = {
                    let mut b = Vec::with_capacity(64);
                    b.extend_from_slice(new_device.noise_key.private_key.serialize());
                    b.extend_from_slice(new_device.noise_key.public_key.public_key_bytes());
                    b
                };
                let identity_key_data = {
                    let mut b = Vec::with_capacity(64);
                    b.extend_from_slice(new_device.identity_key.private_key.serialize());
                    b.extend_from_slice(new_device.identity_key.public_key.public_key_bytes());
                    b
                };
                let signed_pre_key_data = {
                    let mut b = Vec::with_capacity(64);
                    b.extend_from_slice(new_device.signed_pre_key.private_key.serialize());
                    b.extend_from_slice(new_device.signed_pre_key.public_key.public_key_bytes());
                    b
                };
                let device_id: i32 = diesel::insert_into(device::table)
                    .values((
                        device::lid.eq(""),
                        device::pn.eq(""),
                        device::registration_id.eq(new_device.registration_id as i32),
                        device::noise_key.eq(&noise_key_data),
                        device::identity_key.eq(&identity_key_data),
                        device::signed_pre_key.eq(&signed_pre_key_data),
                        device::signed_pre_key_id.eq(new_device.signed_pre_key_id as i32),
                        device::signed_pre_key_signature
                            .eq(&new_device.signed_pre_key_signature[..]),
                        device::adv_secret_key.eq(&new_device.adv_secret_key[..]),
                        device::account.eq(None::<Vec<u8>>),
                        device::push_name.eq(&new_device.push_name),
                        device::app_version_primary.eq(new_device.app_version_primary as i32),
                        device::app_version_secondary.eq(new_device.app_version_secondary as i32),
                        device::app_version_tertiary.eq(new_device.app_version_tertiary as i64),
                        device::app_version_last_fetched_ms
                            .eq(new_device.app_version_last_fetched_ms),
                        device::edge_routing_info.eq(None::<Vec<u8>>),
                        device::props_hash.eq(None::<String>),
                        device::next_pre_key_id.eq(new_device.next_pre_key_id as i32),
                        device::nct_salt.eq(None::<Vec<u8>>),
                        device::server_has_prekeys.eq(new_device.server_has_prekeys),
                        device::first_unupload_pre_key_id
                            .eq(new_device.first_unupload_pre_key_id as i32),
                        device::server_cert_chain.eq(None::<Vec<u8>>),
                        device::login_counter.eq(0i32),
                        device::lid_migrated.eq(false),
                        device::last_signed_pre_key_rotation_ms
                            .eq(new_device.last_signed_pre_key_rotation_ms),
                        device::read_receipts_disabled.eq(false),
                    ))
                    .returning(device::id)
                    .get_result(conn)
                    .map_err(|e| anyhow::anyhow!("insert device: {e}"))?;

                diesel::insert_into(jid_device_map::table)
                    .values((
                        jid_device_map::jid.eq(&jid),
                        jid_device_map::device_id.eq(device_id),
                    ))
                    .execute(conn)
                    .map_err(|e| anyhow::anyhow!("insert jid map: {e}"))?;
                Ok(device_id)
            })
        })
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking: {e}"))??;

        let store = PostgresStore::new_for_device(&self.database_url, device_id)?;
        Ok((device_id, Arc::new(store) as Arc<dyn Backend>))
    }

    /// Drop the device row and all cascading account data for `jid`.
    pub async fn delete_for_jid(&self, jid: &str) -> anyhow::Result<()> {
        let url = self.database_url.clone();
        let jid = jid.to_string();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let mut conn =
                PgConnection::establish(&url).map_err(|e| anyhow::anyhow!("connect: {e}"))?;
            // FK ON DELETE CASCADE on jid_device_map removes the mapping row;
            // device row deletion cascades to all account tables.
            let device_ids: Vec<i32> = jid_device_map::table
                .select(jid_device_map::device_id)
                .filter(jid_device_map::jid.eq(&jid))
                .load(&mut conn)
                .map_err(|e| anyhow::anyhow!("lookup: {e}"))?;
            for id in device_ids {
                diesel::delete(device::table.filter(device::id.eq(id)))
                    .execute(&mut conn)
                    .map_err(|e| anyhow::anyhow!("delete device {id}: {e}"))?;
            }
            Ok(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking: {e}"))??;
        Ok(())
    }

    /// Return every JID that has a device row, for startup restore.
    ///
    /// Lets a freshly-started pod enumerate previously-paired devices so it can
    /// reconnect them without waiting for an external task. A pod must still
    /// win the registry lease per JID before connecting (see
    /// [`crate::redis_registry::register_in_redis`]) to avoid double-connecting
    /// a device already live on another pod.
    pub async fn all_jids(&self) -> anyhow::Result<Vec<String>> {
        let url = self.database_url.clone();
        let jids = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<String>> {
            let mut conn =
                PgConnection::establish(&url).map_err(|e| anyhow::anyhow!("connect: {e}"))?;
            let jids: Vec<String> = jid_device_map::table
                .select(jid_device_map::jid)
                .load(&mut conn)
                .map_err(|e| anyhow::anyhow!("load jids: {e}"))?;
            Ok(jids)
        })
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking: {e}"))??;
        Ok(jids)
    }

    /// Look up a `biz.wa_user` by its current phone number.
    pub async fn biz_user_by_phone(&self, phone: &str) -> StoreResult<Option<crate::biz::BizUser>> {
        crate::biz::biz_user_by_phone(&self.database_url, phone).await
    }

    /// Return the contact phone numbers a user has added, in insertion order.
    pub async fn biz_contacts_for_user(&self, user_id: i64) -> StoreResult<Vec<String>> {
        crate::biz::biz_contacts_for_user(&self.database_url, user_id).await
    }

    /// Persist a presence (online/offline) event for one owner + contact.
    pub async fn record_presence_event(
        &self,
        owner_phone: &str,
        contact_phone: &str,
        event_type: &str,
        ts: i64,
        last_seen: Option<i64>,
    ) -> StoreResult<()> {
        crate::biz::record_presence_event(
            &self.database_url,
            owner_phone,
            contact_phone,
            event_type,
            ts,
            last_seen,
        )
        .await
    }

    /// Query presence events for one owner + contact in a time window.
    pub async fn query_presence_events(
        &self,
        owner_phone: &str,
        contact_phone: &str,
        start: i64,
        end: i64,
    ) -> StoreResult<Vec<crate::biz::PresenceEvent>> {
        crate::biz::query_presence_events(
            &self.database_url,
            owner_phone,
            contact_phone,
            start,
            end,
        )
        .await
    }
}
