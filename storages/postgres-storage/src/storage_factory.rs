//! `StorageFactory` implementation backed by PostgreSQL.
//!
//! Resolves `jid -> device_id` via the `jid_device_map` table. Creating a new
//! device inserts both a `device` row and a `jid_device_map` row in one
//! transaction so a partial failure cannot leave an orphaned device.

use std::sync::Arc;

use async_trait::async_trait;
use diesel::prelude::*;
use log::warn;
use wacore::store::StorageFactory;
use wacore::store::error::{Result as StoreResult, StoreError};
use wacore::store::traits::Backend;

use crate::postgres_store::PostgresStore;
use crate::schema::{device, jid_device_map};

/// Factory producing `PostgresStore` backends per JID. Cheap to clone - all
/// state lives behind the r2d2 pool.
#[derive(Clone)]
pub struct PostgresStorageFactory {
    database_url: String,
}

impl PostgresStorageFactory {
    pub fn new(database_url: String) -> Self {
        Self { database_url }
    }

    /// Run pending migrations once at startup.
    pub async fn run_migrations(&self) -> StoreResult<()> {
        let store = PostgresStore::new(&self.database_url).await?;
        store.run_migrations().await
    }

    async fn device_id_for_jid(&self, jid: &str) -> StoreResult<Option<i32>> {
        let url = self.database_url.clone();
        let jid = jid.to_string();
        tokio::task::spawn_blocking(move || -> StoreResult<Option<i32>> {
            let mut conn = diesel::PgConnection::establish(&url)
                .map_err(|e| StoreError::Connection(e.to_string()))?;
            let row: Option<i32> = jid_device_map::table
                .select(jid_device_map::device_id)
                .filter(jid_device_map::jid.eq(&jid))
                .first::<i32>(&mut conn)
                .optional()
                .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(row)
        })
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?
    }
}

#[async_trait]
impl StorageFactory for PostgresStorageFactory {
    async fn for_jid(&self, jid: &str) -> Option<Arc<dyn Backend>> {
        match self.device_id_for_jid(jid).await {
            Ok(Some(device_id)) => {
                match PostgresStore::new_for_device(&self.database_url, device_id).await {
                    Ok(store) => Some(Arc::new(store) as Arc<dyn Backend>),
                    Err(e) => {
                        warn!("failed to open store for jid={jid} device={device_id}: {e}");
                        None
                    }
                }
            }
            Ok(None) => None,
            Err(e) => {
                warn!("jid lookup failed for jid={jid}: {e}");
                None
            }
        }
    }

    async fn for_device_id(&self, device_id: i32) -> Option<Arc<dyn Backend>> {
        match PostgresStore::new_for_device(&self.database_url, device_id).await {
            Ok(store) => Some(Arc::new(store) as Arc<dyn Backend>),
            Err(e) => {
                warn!("failed to open store for device_id={device_id}: {e}");
                None
            }
        }
    }

    async fn create_for_jid(&self, jid: &str) -> anyhow::Result<(i32, Arc<dyn Backend>)> {
        let url = self.database_url.clone();
        let jid = jid.to_string();
        let device_id = tokio::task::spawn_blocking(move || -> anyhow::Result<i32> {
            let mut conn = diesel::PgConnection::establish(&url)
                .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
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
                // Insert a new device row. Mirror PostgresStore::create_new_device
                // but inline so we can keep the transaction with the map insert.
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

        let store = PostgresStore::new_for_device(&self.database_url, device_id).await?;
        Ok((device_id, Arc::new(store) as Arc<dyn Backend>))
    }

    async fn delete_for_jid(&self, jid: &str) -> anyhow::Result<()> {
        let url = self.database_url.clone();
        let jid = jid.to_string();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let mut conn = diesel::PgConnection::establish(&url)
                .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
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
}
