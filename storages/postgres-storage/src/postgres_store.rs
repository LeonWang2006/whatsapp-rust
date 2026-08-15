//! PostgreSQL-backed [`Backend`] for whatsapp-rust.
//!
//! One [`PostgresStore`] wraps one `device_id` row. All account data is scoped
//! by `device_id`, so many WhatsApp accounts share one database — this is the
//! multi-pod shared storage backend.
//!
//! Every database call goes through [`PostgresStore::with_conn`], which runs
//! the blocking closure on a pooled r2d2 connection inside
//! `tokio::task::spawn_blocking`. PG MVCC lets concurrent transactions
//! proceed without the process-wide semaphore the SQLite backend needs.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::upsert::excluded;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use wacore::appstate::hash::HashState;
use wacore::appstate::processor::AppStateMutationMAC;
use wacore::client_profile::ClientProfile;
use wacore::libsignal::protocol::{KeyPair, PrivateKey, PublicKey};
use wacore::store::Device as CoreDevice;
use wacore::store::device::{CachedServerCertChain, DEVICE_PROPS};
use wacore::store::error::{Result, StoreError};
use wacore::store::traits::*;

use crate::schema::*;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

type PgPool = Pool<ConnectionManager<PgConnection>>;

/// Row representation for the `device` table. Field order must match column order in schema.
#[derive(Queryable)]
#[allow(dead_code)]
struct DeviceRow {
    id: i32,
    lid: String,
    pn: String,
    registration_id: i32,
    noise_key: Vec<u8>,
    identity_key: Vec<u8>,
    signed_pre_key: Vec<u8>,
    signed_pre_key_id: i32,
    signed_pre_key_signature: Vec<u8>,
    adv_secret_key: Vec<u8>,
    account: Option<Vec<u8>>,
    push_name: String,
    app_version_primary: i32,
    app_version_secondary: i32,
    app_version_tertiary: i64,
    app_version_last_fetched_ms: i64,
    edge_routing_info: Option<Vec<u8>>,
    props_hash: Option<String>,
    next_pre_key_id: i32,
    nct_salt: Option<Vec<u8>>,
    server_has_prekeys: bool,
    first_unupload_pre_key_id: i32,
    server_cert_chain: Option<Vec<u8>>,
    login_counter: i32,
    lid_migrated: bool,
    last_signed_pre_key_rotation_ms: i64,
    read_receipts_disabled: bool,
}

/// One pooled `PostgresStore` per device. Cheap to clone — the r2d2 pool is
/// shared behind the `Arc`-owned pool handle.
#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
    device_id: i32,
}

#[derive(Debug)]
struct ConnectionOptions;

impl diesel::r2d2::CustomizeConnection<PgConnection, diesel::r2d2::Error> for ConnectionOptions {
    fn on_acquire(&self, conn: &mut PgConnection) -> std::result::Result<(), diesel::r2d2::Error> {
        diesel::sql_query("SET statement_timeout = 10000;")
            .execute(conn)
            .map_err(diesel::r2d2::Error::QueryError)?;
        diesel::sql_query("SET idle_in_transaction_session_timeout = 30000;")
            .execute(conn)
            .map_err(diesel::r2d2::Error::QueryError)?;
        Ok(())
    }
}

impl PostgresStore {
    /// Open a store for `device_id`, building a fresh r2d2 pool. Each pod's
    /// process builds its own pool; PG multiplexes across them.
    pub fn new_for_device(database_url: &str, device_id: i32) -> Result<Self> {
        let manager = ConnectionManager::<PgConnection>::new(database_url);
        let pool = Pool::builder()
            .max_size(8)
            .connection_customizer(Box::new(ConnectionOptions))
            .build(manager)
            .map_err(|e| StoreError::Connection(Box::new(e)))?;
        Ok(Self { pool, device_id })
    }

    /// Open a store for `device_id` = 1. Convenience for single-account CLIs;
    /// the server factory uses [`Self::new_for_device`].
    pub fn new(database_url: &str) -> Result<Self> {
        Self::new_for_device(database_url, 1)
    }

    /// Run pending migrations once at process startup. Concurrent calls across
    /// pods are safe: PG serializes DDL via the `__diesel_schema_migrations`
    /// table lock, but within one process only invoke once.
    pub async fn run_migrations(&self) -> Result<()> {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = pool
                .get()
                .map_err(|e| StoreError::Connection(Box::new(e)))?;
            conn.run_pending_migrations(MIGRATIONS)
                .map_err(StoreError::Migration)?;
            Ok(())
        })
        .await
        .map_err(|e| StoreError::Database(Box::new(e)))?
    }

    /// Connect, run migrations, and return a store for `device_id` = 1.
    pub async fn connect(database_url: &str) -> Result<Self> {
        let store = Self::new(database_url)?;
        store.run_migrations().await?;
        Ok(store)
    }

    pub fn device_id(&self) -> i32 {
        self.device_id
    }

    /// Run a blocking DB operation on a pooled connection. PG MVCC lets
    /// concurrent transactions proceed without a process-wide semaphore.
    async fn with_conn<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut PgConnection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || -> Result<T> {
            let mut conn = pool
                .get()
                .map_err(|e| StoreError::Connection(Box::new(e)))?;
            f(&mut conn)
        })
        .await
        .map_err(|e| StoreError::Database(Box::new(e)))?
    }

    fn serialize_keypair(key_pair: &KeyPair) -> Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(64);
        bytes.extend_from_slice(key_pair.private_key.serialize());
        bytes.extend_from_slice(key_pair.public_key.public_key_bytes());
        Ok(bytes)
    }

    fn deserialize_keypair(bytes: &[u8]) -> Result<KeyPair> {
        if bytes.len() != 64 {
            return Err(StoreError::Serialization(
                format!("Invalid KeyPair length: {}", bytes.len()).into(),
            ));
        }
        let private_key = PrivateKey::deserialize(&bytes[0..32])
            .map_err(|e| StoreError::Serialization(Box::new(e)))?;
        let public_key = PublicKey::from_djb_public_key_bytes(&bytes[32..64])
            .map_err(|e| StoreError::Serialization(Box::new(e)))?;
        Ok(KeyPair::new(public_key, private_key))
    }

    fn bincode_encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
        bincode::serde::encode_to_vec(value, bincode::config::standard())
            .map_err(|e| StoreError::Serialization(Box::new(e)))
    }

    fn bincode_decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map(|(v, _)| v)
            .map_err(|e| StoreError::Serialization(Box::new(e)))
    }

    // ----- device-scoped helpers (mirror SqliteStore's `*_for_device`) -----

    pub async fn save_device_data_for_device(
        &self,
        device_id: i32,
        device_data: &CoreDevice,
    ) -> Result<()> {
        let noise_key_data = Self::serialize_keypair(&device_data.noise_key)?;
        let identity_key_data = Self::serialize_keypair(&device_data.identity_key)?;
        let signed_pre_key_data = Self::serialize_keypair(&device_data.signed_pre_key)?;
        let account_data = device_data
            .account
            .as_ref()
            .map(|a| wacore::store::device::account_serde::to_bytes(a));
        let registration_id = device_data.registration_id as i32;
        let signed_pre_key_id = device_data.signed_pre_key_id as i32;
        let signed_pre_key_signature: Vec<u8> = device_data.signed_pre_key_signature.to_vec();
        let adv_secret_key: Vec<u8> = device_data.adv_secret_key.to_vec();
        let push_name = device_data.push_name.clone();
        let app_version_primary = device_data.app_version_primary as i32;
        let app_version_secondary = device_data.app_version_secondary as i32;
        let app_version_tertiary = device_data.app_version_tertiary as i64;
        let app_version_last_fetched_ms = device_data.app_version_last_fetched_ms;
        let edge_routing_info = device_data.edge_routing_info.clone();
        let props_hash = device_data.props_hash.clone();
        let next_pre_key_id = device_data.next_pre_key_id as i32;
        let first_unupload_pre_key_id = device_data.first_unupload_pre_key_id as i32;
        let server_has_prekeys = device_data.server_has_prekeys;
        let nct_salt = device_data.nct_salt.clone();
        let server_cert_chain = device_data
            .server_cert_chain
            .as_ref()
            .map(Self::bincode_encode)
            .transpose()?;
        let login_counter = device_data.login_counter;
        let lid_migrated = device_data.lid_migrated;
        let last_signed_pre_key_rotation_ms = device_data.last_signed_pre_key_rotation_ms;
        let read_receipts_disabled = device_data.read_receipts_disabled;
        let new_lid = device_data
            .lid
            .as_ref()
            .map(|j| j.to_string())
            .unwrap_or_default();
        let new_pn = device_data
            .pn
            .as_ref()
            .map(|j| j.to_string())
            .unwrap_or_default();

        self.with_conn(move |conn| {
            diesel::insert_into(device::table)
                .values((
                    device::id.eq(device_id),
                    device::lid.eq(&new_lid),
                    device::pn.eq(&new_pn),
                    device::registration_id.eq(registration_id),
                    device::noise_key.eq(&noise_key_data),
                    device::identity_key.eq(&identity_key_data),
                    device::signed_pre_key.eq(&signed_pre_key_data),
                    device::signed_pre_key_id.eq(signed_pre_key_id),
                    device::signed_pre_key_signature.eq(&signed_pre_key_signature[..]),
                    device::adv_secret_key.eq(&adv_secret_key[..]),
                    device::account.eq(account_data.clone()),
                    device::push_name.eq(&push_name),
                    device::app_version_primary.eq(app_version_primary),
                    device::app_version_secondary.eq(app_version_secondary),
                    device::app_version_tertiary.eq(app_version_tertiary),
                    device::app_version_last_fetched_ms.eq(app_version_last_fetched_ms),
                    device::edge_routing_info.eq(edge_routing_info.clone()),
                    device::props_hash.eq(props_hash.clone()),
                    device::next_pre_key_id.eq(next_pre_key_id),
                    device::nct_salt.eq(nct_salt.clone()),
                    device::server_has_prekeys.eq(server_has_prekeys),
                    device::first_unupload_pre_key_id.eq(first_unupload_pre_key_id),
                    device::server_cert_chain.eq(server_cert_chain.clone()),
                    device::login_counter.eq(login_counter),
                    device::lid_migrated.eq(lid_migrated),
                    device::last_signed_pre_key_rotation_ms.eq(last_signed_pre_key_rotation_ms),
                    device::read_receipts_disabled.eq(read_receipts_disabled),
                ))
                .on_conflict(device::id)
                .do_update()
                .set((
                    device::lid.eq(excluded(device::lid)),
                    device::pn.eq(excluded(device::pn)),
                    device::registration_id.eq(excluded(device::registration_id)),
                    device::noise_key.eq(excluded(device::noise_key)),
                    device::identity_key.eq(excluded(device::identity_key)),
                    device::signed_pre_key.eq(excluded(device::signed_pre_key)),
                    device::signed_pre_key_id.eq(excluded(device::signed_pre_key_id)),
                    device::signed_pre_key_signature.eq(excluded(device::signed_pre_key_signature)),
                    device::adv_secret_key.eq(excluded(device::adv_secret_key)),
                    device::account.eq(excluded(device::account)),
                    device::push_name.eq(excluded(device::push_name)),
                    device::app_version_primary.eq(excluded(device::app_version_primary)),
                    device::app_version_secondary.eq(excluded(device::app_version_secondary)),
                    device::app_version_tertiary.eq(excluded(device::app_version_tertiary)),
                    device::app_version_last_fetched_ms
                        .eq(excluded(device::app_version_last_fetched_ms)),
                    device::edge_routing_info.eq(excluded(device::edge_routing_info)),
                    device::props_hash.eq(excluded(device::props_hash)),
                    device::next_pre_key_id.eq(excluded(device::next_pre_key_id)),
                    device::nct_salt.eq(excluded(device::nct_salt)),
                    device::server_has_prekeys.eq(excluded(device::server_has_prekeys)),
                    device::first_unupload_pre_key_id
                        .eq(excluded(device::first_unupload_pre_key_id)),
                    device::server_cert_chain.eq(excluded(device::server_cert_chain)),
                    device::login_counter.eq(excluded(device::login_counter)),
                    device::lid_migrated.eq(excluded(device::lid_migrated)),
                    device::last_signed_pre_key_rotation_ms
                        .eq(excluded(device::last_signed_pre_key_rotation_ms)),
                    device::read_receipts_disabled.eq(excluded(device::read_receipts_disabled)),
                ))
                .execute(conn)
                .map(|_| ())
                .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    /// Insert a fresh `device` row (generated from [`wacore::store::Device::new`])
    /// and return its `id`. `lid`/`pn` start empty; they are filled in on the
    /// first save after pairing.
    pub async fn create_new_device(&self) -> Result<i32> {
        let new_device = wacore::store::Device::new();
        let noise_key_data = Self::serialize_keypair(&new_device.noise_key)?;
        let identity_key_data = Self::serialize_keypair(&new_device.identity_key)?;
        let signed_pre_key_data = Self::serialize_keypair(&new_device.signed_pre_key)?;

        self.with_conn(move |conn| {
            diesel::insert_into(device::table)
                .values((
                    device::lid.eq(""),
                    device::pn.eq(""),
                    device::registration_id.eq(new_device.registration_id as i32),
                    device::noise_key.eq(&noise_key_data),
                    device::identity_key.eq(&identity_key_data),
                    device::signed_pre_key.eq(&signed_pre_key_data),
                    device::signed_pre_key_id.eq(new_device.signed_pre_key_id as i32),
                    device::signed_pre_key_signature.eq(&new_device.signed_pre_key_signature[..]),
                    device::adv_secret_key.eq(&new_device.adv_secret_key[..]),
                    device::account.eq(None::<Vec<u8>>),
                    device::push_name.eq(&new_device.push_name),
                    device::app_version_primary.eq(new_device.app_version_primary as i32),
                    device::app_version_secondary.eq(new_device.app_version_secondary as i32),
                    device::app_version_tertiary.eq(new_device.app_version_tertiary as i64),
                    device::app_version_last_fetched_ms.eq(new_device.app_version_last_fetched_ms),
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
                .get_result::<i32>(conn)
                .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    pub async fn device_exists(&self, device_id: i32) -> Result<bool> {
        self.with_conn(move |conn| {
            let count: i64 = device::table
                .filter(device::id.eq(device_id))
                .count()
                .get_result(conn)
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(count > 0)
        })
        .await
    }

    pub async fn load_device_data_for_device(&self, device_id: i32) -> Result<Option<CoreDevice>> {
        let row = self
            .with_conn(move |conn| {
                let result = device::table
                    .filter(device::id.eq(device_id))
                    .first::<DeviceRow>(conn)
                    .optional()
                    .map_err(|e| StoreError::Database(Box::new(e)))?;
                Ok(result)
            })
            .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let pn = if !row.pn.is_empty() {
            row.pn.parse().ok()
        } else {
            None
        };
        let lid = if !row.lid.is_empty() {
            row.lid.parse().ok()
        } else {
            None
        };

        let noise_key = Self::deserialize_keypair(&row.noise_key)?;
        let identity_key = Self::deserialize_keypair(&row.identity_key)?;
        let signed_pre_key = Self::deserialize_keypair(&row.signed_pre_key)?;

        let signed_pre_key_signature: [u8; 64] =
            row.signed_pre_key_signature.try_into().map_err(|_| {
                StoreError::Validation("Invalid signed_pre_key_signature length".to_string())
            })?;

        let adv_secret_key: [u8; 32] = row
            .adv_secret_key
            .try_into()
            .map_err(|_| StoreError::Validation("Invalid adv_secret_key length".to_string()))?;

        let account = row
            .account
            .map(|data| {
                wacore::store::device::account_serde::from_bytes(&data)
                    .map_err(|e| StoreError::Serialization(Box::new(e)))
            })
            .transpose()?;

        // The cert chain is a perf cache, not load-bearing identity. A corrupt
        // blob must NOT block startup — log and degrade to None so the next
        // connect simply pays one XX handshake to repopulate.
        let server_cert_chain = row.server_cert_chain.as_deref().and_then(|bytes| {
            match Self::bincode_decode::<CachedServerCertChain>(bytes) {
                Ok(chain) => Some(chain),
                Err(e) => {
                    log::warn!(
                        "device {} server_cert_chain blob ({} bytes) failed to decode: {e}; \
                         dropping cache, next connect will use XX",
                        self.device_id,
                        bytes.len(),
                    );
                    None
                }
            }
        });

        Ok(Some(CoreDevice {
            pn,
            lid,
            registration_id: row.registration_id as u32,
            noise_key,
            identity_key,
            signed_pre_key,
            signed_pre_key_id: row.signed_pre_key_id as u32,
            signed_pre_key_signature,
            adv_secret_key,
            account: account.map(Arc::new),
            push_name: row.push_name,
            app_version_primary: row.app_version_primary as u32,
            app_version_secondary: row.app_version_secondary as u32,
            app_version_tertiary: row.app_version_tertiary.try_into().unwrap_or(0u32),
            app_version_last_fetched_ms: row.app_version_last_fetched_ms,
            device_props: Arc::new(DEVICE_PROPS.clone()),
            client_profile: ClientProfile::web(),
            edge_routing_info: row.edge_routing_info,
            props_hash: row.props_hash,
            next_pre_key_id: row.next_pre_key_id as u32,
            first_unupload_pre_key_id: row.first_unupload_pre_key_id as u32,
            server_has_prekeys: row.server_has_prekeys,
            nct_salt: row.nct_salt,
            nct_salt_sync_seen: false,
            server_cert_chain,
            login_counter: row.login_counter,
            lid_migrated: row.lid_migrated,
            last_signed_pre_key_rotation_ms: row.last_signed_pre_key_rotation_ms,
            read_receipts_disabled: row.read_receipts_disabled,
        }))
    }

    pub async fn put_identity_for_device(
        &self,
        address: &str,
        key: [u8; 32],
        device_id: i32,
    ) -> Result<()> {
        let address = address.to_string();
        let key_vec = key.to_vec();
        self.with_conn(move |conn| {
            diesel::insert_into(identities::table)
                .values((
                    identities::address.eq(address),
                    identities::key.eq(&key_vec[..]),
                    identities::device_id.eq(device_id),
                ))
                .on_conflict((identities::address, identities::device_id))
                .do_update()
                .set(identities::key.eq(&key_vec[..]))
                .execute(conn)
                .map(|_| ())
                .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    pub async fn delete_identity_for_device(&self, address: &str, device_id: i32) -> Result<()> {
        let address = address.to_string();
        self.with_conn(move |conn| {
            diesel::delete(
                identities::table
                    .filter(identities::address.eq(address))
                    .filter(identities::device_id.eq(device_id)),
            )
            .execute(conn)
            .map(|_| ())
            .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    pub async fn load_identity_for_device(
        &self,
        address: &str,
        device_id: i32,
    ) -> Result<Option<Vec<u8>>> {
        let address = address.to_string();
        self.with_conn(move |conn| {
            let res: Option<Vec<u8>> = identities::table
                .select(identities::key)
                .filter(identities::address.eq(address))
                .filter(identities::device_id.eq(device_id))
                .first(conn)
                .optional()
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(res)
        })
        .await
    }

    pub async fn get_session_for_device(
        &self,
        address: &str,
        device_id: i32,
    ) -> Result<Option<Vec<u8>>> {
        let address = address.to_string();
        self.with_conn(move |conn| {
            let res: Option<Vec<u8>> = sessions::table
                .select(sessions::record)
                .filter(sessions::address.eq(address))
                .filter(sessions::device_id.eq(device_id))
                .first(conn)
                .optional()
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(res)
        })
        .await
    }

    pub async fn put_session_for_device(
        &self,
        address: &str,
        session: &[u8],
        device_id: i32,
    ) -> Result<()> {
        let address = address.to_string();
        let session_vec = session.to_vec();
        self.with_conn(move |conn| {
            diesel::insert_into(sessions::table)
                .values((
                    sessions::address.eq(address),
                    sessions::record.eq(&session_vec),
                    sessions::device_id.eq(device_id),
                ))
                .on_conflict((sessions::address, sessions::device_id))
                .do_update()
                .set(sessions::record.eq(&session_vec))
                .execute(conn)
                .map(|_| ())
                .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    pub async fn delete_session_for_device(&self, address: &str, device_id: i32) -> Result<()> {
        let address = address.to_string();
        self.with_conn(move |conn| {
            diesel::delete(
                sessions::table
                    .filter(sessions::address.eq(address))
                    .filter(sessions::device_id.eq(device_id)),
            )
            .execute(conn)
            .map(|_| ())
            .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    pub async fn put_sender_key_for_device(
        &self,
        address: &str,
        record: &[u8],
        device_id: i32,
    ) -> Result<()> {
        let address = address.to_string();
        let record_vec = record.to_vec();
        self.with_conn(move |conn| {
            diesel::insert_into(sender_keys::table)
                .values((
                    sender_keys::address.eq(address),
                    sender_keys::record.eq(&record_vec),
                    sender_keys::device_id.eq(device_id),
                ))
                .on_conflict((sender_keys::address, sender_keys::device_id))
                .do_update()
                .set(sender_keys::record.eq(&record_vec))
                .execute(conn)
                .map(|_| ())
                .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    pub async fn get_sender_key_for_device(
        &self,
        address: &str,
        device_id: i32,
    ) -> Result<Option<Vec<u8>>> {
        let address = address.to_string();
        self.with_conn(move |conn| {
            let res: Option<Vec<u8>> = sender_keys::table
                .select(sender_keys::record)
                .filter(sender_keys::address.eq(address))
                .filter(sender_keys::device_id.eq(device_id))
                .first(conn)
                .optional()
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(res)
        })
        .await
    }

    pub async fn delete_sender_key_for_device(&self, address: &str, device_id: i32) -> Result<()> {
        let address = address.to_string();
        self.with_conn(move |conn| {
            diesel::delete(
                sender_keys::table
                    .filter(sender_keys::address.eq(address))
                    .filter(sender_keys::device_id.eq(device_id)),
            )
            .execute(conn)
            .map(|_| ())
            .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    pub async fn get_app_state_sync_key_for_device(
        &self,
        key_id: &[u8],
        device_id: i32,
    ) -> Result<Option<AppStateSyncKey>> {
        let key_id = key_id.to_vec();
        let res: Option<Vec<u8>> = self
            .with_conn(move |conn| {
                let res: Option<Vec<u8>> = app_state_keys::table
                    .select(app_state_keys::key_data)
                    .filter(app_state_keys::key_id.eq(&key_id))
                    .filter(app_state_keys::device_id.eq(device_id))
                    .first(conn)
                    .optional()
                    .map_err(|e| StoreError::Database(Box::new(e)))?;
                Ok(res)
            })
            .await?;

        match res {
            Some(data) => Ok(Some(Self::bincode_decode(&data)?)),
            None => Ok(None),
        }
    }

    pub async fn set_app_state_sync_key_for_device(
        &self,
        key_id: &[u8],
        key: AppStateSyncKey,
        device_id: i32,
    ) -> Result<()> {
        let key_id = key_id.to_vec();
        let data = Self::bincode_encode(&key)?;
        self.with_conn(move |conn| {
            diesel::insert_into(app_state_keys::table)
                .values((
                    app_state_keys::key_id.eq(&key_id),
                    app_state_keys::key_data.eq(&data),
                    app_state_keys::device_id.eq(device_id),
                ))
                .on_conflict((app_state_keys::key_id, app_state_keys::device_id))
                .do_update()
                .set(app_state_keys::key_data.eq(&data))
                .execute(conn)
                .map(|_| ())
                .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    pub async fn get_latest_app_state_sync_key_id_for_device(
        &self,
        device_id: i32,
    ) -> Result<Option<Vec<u8>>> {
        self.with_conn(move |conn| {
            let res: Option<Vec<u8>> = app_state_keys::table
                .select(app_state_keys::key_id)
                .filter(app_state_keys::device_id.eq(device_id))
                .order(app_state_keys::key_id.desc())
                .first(conn)
                .optional()
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(res)
        })
        .await
    }

    pub async fn get_app_state_version_for_device(
        &self,
        name: &str,
        device_id: i32,
    ) -> Result<HashState> {
        let name = name.to_string();
        let res: Option<Vec<u8>> = self
            .with_conn(move |conn| {
                let res: Option<Vec<u8>> = app_state_versions::table
                    .select(app_state_versions::state_data)
                    .filter(app_state_versions::name.eq(name))
                    .filter(app_state_versions::device_id.eq(device_id))
                    .first(conn)
                    .optional()
                    .map_err(|e| StoreError::Database(Box::new(e)))?;
                Ok(res)
            })
            .await?;

        match res {
            Some(data) => Ok(Self::bincode_decode(&data)?),
            None => Ok(HashState::default()),
        }
    }

    pub async fn set_app_state_version_for_device(
        &self,
        name: &str,
        state: HashState,
        device_id: i32,
    ) -> Result<()> {
        let name = name.to_string();
        let data = Self::bincode_encode(&state)?;
        self.with_conn(move |conn| {
            diesel::insert_into(app_state_versions::table)
                .values((
                    app_state_versions::name.eq(&name),
                    app_state_versions::state_data.eq(&data),
                    app_state_versions::device_id.eq(device_id),
                ))
                .on_conflict((app_state_versions::name, app_state_versions::device_id))
                .do_update()
                .set(app_state_versions::state_data.eq(&data))
                .execute(conn)
                .map(|_| ())
                .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    pub async fn put_app_state_mutation_macs_for_device(
        &self,
        name: &str,
        version: u64,
        mutations: &[AppStateMutationMAC],
        device_id: i32,
    ) -> Result<()> {
        if mutations.is_empty() {
            return Ok(());
        }
        let name = name.to_string();
        let mutations = mutations.to_vec();
        self.with_conn(move |conn| {
            let records: Vec<_> = mutations
                .iter()
                .map(|m| {
                    (
                        app_state_mutation_macs::name.eq(&name),
                        app_state_mutation_macs::version.eq(version as i64),
                        app_state_mutation_macs::index_mac.eq(&m.index_mac),
                        app_state_mutation_macs::value_mac.eq(&m.value_mac),
                        app_state_mutation_macs::device_id.eq(device_id),
                    )
                })
                .collect();

            conn.transaction(|conn| {
                for chunk in records.chunks(500) {
                    diesel::insert_into(app_state_mutation_macs::table)
                        .values(chunk)
                        .on_conflict((
                            app_state_mutation_macs::name,
                            app_state_mutation_macs::index_mac,
                            app_state_mutation_macs::device_id,
                        ))
                        .do_update()
                        .set((
                            app_state_mutation_macs::version
                                .eq(excluded(app_state_mutation_macs::version)),
                            app_state_mutation_macs::value_mac
                                .eq(excluded(app_state_mutation_macs::value_mac)),
                        ))
                        .execute(conn)?;
                }
                Ok::<(), diesel::result::Error>(())
            })
            .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(())
        })
        .await
    }

    pub async fn delete_app_state_mutation_macs_for_device(
        &self,
        name: &str,
        index_macs: &[Vec<u8>],
        device_id: i32,
    ) -> Result<()> {
        if index_macs.is_empty() {
            return Ok(());
        }
        let name = name.to_string();
        let index_macs = index_macs.to_vec();
        self.with_conn(move |conn| {
            conn.transaction(|conn| {
                for chunk in index_macs.chunks(500) {
                    diesel::delete(
                        app_state_mutation_macs::table.filter(
                            app_state_mutation_macs::name
                                .eq(&name)
                                .and(app_state_mutation_macs::index_mac.eq_any(chunk))
                                .and(app_state_mutation_macs::device_id.eq(device_id)),
                        ),
                    )
                    .execute(conn)?;
                }
                Ok::<(), diesel::result::Error>(())
            })
            .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(())
        })
        .await
    }

    pub async fn get_app_state_mutation_mac_for_device(
        &self,
        name: &str,
        index_mac: &[u8],
        device_id: i32,
    ) -> Result<Option<Vec<u8>>> {
        let name = name.to_string();
        let index_mac = index_mac.to_vec();
        self.with_conn(move |conn| {
            let res: Option<Vec<u8>> = app_state_mutation_macs::table
                .select(app_state_mutation_macs::value_mac)
                .filter(app_state_mutation_macs::name.eq(&name))
                .filter(app_state_mutation_macs::index_mac.eq(&index_mac))
                .filter(app_state_mutation_macs::device_id.eq(device_id))
                .first(conn)
                .optional()
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(res)
        })
        .await
    }

    pub async fn clear_app_state_mutation_macs_for_device(
        &self,
        name: &str,
        device_id: i32,
    ) -> Result<()> {
        let name = name.to_string();
        self.with_conn(move |conn| {
            diesel::delete(
                app_state_mutation_macs::table
                    .filter(app_state_mutation_macs::name.eq(&name))
                    .filter(app_state_mutation_macs::device_id.eq(device_id)),
            )
            .execute(conn)
            .map(|_| ())
            .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    /// Batch variant of [`Self::get_app_state_mutation_mac_for_device`]: one SQL
    /// `IN (...)` round-trip instead of an N+1 (called per mutation in appstate
    /// sync).
    pub async fn get_app_state_mutation_macs_for_device(
        &self,
        name: &str,
        index_macs: &[[u8; 32]],
        device_id: i32,
    ) -> Result<std::collections::HashMap<[u8; 32], Vec<u8>>> {
        if index_macs.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let name = name.to_string();
        let index_macs = index_macs.to_vec();
        self.with_conn(move |conn| {
            let rows: Vec<(Vec<u8>, Vec<u8>)> = app_state_mutation_macs::table
                .select((
                    app_state_mutation_macs::index_mac,
                    app_state_mutation_macs::value_mac,
                ))
                .filter(app_state_mutation_macs::name.eq(&name))
                .filter(app_state_mutation_macs::index_mac.eq_any(&index_macs))
                .filter(app_state_mutation_macs::device_id.eq(device_id))
                .load(conn)
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            let mut out = std::collections::HashMap::with_capacity(rows.len());
            for (index_mac, value_mac) in rows {
                if let Ok(key) = <[u8; 32]>::try_from(index_mac) {
                    out.insert(key, value_mac);
                }
            }
            Ok(out)
        })
        .await
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl SignalStore for PostgresStore {
    async fn put_identity(&self, address: &str, key: [u8; 32]) -> Result<()> {
        self.put_identity_for_device(address, key, self.device_id)
            .await
    }

    async fn load_identity(&self, address: &str) -> Result<Option<[u8; 32]>> {
        match self
            .load_identity_for_device(address, self.device_id)
            .await?
        {
            Some(v) => Ok(Some(v.try_into().map_err(|v: Vec<u8>| {
                StoreError::Validation(format!(
                    "identity key for '{address}' has invalid length {} (expected 32)",
                    v.len()
                ))
            })?)),
            None => Ok(None),
        }
    }

    async fn delete_identity(&self, address: &str) -> Result<()> {
        self.delete_identity_for_device(address, self.device_id)
            .await
    }

    async fn get_session(&self, address: &str) -> Result<Option<Bytes>> {
        Ok(self
            .get_session_for_device(address, self.device_id)
            .await?
            .map(Bytes::from))
    }

    async fn put_session(&self, address: &str, session: &[u8]) -> Result<()> {
        self.put_session_for_device(address, session, self.device_id)
            .await
    }

    async fn has_session(&self, address: &str) -> Result<bool> {
        let device_id = self.device_id;
        let address = address.to_string();
        self.with_conn(move |conn| {
            let exists = diesel::select(diesel::dsl::exists(
                sessions::table
                    .filter(sessions::address.eq(&address))
                    .filter(sessions::device_id.eq(device_id)),
            ))
            .get_result(conn)
            .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(exists)
        })
        .await
    }

    async fn delete_session(&self, address: &str) -> Result<()> {
        self.delete_session_for_device(address, self.device_id)
            .await
    }

    async fn has_signal_state_for_user(&self, user: &str) -> Result<bool> {
        let device_id = self.device_id;
        // Address is `user@server` (device 0) or `user:dev@server`; `user` is a
        // numeric PN/LID so it carries no LIKE wildcards.
        let pat_at = format!("{user}@%");
        let pat_dev = format!("{user}:%");
        self.with_conn(move |conn| {
            let has_session = diesel::select(diesel::dsl::exists(
                sessions::table
                    .filter(sessions::device_id.eq(device_id))
                    .filter(
                        sessions::address
                            .like(&pat_at)
                            .or(sessions::address.like(&pat_dev)),
                    ),
            ))
            .get_result(conn)
            .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(has_session)
        })
        .await
    }

    async fn store_prekey(&self, id: u32, record: &[u8], uploaded: bool) -> Result<()> {
        let device_id = self.device_id;
        let record = record.to_vec();
        self.with_conn(move |conn| {
            diesel::insert_into(prekeys::table)
                .values((
                    prekeys::id.eq(id as i32),
                    prekeys::key.eq(&record),
                    prekeys::uploaded.eq(uploaded),
                    prekeys::device_id.eq(device_id),
                ))
                .on_conflict((prekeys::id, prekeys::device_id))
                .do_update()
                .set((prekeys::key.eq(&record), prekeys::uploaded.eq(uploaded)))
                .execute(conn)
                .map(|_| ())
                .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn store_prekeys_batch(&self, keys: &[(u32, Bytes)], uploaded: bool) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let device_id = self.device_id;
        let keys: Vec<(i32, Vec<u8>)> = keys
            .iter()
            .map(|(id, record)| (*id as i32, record.to_vec()))
            .collect();
        self.with_conn(move |conn| {
            conn.transaction(|conn| {
                for (id, record) in &keys {
                    diesel::insert_into(prekeys::table)
                        .values((
                            prekeys::id.eq(id),
                            prekeys::key.eq(record),
                            prekeys::uploaded.eq(uploaded),
                            prekeys::device_id.eq(device_id),
                        ))
                        .on_conflict((prekeys::id, prekeys::device_id))
                        .do_update()
                        .set((prekeys::key.eq(record), prekeys::uploaded.eq(uploaded)))
                        .execute(conn)?;
                }
                Ok::<(), diesel::result::Error>(())
            })
            .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(())
        })
        .await
    }

    async fn load_prekey(&self, id: u32) -> Result<Option<Bytes>> {
        let device_id = self.device_id;
        self.with_conn(move |conn| {
            let res: Option<Vec<u8>> = prekeys::table
                .select(prekeys::key)
                .filter(prekeys::id.eq(id as i32))
                .filter(prekeys::device_id.eq(device_id))
                .first(conn)
                .optional()
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(res.map(Bytes::from))
        })
        .await
    }

    async fn load_prekeys_batch(&self, ids: &[u32]) -> Result<Vec<(u32, Bytes)>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let device_id = self.device_id;
        let ids: Vec<i32> = ids.iter().map(|id| *id as i32).collect();
        self.with_conn(move |conn| {
            let rows: Vec<(i32, Vec<u8>)> = prekeys::table
                .select((prekeys::id, prekeys::key))
                .filter(prekeys::id.eq_any(&ids))
                .filter(prekeys::device_id.eq(device_id))
                .load(conn)
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(rows
                .into_iter()
                .map(|(id, key)| (id as u32, Bytes::from(key)))
                .collect())
        })
        .await
    }

    async fn mark_prekeys_uploaded(&self, ids: &[u32]) -> Result<()> {
        let device_id = self.device_id;
        let ids: Vec<i32> = ids.iter().map(|id| *id as i32).collect();
        self.with_conn(move |conn| {
            diesel::update(
                prekeys::table
                    .filter(prekeys::id.eq_any(&ids))
                    .filter(prekeys::device_id.eq(device_id)),
            )
            .set(prekeys::uploaded.eq(true))
            .execute(conn)
            .map(|_| ())
            .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn remove_prekey(&self, id: u32) -> Result<()> {
        let device_id = self.device_id;
        self.with_conn(move |conn| {
            diesel::delete(
                prekeys::table
                    .filter(prekeys::id.eq(id as i32))
                    .filter(prekeys::device_id.eq(device_id)),
            )
            .execute(conn)
            .map(|_| ())
            .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn get_max_prekey_id(&self) -> Result<u32> {
        let device_id = self.device_id;
        self.with_conn(move |conn| {
            use diesel::dsl::max;
            let result: Option<i32> = prekeys::table
                .filter(prekeys::device_id.eq(device_id))
                .select(max(prekeys::id))
                .first(conn)
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(result.unwrap_or(0) as u32)
        })
        .await
    }

    async fn store_signed_prekey(&self, id: u32, record: &[u8]) -> Result<()> {
        let device_id = self.device_id;
        let record = record.to_vec();
        self.with_conn(move |conn| {
            diesel::insert_into(signed_prekeys::table)
                .values((
                    signed_prekeys::id.eq(id as i32),
                    signed_prekeys::record.eq(&record),
                    signed_prekeys::device_id.eq(device_id),
                ))
                .on_conflict((signed_prekeys::id, signed_prekeys::device_id))
                .do_update()
                .set(signed_prekeys::record.eq(&record))
                .execute(conn)
                .map(|_| ())
                .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn load_signed_prekey(&self, id: u32) -> Result<Option<Vec<u8>>> {
        let device_id = self.device_id;
        self.with_conn(move |conn| {
            let res: Option<Vec<u8>> = signed_prekeys::table
                .select(signed_prekeys::record)
                .filter(signed_prekeys::id.eq(id as i32))
                .filter(signed_prekeys::device_id.eq(device_id))
                .first(conn)
                .optional()
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(res)
        })
        .await
    }

    async fn load_all_signed_prekeys(&self) -> Result<Vec<(u32, Vec<u8>)>> {
        let device_id = self.device_id;
        self.with_conn(move |conn| {
            let results: Vec<(i32, Vec<u8>)> = signed_prekeys::table
                .select((signed_prekeys::id, signed_prekeys::record))
                .filter(signed_prekeys::device_id.eq(device_id))
                .load(conn)
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(results
                .into_iter()
                .map(|(id, record)| (id as u32, record))
                .collect())
        })
        .await
    }

    async fn remove_signed_prekey(&self, id: u32) -> Result<()> {
        let device_id = self.device_id;
        self.with_conn(move |conn| {
            diesel::delete(
                signed_prekeys::table
                    .filter(signed_prekeys::id.eq(id as i32))
                    .filter(signed_prekeys::device_id.eq(device_id)),
            )
            .execute(conn)
            .map(|_| ())
            .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn put_sender_key(&self, address: &str, record: &[u8]) -> Result<()> {
        self.put_sender_key_for_device(address, record, self.device_id)
            .await
    }

    async fn put_sender_keys_batch(&self, sender_keys: &[(Arc<str>, Bytes)]) -> Result<()> {
        if sender_keys.is_empty() {
            return Ok(());
        }
        let device_id = self.device_id;
        let keys: Vec<(String, Vec<u8>)> = sender_keys
            .iter()
            .map(|(address, record)| (address.to_string(), record.to_vec()))
            .collect();
        self.with_conn(move |conn| {
            conn.transaction(|conn| {
                for (address, record) in &keys {
                    diesel::insert_into(sender_keys::table)
                        .values((
                            sender_keys::address.eq(address),
                            sender_keys::record.eq(record),
                            sender_keys::device_id.eq(device_id),
                        ))
                        .on_conflict((sender_keys::address, sender_keys::device_id))
                        .do_update()
                        .set(sender_keys::record.eq(record))
                        .execute(conn)?;
                }
                Ok::<(), diesel::result::Error>(())
            })
            .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(())
        })
        .await
    }

    async fn get_sender_key(&self, address: &str) -> Result<Option<Vec<u8>>> {
        self.get_sender_key_for_device(address, self.device_id)
            .await
    }

    async fn delete_sender_key(&self, address: &str) -> Result<()> {
        self.delete_sender_key_for_device(address, self.device_id)
            .await
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl AppSyncStore for PostgresStore {
    async fn get_sync_key(&self, key_id: &[u8]) -> Result<Option<AppStateSyncKey>> {
        self.get_app_state_sync_key_for_device(key_id, self.device_id)
            .await
    }

    async fn set_sync_key(&self, key_id: &[u8], key: AppStateSyncKey) -> Result<()> {
        self.set_app_state_sync_key_for_device(key_id, key, self.device_id)
            .await
    }

    async fn get_version(&self, name: &str) -> Result<HashState> {
        self.get_app_state_version_for_device(name, self.device_id)
            .await
    }

    async fn set_version(&self, name: &str, state: HashState) -> Result<()> {
        self.set_app_state_version_for_device(name, state, self.device_id)
            .await
    }

    async fn put_mutation_macs(
        &self,
        name: &str,
        version: u64,
        mutations: &[AppStateMutationMAC],
    ) -> Result<()> {
        self.put_app_state_mutation_macs_for_device(name, version, mutations, self.device_id)
            .await
    }

    async fn get_mutation_mac(&self, name: &str, index_mac: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_app_state_mutation_mac_for_device(name, index_mac, self.device_id)
            .await
    }

    async fn get_mutation_macs(
        &self,
        name: &str,
        index_macs: &[[u8; 32]],
    ) -> Result<std::collections::HashMap<[u8; 32], Vec<u8>>> {
        self.get_app_state_mutation_macs_for_device(name, index_macs, self.device_id)
            .await
    }

    async fn delete_mutation_macs(&self, name: &str, index_macs: &[Vec<u8>]) -> Result<()> {
        self.delete_app_state_mutation_macs_for_device(name, index_macs, self.device_id)
            .await
    }

    async fn clear_mutation_macs(&self, name: &str) -> Result<()> {
        self.clear_app_state_mutation_macs_for_device(name, self.device_id)
            .await
    }

    async fn get_latest_sync_key_id(&self) -> Result<Option<Vec<u8>>> {
        self.get_latest_app_state_sync_key_id_for_device(self.device_id)
            .await
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl ProtocolStore for PostgresStore {
    async fn get_sender_key_devices(&self, group_jid: &str) -> Result<Vec<(String, bool)>> {
        let device_id = self.device_id;
        let group_jid = group_jid.to_string();
        self.with_conn(move |conn| {
            let rows: Vec<(String, i32)> = sender_key_devices::table
                .select((sender_key_devices::device_jid, sender_key_devices::has_key))
                .filter(sender_key_devices::group_jid.eq(&group_jid))
                .filter(sender_key_devices::device_id.eq(device_id))
                .load(conn)
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(rows
                .into_iter()
                .map(|(jid, has_key)| (jid, has_key != 0))
                .collect())
        })
        .await
    }

    async fn set_sender_key_status(&self, group_jid: &str, entries: &[(&str, bool)]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let device_id = self.device_id;
        let group_jid = group_jid.to_string();
        let owned_entries: Vec<(String, bool)> = entries
            .iter()
            .map(|(jid, has_key)| (jid.to_string(), *has_key))
            .collect();
        let now = wacore::time::now_secs();
        self.with_conn(move |conn| {
            conn.transaction(|conn| {
                let values: Vec<_> = owned_entries
                    .iter()
                    .map(|(device_jid, has_key)| {
                        (
                            sender_key_devices::group_jid.eq(&group_jid),
                            sender_key_devices::device_jid.eq(device_jid),
                            sender_key_devices::has_key.eq(i32::from(*has_key)),
                            sender_key_devices::device_id.eq(device_id),
                            sender_key_devices::updated_at.eq(now),
                        )
                    })
                    .collect();

                for chunk in values.chunks(190) {
                    diesel::insert_into(sender_key_devices::table)
                        .values(chunk)
                        .on_conflict((
                            sender_key_devices::group_jid,
                            sender_key_devices::device_jid,
                            sender_key_devices::device_id,
                        ))
                        .do_update()
                        .set((
                            sender_key_devices::has_key.eq(excluded(sender_key_devices::has_key)),
                            sender_key_devices::updated_at.eq(now),
                        ))
                        .execute(conn)?;
                }
                Ok::<(), diesel::result::Error>(())
            })
            .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(())
        })
        .await
    }

    async fn clear_sender_key_devices(&self, group_jid: &str) -> Result<()> {
        let device_id = self.device_id;
        let group_jid = group_jid.to_string();
        self.with_conn(move |conn| {
            diesel::delete(
                sender_key_devices::table
                    .filter(sender_key_devices::group_jid.eq(&group_jid))
                    .filter(sender_key_devices::device_id.eq(device_id)),
            )
            .execute(conn)
            .map(|_| ())
            .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn delete_sender_key_device_rows(&self, device_jids: &[&str]) -> Result<()> {
        if device_jids.is_empty() {
            return Ok(());
        }
        let device_id = self.device_id;
        let jids: Vec<String> = device_jids.iter().map(|j| j.to_string()).collect();
        self.with_conn(move |conn| {
            diesel::delete(
                sender_key_devices::table
                    .filter(sender_key_devices::device_jid.eq_any(&jids))
                    .filter(sender_key_devices::device_id.eq(device_id)),
            )
            .execute(conn)
            .map(|_| ())
            .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn clear_all_sender_key_devices(&self) -> Result<()> {
        let device_id = self.device_id;
        self.with_conn(move |conn| {
            diesel::delete(
                sender_key_devices::table.filter(sender_key_devices::device_id.eq(device_id)),
            )
            .execute(conn)
            .map(|_| ())
            .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn get_lid_mapping(&self, lid: &str) -> Result<Option<LidPnMappingEntry>> {
        let device_id = self.device_id;
        let lid = lid.to_string();
        self.with_conn(move |conn| {
            let row: Option<(String, String, i64, String, i64)> = lid_pn_mapping::table
                .select((
                    lid_pn_mapping::lid,
                    lid_pn_mapping::phone_number,
                    lid_pn_mapping::created_at,
                    lid_pn_mapping::learning_source,
                    lid_pn_mapping::updated_at,
                ))
                .filter(lid_pn_mapping::lid.eq(&lid))
                .filter(lid_pn_mapping::device_id.eq(device_id))
                .first(conn)
                .optional()
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(row.map(
                |(lid, phone_number, created_at, learning_source, updated_at)| LidPnMappingEntry {
                    lid,
                    phone_number,
                    created_at,
                    updated_at,
                    learning_source,
                },
            ))
        })
        .await
    }

    async fn get_pn_mapping(&self, phone: &str) -> Result<Option<LidPnMappingEntry>> {
        let device_id = self.device_id;
        let phone = phone.to_string();
        self.with_conn(move |conn| {
            let row: Option<(String, String, i64, String, i64)> = lid_pn_mapping::table
                .select((
                    lid_pn_mapping::lid,
                    lid_pn_mapping::phone_number,
                    lid_pn_mapping::created_at,
                    lid_pn_mapping::learning_source,
                    lid_pn_mapping::updated_at,
                ))
                .filter(lid_pn_mapping::phone_number.eq(&phone))
                .filter(lid_pn_mapping::device_id.eq(device_id))
                .order(lid_pn_mapping::updated_at.desc())
                .first(conn)
                .optional()
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(row.map(
                |(lid, phone_number, created_at, learning_source, updated_at)| LidPnMappingEntry {
                    lid,
                    phone_number,
                    created_at,
                    updated_at,
                    learning_source,
                },
            ))
        })
        .await
    }

    async fn put_lid_mapping(&self, entry: &LidPnMappingEntry) -> Result<()> {
        let device_id = self.device_id;
        let entry = entry.clone();
        self.with_conn(move |conn| {
            diesel::insert_into(lid_pn_mapping::table)
                .values((
                    lid_pn_mapping::lid.eq(&entry.lid),
                    lid_pn_mapping::phone_number.eq(&entry.phone_number),
                    lid_pn_mapping::created_at.eq(entry.created_at),
                    lid_pn_mapping::learning_source.eq(&entry.learning_source),
                    lid_pn_mapping::updated_at.eq(entry.updated_at),
                    lid_pn_mapping::device_id.eq(device_id),
                ))
                .on_conflict((lid_pn_mapping::lid, lid_pn_mapping::device_id))
                .do_update()
                .set((
                    lid_pn_mapping::phone_number.eq(&entry.phone_number),
                    lid_pn_mapping::learning_source.eq(&entry.learning_source),
                    lid_pn_mapping::updated_at.eq(entry.updated_at),
                ))
                .execute(conn)
                .map(|_| ())
                .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn get_all_lid_mappings(&self) -> Result<Vec<LidPnMappingEntry>> {
        let device_id = self.device_id;
        self.with_conn(move |conn| {
            let rows: Vec<(String, String, i64, String, i64)> = lid_pn_mapping::table
                .select((
                    lid_pn_mapping::lid,
                    lid_pn_mapping::phone_number,
                    lid_pn_mapping::created_at,
                    lid_pn_mapping::learning_source,
                    lid_pn_mapping::updated_at,
                ))
                .filter(lid_pn_mapping::device_id.eq(device_id))
                .load(conn)
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(rows
                .into_iter()
                .map(
                    |(lid, phone_number, created_at, learning_source, updated_at)| {
                        LidPnMappingEntry {
                            lid,
                            phone_number,
                            created_at,
                            updated_at,
                            learning_source,
                        }
                    },
                )
                .collect())
        })
        .await
    }

    async fn save_base_key(&self, address: &str, message_id: &str, base_key: &[u8]) -> Result<()> {
        let device_id = self.device_id;
        let address = address.to_string();
        let message_id = message_id.to_string();
        let base_key = base_key.to_vec();
        self.with_conn(move |conn| {
            diesel::insert_into(base_keys::table)
                .values((
                    base_keys::address.eq(&address),
                    base_keys::message_id.eq(&message_id),
                    base_keys::base_key.eq(&base_key),
                    base_keys::device_id.eq(device_id),
                ))
                .on_conflict((
                    base_keys::address,
                    base_keys::message_id,
                    base_keys::device_id,
                ))
                .do_update()
                .set(base_keys::base_key.eq(&base_key))
                .execute(conn)
                .map(|_| ())
                .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn has_same_base_key(
        &self,
        address: &str,
        message_id: &str,
        current_base_key: &[u8],
    ) -> Result<bool> {
        let device_id = self.device_id;
        let address = address.to_string();
        let message_id = message_id.to_string();
        let current_base_key = current_base_key.to_vec();
        self.with_conn(move |conn| {
            let stored_key: Option<Vec<u8>> = base_keys::table
                .select(base_keys::base_key)
                .filter(base_keys::address.eq(&address))
                .filter(base_keys::message_id.eq(&message_id))
                .filter(base_keys::device_id.eq(device_id))
                .first(conn)
                .optional()
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(stored_key.as_ref() == Some(&current_base_key))
        })
        .await
    }

    async fn delete_base_key(&self, address: &str, message_id: &str) -> Result<()> {
        let device_id = self.device_id;
        let address = address.to_string();
        let message_id = message_id.to_string();
        self.with_conn(move |conn| {
            diesel::delete(
                base_keys::table
                    .filter(base_keys::address.eq(&address))
                    .filter(base_keys::message_id.eq(&message_id))
                    .filter(base_keys::device_id.eq(device_id)),
            )
            .execute(conn)
            .map(|_| ())
            .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn update_device_list(&self, record: DeviceListRecord) -> Result<()> {
        let device_id = self.device_id;
        let devices_json = serde_json::to_string(&record.devices)
            .map_err(|e| StoreError::Serialization(Box::new(e)))?;
        let now = wacore::time::now_secs().max(0) as i32;
        self.with_conn(move |conn| {
            diesel::insert_into(device_registry::table)
                .values((
                    device_registry::user_id.eq(&record.user),
                    device_registry::devices_json.eq(&devices_json),
                    device_registry::timestamp.eq(record.timestamp as i32),
                    device_registry::phash.eq(&record.phash),
                    device_registry::device_id.eq(device_id),
                    device_registry::updated_at.eq(now),
                    device_registry::raw_id.eq(record.raw_id.map(|v| v as i32)),
                ))
                .on_conflict((device_registry::user_id, device_registry::device_id))
                .do_update()
                .set((
                    device_registry::devices_json.eq(&devices_json),
                    device_registry::timestamp.eq(record.timestamp as i32),
                    device_registry::phash.eq(&record.phash),
                    device_registry::updated_at.eq(now),
                    device_registry::raw_id.eq(record.raw_id.map(|v| v as i32)),
                ))
                .execute(conn)
                .map(|_| ())
                .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn update_device_lists(&self, records: Vec<DeviceListRecord>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let device_id = self.device_id;
        let now = wacore::time::now_secs().max(0) as i32;
        // Serialize before the closure so any serde error surfaces eagerly
        // rather than inside the spawned task.
        let records: Vec<(String, String, i32, Option<String>, Option<i32>)> = records
            .into_iter()
            .map(|record| {
                let devices_json = serde_json::to_string(&record.devices)
                    .map_err(|e| StoreError::Serialization(Box::new(e)))?;
                Ok((
                    record.user,
                    devices_json,
                    record.timestamp as i32,
                    record.phash,
                    record.raw_id.map(|v| v as i32),
                ))
            })
            .collect::<Result<_>>()?;
        self.with_conn(move |conn| {
            conn.transaction(|conn| {
                for (user, devices_json, timestamp, phash, raw_id) in &records {
                    diesel::insert_into(device_registry::table)
                        .values((
                            device_registry::user_id.eq(user),
                            device_registry::devices_json.eq(devices_json),
                            device_registry::timestamp.eq(timestamp),
                            device_registry::phash.eq(phash),
                            device_registry::device_id.eq(device_id),
                            device_registry::updated_at.eq(now),
                            device_registry::raw_id.eq(raw_id),
                        ))
                        .on_conflict((device_registry::user_id, device_registry::device_id))
                        .do_update()
                        .set((
                            device_registry::devices_json.eq(devices_json),
                            device_registry::timestamp.eq(timestamp),
                            device_registry::phash.eq(phash),
                            device_registry::updated_at.eq(now),
                            device_registry::raw_id.eq(raw_id),
                        ))
                        .execute(conn)?;
                }
                Ok::<(), diesel::result::Error>(())
            })
            .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(())
        })
        .await
    }

    async fn get_devices(&self, user: &str) -> Result<Option<DeviceListRecord>> {
        let device_id = self.device_id;
        let user = user.to_string();
        self.with_conn(move |conn| {
            let row: Option<(String, String, i32, Option<String>, Option<i32>)> =
                device_registry::table
                    .select((
                        device_registry::user_id,
                        device_registry::devices_json,
                        device_registry::timestamp,
                        device_registry::phash,
                        device_registry::raw_id,
                    ))
                    .filter(device_registry::user_id.eq(&user))
                    .filter(device_registry::device_id.eq(device_id))
                    .first(conn)
                    .optional()
                    .map_err(|e| StoreError::Database(Box::new(e)))?;
            match row {
                Some((user, devices_json, timestamp, phash, raw_id)) => {
                    let devices: Vec<DeviceInfo> = serde_json::from_str(&devices_json)
                        .map_err(|e| StoreError::Serialization(Box::new(e)))?;
                    Ok(Some(DeviceListRecord {
                        user,
                        devices,
                        timestamp: timestamp as i64,
                        phash,
                        raw_id: raw_id.map(|v| v as u32),
                    }))
                }
                None => Ok(None),
            }
        })
        .await
    }

    async fn delete_devices(&self, user: &str) -> Result<()> {
        let device_id = self.device_id;
        let user = user.to_string();
        self.with_conn(move |conn| {
            diesel::delete(
                device_registry::table
                    .filter(device_registry::user_id.eq(&user))
                    .filter(device_registry::device_id.eq(device_id)),
            )
            .execute(conn)
            .map(|_| ())
            .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn get_group_metadata(&self, group_jid: &str) -> Result<Option<Vec<u8>>> {
        let device_id = self.device_id;
        let group_jid = group_jid.to_string();
        self.with_conn(move |conn| {
            let res: Option<Vec<u8>> = group_metadata::table
                .select(group_metadata::info)
                .filter(group_metadata::group_jid.eq(&group_jid))
                .filter(group_metadata::device_id.eq(device_id))
                .first(conn)
                .optional()
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(res)
        })
        .await
    }

    async fn put_group_metadata(&self, group_jid: &str, blob: &[u8]) -> Result<()> {
        let device_id = self.device_id;
        let group_jid = group_jid.to_string();
        let blob = blob.to_vec();
        let now = wacore::time::now_secs();
        self.with_conn(move |conn| {
            diesel::insert_into(group_metadata::table)
                .values((
                    group_metadata::group_jid.eq(&group_jid),
                    group_metadata::info.eq(&blob),
                    group_metadata::device_id.eq(device_id),
                    group_metadata::updated_at.eq(now),
                ))
                .on_conflict((group_metadata::group_jid, group_metadata::device_id))
                .do_update()
                .set((
                    group_metadata::info.eq(&blob),
                    group_metadata::updated_at.eq(now),
                ))
                .execute(conn)
                .map(|_| ())
                .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn delete_group_metadata(&self, group_jid: &str) -> Result<()> {
        let device_id = self.device_id;
        let group_jid = group_jid.to_string();
        self.with_conn(move |conn| {
            diesel::delete(
                group_metadata::table
                    .filter(group_metadata::group_jid.eq(&group_jid))
                    .filter(group_metadata::device_id.eq(device_id)),
            )
            .execute(conn)
            .map(|_| ())
            .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn get_tc_token(&self, jid: &str) -> Result<Option<TcTokenEntry>> {
        let device_id = self.device_id;
        let jid = jid.to_string();
        self.with_conn(move |conn| {
            let row: Option<(Vec<u8>, i64, Option<i64>)> = tc_tokens::table
                .select((
                    tc_tokens::token,
                    tc_tokens::token_timestamp,
                    tc_tokens::sender_timestamp,
                ))
                .filter(tc_tokens::jid.eq(&jid))
                .filter(tc_tokens::device_id.eq(device_id))
                .first(conn)
                .optional()
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(
                row.map(|(token, token_timestamp, sender_timestamp)| TcTokenEntry {
                    token,
                    token_timestamp,
                    sender_timestamp,
                }),
            )
        })
        .await
    }

    async fn put_tc_token(&self, jid: &str, entry: &TcTokenEntry) -> Result<()> {
        let device_id = self.device_id;
        let jid = jid.to_string();
        let entry = entry.clone();
        let now = wacore::time::now_secs();
        self.with_conn(move |conn| {
            diesel::insert_into(tc_tokens::table)
                .values((
                    tc_tokens::jid.eq(&jid),
                    tc_tokens::token.eq(&entry.token),
                    tc_tokens::token_timestamp.eq(entry.token_timestamp),
                    tc_tokens::sender_timestamp.eq(entry.sender_timestamp),
                    tc_tokens::device_id.eq(device_id),
                    tc_tokens::updated_at.eq(now),
                ))
                .on_conflict((tc_tokens::jid, tc_tokens::device_id))
                .do_update()
                .set((
                    tc_tokens::token.eq(&entry.token),
                    tc_tokens::token_timestamp.eq(entry.token_timestamp),
                    tc_tokens::sender_timestamp.eq(entry.sender_timestamp),
                    tc_tokens::updated_at.eq(now),
                ))
                .execute(conn)
                .map(|_| ())
                .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn delete_tc_token(&self, jid: &str) -> Result<()> {
        let device_id = self.device_id;
        let jid = jid.to_string();
        self.with_conn(move |conn| {
            diesel::delete(
                tc_tokens::table
                    .filter(tc_tokens::jid.eq(&jid))
                    .filter(tc_tokens::device_id.eq(device_id)),
            )
            .execute(conn)
            .map(|_| ())
            .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn get_all_tc_token_jids(&self) -> Result<Vec<String>> {
        let device_id = self.device_id;
        self.with_conn(move |conn| {
            let jids: Vec<String> = tc_tokens::table
                .select(tc_tokens::jid)
                .filter(tc_tokens::device_id.eq(device_id))
                .load(conn)
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(jids)
        })
        .await
    }

    async fn delete_expired_tc_tokens(&self, token_cutoff: i64, sender_cutoff: i64) -> Result<u32> {
        let device_id = self.device_id;
        self.with_conn(move |conn| {
            // A row is removed only when its received token is expired-or-absent
            // AND its sender bucket is expired-or-absent, so recent sender state
            // is never dropped just because the received token expired.
            let deleted = diesel::delete(
                tc_tokens::table
                    .filter(tc_tokens::device_id.eq(device_id))
                    .filter(
                        tc_tokens::token_timestamp
                            .lt(token_cutoff)
                            .or(tc_tokens::token_timestamp.eq(0)),
                    )
                    .filter(
                        tc_tokens::sender_timestamp
                            .lt(sender_cutoff)
                            .or(tc_tokens::sender_timestamp.is_null()),
                    ),
            )
            .execute(conn)
            .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(deleted as u32)
        })
        .await
    }

    async fn store_sent_message(
        &self,
        chat_jid: &str,
        message_id: &str,
        payload: &[u8],
    ) -> Result<()> {
        let device_id = self.device_id;
        let chat_jid = chat_jid.to_string();
        let message_id = message_id.to_string();
        let payload: Arc<Vec<u8>> = Arc::new(payload.to_vec());
        self.with_conn(move |conn| {
            diesel::insert_into(sent_messages::table)
                .values((
                    sent_messages::chat_jid.eq(&chat_jid),
                    sent_messages::message_id.eq(&message_id),
                    sent_messages::payload.eq(payload.as_slice()),
                    sent_messages::device_id.eq(device_id),
                ))
                .on_conflict((
                    sent_messages::chat_jid,
                    sent_messages::message_id,
                    sent_messages::device_id,
                ))
                .do_update()
                .set(sent_messages::payload.eq(payload.as_slice()))
                .execute(conn)
                .map(|_| ())
                .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn take_sent_message(&self, chat_jid: &str, message_id: &str) -> Result<Option<Vec<u8>>> {
        let device_id = self.device_id;
        let chat_jid = chat_jid.to_string();
        let message_id = message_id.to_string();
        self.with_conn(move |conn| {
            conn.transaction(|conn| {
                let row: Option<Vec<u8>> = sent_messages::table
                    .select(sent_messages::payload)
                    .filter(sent_messages::chat_jid.eq(&chat_jid))
                    .filter(sent_messages::message_id.eq(&message_id))
                    .filter(sent_messages::device_id.eq(device_id))
                    .first(conn)
                    .optional()?;
                if row.is_some() {
                    diesel::delete(
                        sent_messages::table
                            .filter(sent_messages::chat_jid.eq(&chat_jid))
                            .filter(sent_messages::message_id.eq(&message_id))
                            .filter(sent_messages::device_id.eq(device_id)),
                    )
                    .execute(conn)?;
                }
                Ok::<Option<Vec<u8>>, diesel::result::Error>(row)
            })
            .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn delete_expired_sent_messages(&self, cutoff_timestamp: i64) -> Result<u32> {
        let device_id = self.device_id;
        self.with_conn(move |conn| {
            let deleted = diesel::delete(
                sent_messages::table
                    .filter(sent_messages::created_at.lt(cutoff_timestamp))
                    .filter(sent_messages::device_id.eq(device_id)),
            )
            .execute(conn)
            .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(deleted as u32)
        })
        .await
    }

    async fn store_pending_inbound(
        &self,
        chat: &str,
        sender: &str,
        id: &str,
        message: &[u8],
    ) -> Result<()> {
        let device_id = self.device_id;
        let chat = chat.to_string();
        let sender = sender.to_string();
        let id = id.to_string();
        let message: Arc<Vec<u8>> = Arc::new(message.to_vec());
        let now = wacore::time::now_secs();
        self.with_conn(move |conn| {
            diesel::insert_into(pending_inbound_messages::table)
                .values((
                    pending_inbound_messages::chat.eq(&chat),
                    pending_inbound_messages::sender.eq(&sender),
                    pending_inbound_messages::id.eq(&id),
                    pending_inbound_messages::message.eq(message.as_slice()),
                    pending_inbound_messages::device_id.eq(device_id),
                    pending_inbound_messages::inserted_at.eq(now),
                ))
                .on_conflict((
                    pending_inbound_messages::chat,
                    pending_inbound_messages::sender,
                    pending_inbound_messages::id,
                    pending_inbound_messages::device_id,
                ))
                .do_update()
                .set(pending_inbound_messages::message.eq(message.as_slice()))
                .execute(conn)
                .map(|_| ())
                .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn get_pending_inbound(
        &self,
        chat: &str,
        sender: &str,
        id: &str,
    ) -> Result<Option<Vec<u8>>> {
        let device_id = self.device_id;
        let chat = chat.to_string();
        let sender = sender.to_string();
        let id = id.to_string();
        self.with_conn(move |conn| {
            let row: Option<Vec<u8>> = pending_inbound_messages::table
                .select(pending_inbound_messages::message)
                .filter(pending_inbound_messages::chat.eq(&chat))
                .filter(pending_inbound_messages::sender.eq(&sender))
                .filter(pending_inbound_messages::id.eq(&id))
                .filter(pending_inbound_messages::device_id.eq(device_id))
                .first(conn)
                .optional()
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(row)
        })
        .await
    }

    async fn delete_pending_inbound(&self, chat: &str, sender: &str, id: &str) -> Result<()> {
        let device_id = self.device_id;
        let chat = chat.to_string();
        let sender = sender.to_string();
        let id = id.to_string();
        self.with_conn(move |conn| {
            diesel::delete(
                pending_inbound_messages::table
                    .filter(pending_inbound_messages::chat.eq(&chat))
                    .filter(pending_inbound_messages::sender.eq(&sender))
                    .filter(pending_inbound_messages::id.eq(&id))
                    .filter(pending_inbound_messages::device_id.eq(device_id)),
            )
            .execute(conn)
            .map(|_| ())
            .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn delete_expired_pending_inbound(&self, cutoff_timestamp: i64) -> Result<u32> {
        let device_id = self.device_id;
        self.with_conn(move |conn| {
            let deleted = diesel::delete(
                pending_inbound_messages::table
                    .filter(pending_inbound_messages::inserted_at.lt(cutoff_timestamp))
                    .filter(pending_inbound_messages::device_id.eq(device_id)),
            )
            .execute(conn)
            .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(deleted as u32)
        })
        .await
    }

    async fn store_pending_inbound_batch(&self, rows: &[PendingInboundRow<'_>]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let device_id = self.device_id;
        let rows: Vec<(String, String, String, Vec<u8>)> = rows
            .iter()
            .map(|r| {
                (
                    r.chat.to_string(),
                    r.sender.to_string(),
                    r.id.to_string(),
                    r.message.to_vec(),
                )
            })
            .collect();
        let now = wacore::time::now_secs();
        self.with_conn(move |conn| {
            conn.transaction(|conn| {
                for (chat, sender, id, message) in &rows {
                    diesel::insert_into(pending_inbound_messages::table)
                        .values((
                            pending_inbound_messages::chat.eq(chat),
                            pending_inbound_messages::sender.eq(sender),
                            pending_inbound_messages::id.eq(id),
                            pending_inbound_messages::message.eq(message),
                            pending_inbound_messages::device_id.eq(device_id),
                            pending_inbound_messages::inserted_at.eq(now),
                        ))
                        .on_conflict((
                            pending_inbound_messages::chat,
                            pending_inbound_messages::sender,
                            pending_inbound_messages::id,
                            pending_inbound_messages::device_id,
                        ))
                        .do_update()
                        .set(pending_inbound_messages::message.eq(message))
                        .execute(conn)?;
                }
                Ok::<(), diesel::result::Error>(())
            })
            .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(())
        })
        .await
    }

    async fn delete_pending_inbound_batch(&self, keys: &[PendingInboundKey<'_>]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let device_id = self.device_id;
        let keys: Vec<(String, String, String)> = keys
            .iter()
            .map(|k| (k.chat.to_string(), k.sender.to_string(), k.id.to_string()))
            .collect();
        self.with_conn(move |conn| {
            conn.transaction(|conn| {
                for (chat, sender, id) in &keys {
                    diesel::delete(
                        pending_inbound_messages::table
                            .filter(pending_inbound_messages::chat.eq(chat))
                            .filter(pending_inbound_messages::sender.eq(sender))
                            .filter(pending_inbound_messages::id.eq(id))
                            .filter(pending_inbound_messages::device_id.eq(device_id)),
                    )
                    .execute(conn)?;
                }
                Ok::<(), diesel::result::Error>(())
            })
            .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(())
        })
        .await
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl MsgSecretStore for PostgresStore {
    async fn put_msg_secrets(&self, entries: Vec<MsgSecretEntry>) -> Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }
        let device_id = self.device_id;
        let entries = Arc::new(entries);
        let now = wacore::time::now_secs();
        self.with_conn(move |conn| {
            conn.transaction(|conn| {
                let mut stored = 0usize;
                for chunk in entries.chunks(500) {
                    let records: Vec<_> = chunk
                        .iter()
                        .map(|entry| {
                            (
                                msg_secrets::chat.eq(entry.chat.as_ref()),
                                msg_secrets::sender.eq(entry.sender.as_ref()),
                                msg_secrets::msg_id.eq(entry.msg_id.as_ref()),
                                msg_secrets::secret.eq(entry.secret.as_ref()),
                                msg_secrets::device_id.eq(device_id),
                                msg_secrets::created_at.eq(now),
                                msg_secrets::expires_at.eq(entry.expires_at),
                                msg_secrets::message_ts.eq(entry.message_ts),
                            )
                        })
                        .collect();
                    stored += diesel::insert_into(msg_secrets::table)
                        .values(&records)
                        .on_conflict((
                            msg_secrets::chat,
                            msg_secrets::sender,
                            msg_secrets::msg_id,
                            msg_secrets::device_id,
                        ))
                        .do_update()
                        .set((
                            msg_secrets::secret.eq(excluded(msg_secrets::secret)),
                            msg_secrets::created_at.eq(now),
                            // Keep the later deadline; 0 (never) wins. Mirrors
                            // merge_msg_secret_expiry so a redelivery or edit
                            // re-persist never shortens an existing window.
                            msg_secrets::expires_at.eq(
                                diesel::dsl::sql::<diesel::sql_types::BigInt>(
                                    "CASE WHEN msg_secrets.expires_at = 0 \
                                 OR excluded.expires_at = 0 THEN 0 \
                                 ELSE GREATEST(msg_secrets.expires_at, excluded.expires_at) END",
                                ),
                            ),
                            // Parent event time is immutable; keep the known
                            // (non-zero / later) value across redeliveries.
                            msg_secrets::message_ts.eq(
                                diesel::dsl::sql::<diesel::sql_types::BigInt>(
                                    "GREATEST(msg_secrets.message_ts, excluded.message_ts)",
                                ),
                            ),
                        ))
                        .execute(conn)?;
                }
                Ok::<usize, diesel::result::Error>(stored)
            })
            .map_err(|e| StoreError::Database(Box::new(e)))
        })
        .await
    }

    async fn get_msg_secret(
        &self,
        chat: &str,
        sender: &str,
        msg_id: &str,
    ) -> Result<Option<Vec<u8>>> {
        Ok(self
            .get_msg_secret_with_ts(chat, sender, msg_id)
            .await?
            .map(|(secret, _)| secret))
    }

    async fn get_msg_secret_with_ts(
        &self,
        chat: &str,
        sender: &str,
        msg_id: &str,
    ) -> Result<Option<(Vec<u8>, i64)>> {
        let device_id = self.device_id;
        let chat = chat.to_string();
        let sender = sender.to_string();
        let msg_id = msg_id.to_string();
        self.with_conn(move |conn| {
            let row: Option<(Vec<u8>, i64)> = msg_secrets::table
                .select((msg_secrets::secret, msg_secrets::message_ts))
                .filter(msg_secrets::chat.eq(&chat))
                .filter(msg_secrets::sender.eq(&sender))
                .filter(msg_secrets::msg_id.eq(&msg_id))
                .filter(msg_secrets::device_id.eq(device_id))
                .first(conn)
                .optional()
                .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(row)
        })
        .await
    }

    async fn delete_expired_msg_secrets(&self, cutoff_timestamp: i64) -> Result<u32> {
        let device_id = self.device_id;
        self.with_conn(move |conn| {
            // Rows with expires_at = 0 never expire; only delete passed deadlines.
            let deleted = diesel::delete(
                msg_secrets::table
                    .filter(msg_secrets::expires_at.ne(0))
                    .filter(msg_secrets::expires_at.le(cutoff_timestamp))
                    .filter(msg_secrets::device_id.eq(device_id)),
            )
            .execute(conn)
            .map_err(|e| StoreError::Database(Box::new(e)))?;
            Ok(deleted as u32)
        })
        .await
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl DeviceStore for PostgresStore {
    async fn save(&self, device: &CoreDevice) -> Result<()> {
        self.save_device_data_for_device(self.device_id, device)
            .await
    }

    async fn load(&self) -> Result<Option<CoreDevice>> {
        self.load_device_data_for_device(self.device_id).await
    }

    async fn exists(&self) -> Result<bool> {
        self.device_exists(self.device_id).await
    }

    async fn create(&self) -> Result<i32> {
        self.create_new_device().await
    }

    /// PG data lives in a remote server, not this process's memory.
    async fn resource_report(&self) -> wacore::stats::StorageResourceReport {
        wacore::stats::StorageResourceReport {
            memory_bytes: Some(0),
            ..Default::default()
        }
    }
}
