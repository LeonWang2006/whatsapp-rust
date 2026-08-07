use crate::schema::*;
use async_trait::async_trait;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::upsert::excluded;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use std::sync::Arc;
use wacore::appstate::hash::HashState;
use wacore::appstate::processor::AppStateMutationMAC;
use wacore::libsignal::protocol::{KeyPair, PrivateKey, PublicKey};
use wacore::store::Device as CoreDevice;
use wacore::store::error::{Result, StoreError};
use wacore::store::traits::*;

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
}

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
    pub async fn new(database_url: &str) -> std::result::Result<Self, StoreError> {
        Self::new_for_device(database_url, 1).await
    }

    pub async fn new_for_device(
        database_url: &str,
        device_id: i32,
    ) -> std::result::Result<Self, StoreError> {
        let manager = ConnectionManager::<PgConnection>::new(database_url);
        let pool = Pool::builder()
            .max_size(8)
            .connection_customizer(Box::new(ConnectionOptions))
            .build(manager)
            .map_err(|e| StoreError::Connection(e.to_string()))?;

        Ok(Self { pool, device_id })
    }

    /// Run pending database migrations. Call once at process startup before
    /// creating any session. Concurrent calls across pods are safe because PG
    /// serializes DDL via the `__diesel_schema_migrations` table lock, but
    /// within one process you should only invoke this once.
    pub async fn run_migrations(&self) -> std::result::Result<(), StoreError> {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || -> std::result::Result<(), StoreError> {
            let mut conn = pool
                .get()
                .map_err(|e| StoreError::Connection(e.to_string()))?;
            conn.run_pending_migrations(MIGRATIONS)
                .map_err(|e| StoreError::Migration(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| StoreError::Database(e.to_string()))??;
        Ok(())
    }

    /// Connect, run migrations, and return a store for `device_id` = 1.
    /// Convenience for single-account CLIs; multi-session servers should call
    /// `new_for_device` + `run_migrations` separately.
    pub async fn connect(database_url: &str) -> std::result::Result<Self, StoreError> {
        let store = Self::new(database_url).await?;
        store.run_migrations().await?;
        Ok(store)
    }

    pub fn device_id(&self) -> i32 {
        self.device_id
    }

    /// Run a blocking DB operation on a pooled connection. PG MVCC lets concurrent
    /// transactions proceed without the process-wide semaphore SQLite needed.
    async fn with_conn<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut PgConnection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || -> Result<T> {
            let mut conn = pool
                .get()
                .map_err(|e| StoreError::Connection(e.to_string()))?;
            f(&mut conn)
        })
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?
    }

    fn serialize_keypair(key_pair: &KeyPair) -> Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(64);
        bytes.extend_from_slice(key_pair.private_key.serialize());
        bytes.extend_from_slice(key_pair.public_key.public_key_bytes());
        Ok(bytes)
    }

    fn deserialize_keypair(bytes: &[u8]) -> Result<KeyPair> {
        if bytes.len() != 64 {
            return Err(StoreError::Serialization(format!(
                "Invalid KeyPair length: {}",
                bytes.len()
            )));
        }
        let private_key = PrivateKey::deserialize(&bytes[0..32])
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let public_key = PublicKey::from_djb_public_key_bytes(&bytes[32..64])
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        Ok(KeyPair::new(public_key, private_key))
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
            .map(wacore::store::device::account_serde::to_bytes);
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
        let nct_salt = device_data.nct_salt.clone();
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
                ))
                .on_conflict(device::id)
                .do_update()
                .set((
                    device::lid.eq(&new_lid),
                    device::pn.eq(&new_pn),
                    device::registration_id.eq(registration_id),
                    device::noise_key.eq(&noise_key_data),
                    device::identity_key.eq(&identity_key_data),
                    device::signed_pre_key.eq(&signed_pre_key_data),
                    device::signed_pre_key_id.eq(signed_pre_key_id),
                    device::signed_pre_key_signature.eq(&signed_pre_key_signature[..]),
                    device::adv_secret_key.eq(&adv_secret_key[..]),
                    device::account.eq(account_data),
                    device::push_name.eq(&push_name),
                    device::app_version_primary.eq(app_version_primary),
                    device::app_version_secondary.eq(app_version_secondary),
                    device::app_version_tertiary.eq(app_version_tertiary),
                    device::app_version_last_fetched_ms.eq(app_version_last_fetched_ms),
                    device::edge_routing_info.eq(edge_routing_info),
                    device::props_hash.eq(props_hash),
                    device::next_pre_key_id.eq(next_pre_key_id),
                    device::nct_salt.eq(nct_salt),
                ))
                .execute(conn)
                .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
        })
        .await
    }

    pub async fn create_new_device(&self) -> Result<i32> {
        let new_device = wacore::store::Device::new();

        let noise_key_data = {
            let mut bytes = Vec::with_capacity(64);
            bytes.extend_from_slice(new_device.noise_key.private_key.serialize());
            bytes.extend_from_slice(new_device.noise_key.public_key.public_key_bytes());
            bytes
        };
        let identity_key_data = {
            let mut bytes = Vec::with_capacity(64);
            bytes.extend_from_slice(new_device.identity_key.private_key.serialize());
            bytes.extend_from_slice(new_device.identity_key.public_key.public_key_bytes());
            bytes
        };
        let signed_pre_key_data = {
            let mut bytes = Vec::with_capacity(64);
            bytes.extend_from_slice(new_device.signed_pre_key.private_key.serialize());
            bytes.extend_from_slice(new_device.signed_pre_key.public_key.public_key_bytes());
            bytes
        };

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
                ))
                .returning(device::id)
                .get_result::<i32>(conn)
                .map_err(|e| StoreError::Database(e.to_string()))
        })
        .await
    }

    pub async fn device_exists(&self, device_id: i32) -> Result<bool> {
        self.with_conn(move |conn| {
            let count: i64 = device::table
                .filter(device::id.eq(device_id))
                .count()
                .get_result(conn)
                .map_err(|e| StoreError::Database(e.to_string()))?;
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
                    .map_err(|e| StoreError::Database(e.to_string()))?;
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
                StoreError::Serialization("Invalid signed_pre_key_signature length".to_string())
            })?;

        let adv_secret_key: [u8; 32] = row
            .adv_secret_key
            .try_into()
            .map_err(|_| StoreError::Serialization("Invalid adv_secret_key length".to_string()))?;

        let account = row
            .account
            .map(|data| {
                wacore::store::device::account_serde::from_bytes(&data)
                    .map_err(|e| StoreError::Serialization(e.to_string()))
            })
            .transpose()?;

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
            account,
            push_name: row.push_name,
            app_version_primary: row.app_version_primary as u32,
            app_version_secondary: row.app_version_secondary as u32,
            app_version_tertiary: row.app_version_tertiary.try_into().unwrap_or(0u32),
            app_version_last_fetched_ms: row.app_version_last_fetched_ms,
            device_props: {
                use wacore::store::device::DEVICE_PROPS;
                DEVICE_PROPS.clone()
            },
            edge_routing_info: row.edge_routing_info,
            props_hash: row.props_hash,
            next_pre_key_id: row.next_pre_key_id as u32,
            nct_salt: row.nct_salt,
            nct_salt_sync_seen: false,
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
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
            .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
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
            .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
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
            .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
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
                    .map_err(|e| StoreError::Database(e.to_string()))?;
                Ok(res)
            })
            .await?;

        if let Some(data) = res {
            let (key, _) = bincode::serde::decode_from_slice(&data, bincode::config::standard())
                .map_err(|e| StoreError::Serialization(e.to_string()))?;
            Ok(Some(key))
        } else {
            Ok(None)
        }
    }

    pub async fn set_app_state_sync_key_for_device(
        &self,
        key_id: &[u8],
        key: AppStateSyncKey,
        device_id: i32,
    ) -> Result<()> {
        let key_id = key_id.to_vec();
        let data = bincode::serde::encode_to_vec(&key, bincode::config::standard())
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
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
                    .map_err(|e| StoreError::Database(e.to_string()))?;
                Ok(res)
            })
            .await?;

        if let Some(data) = res {
            let (state, _) = bincode::serde::decode_from_slice(&data, bincode::config::standard())
                .map_err(|e| StoreError::Serialization(e.to_string()))?;
            Ok(state)
        } else {
            Ok(HashState::default())
        }
    }

    pub async fn set_app_state_version_for_device(
        &self,
        name: &str,
        state: HashState,
        device_id: i32,
    ) -> Result<()> {
        let name = name.to_string();
        let data = bincode::serde::encode_to_vec(&state, bincode::config::standard())
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
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
            .map_err(|e| StoreError::Database(e.to_string()))?;
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
            .map_err(|e| StoreError::Database(e.to_string()))?;
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(res)
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

    async fn load_identity(&self, address: &str) -> Result<Option<Vec<u8>>> {
        self.load_identity_for_device(address, self.device_id).await
    }

    async fn delete_identity(&self, address: &str) -> Result<()> {
        self.delete_identity_for_device(address, self.device_id)
            .await
    }

    async fn get_session(&self, address: &str) -> Result<Option<Vec<u8>>> {
        self.get_session_for_device(address, self.device_id).await
    }

    async fn put_session(&self, address: &str, session: &[u8]) -> Result<()> {
        self.put_session_for_device(address, session, self.device_id)
            .await
    }

    async fn delete_session(&self, address: &str) -> Result<()> {
        self.delete_session_for_device(address, self.device_id)
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
        })
        .await
    }

    async fn store_prekeys_batch(&self, keys: &[(u32, Vec<u8>)], uploaded: bool) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let device_id = self.device_id;
        let keys = keys.to_vec();
        self.with_conn(move |conn| {
            conn.transaction(|conn| {
                for (id, record) in &keys {
                    diesel::insert_into(prekeys::table)
                        .values((
                            prekeys::id.eq(*id as i32),
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
            .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
        })
        .await
    }

    async fn load_prekey(&self, id: u32) -> Result<Option<Vec<u8>>> {
        let device_id = self.device_id;
        self.with_conn(move |conn| {
            let res: Option<Vec<u8>> = prekeys::table
                .select(prekeys::key)
                .filter(prekeys::id.eq(id as i32))
                .filter(prekeys::device_id.eq(device_id))
                .first(conn)
                .optional()
                .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(res)
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
            .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
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
            .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
        })
        .await
    }

    async fn put_sender_key(&self, address: &str, record: &[u8]) -> Result<()> {
        self.put_sender_key_for_device(address, record, self.device_id)
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

    async fn delete_mutation_macs(&self, name: &str, index_macs: &[Vec<u8>]) -> Result<()> {
        self.delete_app_state_mutation_macs_for_device(name, index_macs, self.device_id)
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
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
            .map_err(|e| StoreError::Database(e.to_string()))?;
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
            .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
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
            .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
        })
        .await
    }

    async fn update_device_list(&self, record: DeviceListRecord) -> Result<()> {
        let device_id = self.device_id;
        let devices_json = serde_json::to_string(&record.devices)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i32;
        self.with_conn(move |conn| {
            diesel::insert_into(device_registry::table)
                .values((
                    device_registry::user_id.eq(&record.user),
                    device_registry::devices_json.eq(&devices_json),
                    device_registry::timestamp.eq(record.timestamp as i32),
                    device_registry::phash.eq(&record.phash),
                    device_registry::device_id.eq(device_id),
                    device_registry::updated_at.eq(now),
                ))
                .on_conflict((device_registry::user_id, device_registry::device_id))
                .do_update()
                .set((
                    device_registry::devices_json.eq(&devices_json),
                    device_registry::timestamp.eq(record.timestamp as i32),
                    device_registry::phash.eq(&record.phash),
                    device_registry::updated_at.eq(now),
                ))
                .execute(conn)
                .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
        })
        .await
    }

    async fn get_devices(&self, user: &str) -> Result<Option<DeviceListRecord>> {
        let device_id = self.device_id;
        let user = user.to_string();
        self.with_conn(move |conn| {
            let row: Option<(String, String, i32, Option<String>)> = device_registry::table
                .select((
                    device_registry::user_id,
                    device_registry::devices_json,
                    device_registry::timestamp,
                    device_registry::phash,
                ))
                .filter(device_registry::user_id.eq(&user))
                .filter(device_registry::device_id.eq(device_id))
                .first(conn)
                .optional()
                .map_err(|e| StoreError::Database(e.to_string()))?;
            match row {
                Some((user, devices_json, timestamp, phash)) => {
                    let devices: Vec<DeviceInfo> = serde_json::from_str(&devices_json)
                        .map_err(|e| StoreError::Serialization(e.to_string()))?;
                    Ok(Some(DeviceListRecord {
                        user,
                        devices,
                        timestamp: timestamp as i64,
                        phash,
                    }))
                }
                None => Ok(None),
            }
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
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
            .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
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
                .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(jids)
        })
        .await
    }

    async fn delete_expired_tc_tokens(&self, cutoff_timestamp: i64) -> Result<u32> {
        let device_id = self.device_id;
        self.with_conn(move |conn| {
            let deleted = diesel::delete(
                tc_tokens::table
                    .filter(tc_tokens::token_timestamp.lt(cutoff_timestamp))
                    .filter(tc_tokens::device_id.eq(device_id)),
            )
            .execute(conn)
            .map_err(|e| StoreError::Database(e.to_string()))?;
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
                .set((
                    sent_messages::payload.eq(payload.as_slice()),
                    sent_messages::chat_jid.eq(&chat_jid),
                    sent_messages::message_id.eq(&message_id),
                ))
                .execute(conn)
                .map_err(|e| StoreError::Database(e.to_string()))?;
            Ok(())
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
            .map_err(|e| StoreError::Database(e.to_string()))
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
            .map_err(|e| StoreError::Database(e.to_string()))?;
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

    // PG snapshot would use pg_dump; left as no-op (mirrors default).
}
