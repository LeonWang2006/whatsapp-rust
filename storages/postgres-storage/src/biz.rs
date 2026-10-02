//! Business-layer (biz schema) queries for the multi-pod server.
//!
//! The `biz` schema tables (`wa_user`, `contact`, ...) are managed by
//! `wa-server/deploy/sql/biz_init.sql`, NOT by diesel migrations, so they have
//! no `schema.rs` module here. Queries run as raw SQL against a connection
//! opened from the same database URL the account tables use.
//!
//! Why raw SQL instead of a diesel `table!` macro: the biz schema is owned by
//! the server's deploy SQL and can evolve independently; a hand-written
//! `table!` would drift from it the same way generated schema.rs does for the
//! public tables. Raw SQL keeps the mapping explicit and the column names
//! single-sourced in the deploy scripts.

use diesel::prelude::*;
use wacore::store::error::{Result as StoreResult, StoreError};

/// A `biz.wa_user` row identified by its current phone number.
#[derive(Debug, Clone)]
pub struct BizUser {
    pub id: i64,
    pub phone_number: String,
}

/// A `biz.wa_user` row including the infra-side device reference. Used by the
/// init/换卡 path where the caller must know whether the user already holds a
/// paired WhatsApp device (`wa_device_id`) so it can decide whether to clear it.
#[derive(Debug, Clone)]
pub struct BizUserFull {
    pub id: i64,
    pub device_uuid: String,
    pub phone_number: String,
    /// `public.device.id` of the currently-paired WhatsApp account, if any.
    /// SERIAL PK on the infra side, hence globally unique when non-NULL.
    pub wa_device_id: Option<i32>,
}

/// Row adapter for `SELECT id, phone_number FROM biz.wa_user ...` via raw SQL.
#[derive(QueryableByName)]
struct UserRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    id: i64,
    #[diesel(sql_type = diesel::sql_types::Text)]
    phone_number: String,
}

/// Row adapter for a full `biz.wa_user` lookup (init path needs `device_uuid`
/// and `wa_device_id` to detect a new user vs a phone-number change).
#[derive(QueryableByName)]
struct UserRowFull {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    id: i64,
    #[diesel(sql_type = diesel::sql_types::Text)]
    device_uuid: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    phone_number: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Int4>)]
    wa_device_id: Option<i32>,
}

/// Row adapter for a single phone-number column selected via raw SQL.
#[derive(QueryableByName)]
struct PhoneRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    phone_number: String,
}

/// Look up a `biz.wa_user` by its current phone number, across the whole
/// database (no `device_id` — biz rows are account-level, not device-sharded).
pub async fn biz_user_by_phone(database_url: &str, phone: &str) -> StoreResult<Option<BizUser>> {
    let url = database_url.to_string();
    let phone = phone.to_string();
    tokio::task::spawn_blocking(move || -> StoreResult<Option<BizUser>> {
        let mut conn =
            PgConnection::establish(&url).map_err(|e| StoreError::Connection(Box::new(e)))?;
        let row: Option<UserRow> =
            diesel::sql_query("SELECT id, phone_number FROM biz.wa_user WHERE phone_number = $1")
                .bind::<diesel::sql_types::Text, _>(&phone)
                .get_result(&mut conn)
                .optional()
                .map_err(|e| StoreError::Database(Box::new(e)))?;
        Ok(row.map(|r| BizUser {
            id: r.id,
            phone_number: r.phone_number,
        }))
    })
    .await
    .map_err(|e| StoreError::Database(Box::new(e)))?
}

/// Look up a `biz.wa_user` by its `device_uuid` (the client-generated device
/// identity). Includes `wa_device_id` so the init path can tell a brand-new
/// user from one that already holds a paired WhatsApp device (a phone-number
/// change must clear the stale reference).
pub async fn biz_user_by_device_uuid(
    database_url: &str,
    device_uuid: &str,
) -> StoreResult<Option<BizUserFull>> {
    let url = database_url.to_string();
    let uuid = device_uuid.to_string();
    tokio::task::spawn_blocking(move || -> StoreResult<Option<BizUserFull>> {
        let mut conn =
            PgConnection::establish(&url).map_err(|e| StoreError::Connection(Box::new(e)))?;
        let row: Option<UserRowFull> = diesel::sql_query(
            "SELECT id, device_uuid, phone_number, wa_device_id \
             FROM biz.wa_user WHERE device_uuid = $1",
        )
        .bind::<diesel::sql_types::Text, _>(&uuid)
        .get_result(&mut conn)
        .optional()
        .map_err(|e| StoreError::Database(Box::new(e)))?;
        Ok(row.map(|r| BizUserFull {
            id: r.id,
            device_uuid: r.device_uuid,
            phone_number: r.phone_number,
            wa_device_id: r.wa_device_id,
        }))
    })
    .await
    .map_err(|e| StoreError::Database(Box::new(e)))?
}

/// Upsert a `biz.wa_user` row keyed by `device_uuid`, returning the resolved
/// row so the caller can detect a phone-number change.
///
/// `phone_number` is required and updated on every call (换卡 = the client
/// re-inits with a new number under the same `device_uuid`). Device metadata
/// and push tokens are overwritten with the client's latest values.
///
/// `wa_device_id` is intentionally NOT written here — it is owned by the
/// pairing path (`create_for_jid`) and only cleared by the init path when it
/// detects a phone-number change. The caller passes `clear_wa_device_id` to
/// signal that; this function only ever *clears*, never *sets*.
#[allow(clippy::too_many_arguments)]
pub async fn upsert_biz_user(
    database_url: &str,
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
) -> StoreResult<BizUserFull> {
    let url = database_url.to_string();
    let uuid = device_uuid.to_string();
    let phone = phone_number.to_string();
    let os_version = os_version.map(str::to_owned);
    let manufacturer = manufacturer.map(str::to_owned);
    let device_model = device_model.map(str::to_owned);
    let os_build_number = os_build_number.map(str::to_owned);
    let locale_language = locale_language.map(str::to_owned);
    let locale_country = locale_country.map(str::to_owned);
    let device_info_raw = device_info_raw.map(str::to_owned);
    let firebase_token = firebase_token.map(str::to_owned);
    let apns_token = apns_token.map(str::to_owned);

    tokio::task::spawn_blocking(move || -> StoreResult<BizUserFull> {
        let mut conn =
            PgConnection::establish(&url).map_err(|e| StoreError::Connection(Box::new(e)))?;
        let row: UserRowFull = diesel::sql_query(
            "INSERT INTO biz.wa_user \
             (device_uuid, phone_number, status, platform, os_version, manufacturer, \
              device_model, os_build_number, locale_language, locale_country, \
              device_info_raw, firebase_token, apns_token, notification) \
             VALUES ($1, $2, 'init', $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
             ON CONFLICT (device_uuid) DO UPDATE SET \
               phone_number = EXCLUDED.phone_number, \
               platform = EXCLUDED.platform, \
               os_version = EXCLUDED.os_version, \
               manufacturer = EXCLUDED.manufacturer, \
               device_model = EXCLUDED.device_model, \
               os_build_number = EXCLUDED.os_build_number, \
               locale_language = EXCLUDED.locale_language, \
               locale_country = EXCLUDED.locale_country, \
               device_info_raw = EXCLUDED.device_info_raw, \
               firebase_token = EXCLUDED.firebase_token, \
               apns_token = EXCLUDED.apns_token, \
               notification = EXCLUDED.notification, \
               updated_at = now(), \
               wa_device_id = CASE WHEN $14 THEN NULL ELSE biz.wa_user.wa_device_id END \
             RETURNING id, device_uuid, phone_number, wa_device_id",
        )
        .bind::<diesel::sql_types::Text, _>(&uuid)
        .bind::<diesel::sql_types::Text, _>(&phone)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Int2>, _>(platform)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(os_version)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(manufacturer)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(device_model)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(os_build_number)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(locale_language)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(locale_country)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(device_info_raw)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(firebase_token)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(apns_token)
        .bind::<diesel::sql_types::Bool, _>(notification)
        .bind::<diesel::sql_types::Bool, _>(clear_wa_device_id)
        .get_result(&mut conn)
        .map_err(|e| StoreError::Database(Box::new(e)))?;
        Ok(BizUserFull {
            id: row.id,
            device_uuid: row.device_uuid,
            phone_number: row.phone_number,
            wa_device_id: row.wa_device_id,
        })
    })
    .await
    .map_err(|e| StoreError::Database(Box::new(e)))?
}

/// Return the contact phone numbers a user has added, in insertion order.
pub async fn biz_contacts_for_user(database_url: &str, user_id: i64) -> StoreResult<Vec<String>> {
    let url = database_url.to_string();
    tokio::task::spawn_blocking(move || -> StoreResult<Vec<String>> {
        let mut conn =
            PgConnection::establish(&url).map_err(|e| StoreError::Connection(Box::new(e)))?;
        let rows: Vec<PhoneRow> = diesel::sql_query(
            "SELECT phone_number FROM biz.contact WHERE user_id = $1 ORDER BY id",
        )
        .bind::<diesel::sql_types::BigInt, _>(user_id)
        .load(&mut conn)
        .map_err(|e| StoreError::Database(Box::new(e)))?;
        Ok(rows.into_iter().map(|r| r.phone_number).collect())
    })
    .await
    .map_err(|e| StoreError::Database(Box::new(e)))?
}

/// Record a pairing-flow lifecycle event in `biz.pair_history` (pair/logout/
/// stream_replaced/reconnect/replace_phone). Best-effort audit trail for the
/// admin; a failure to write history must not fail the operation that caused it.
pub async fn record_pair_history(
    database_url: &str,
    user_id: i64,
    phone_number: Option<&str>,
    wa_device_id: Option<i32>,
    action: &str,
    pair_code: Option<&str>,
    detail: Option<&str>,
) -> StoreResult<()> {
    let url = database_url.to_string();
    let phone = phone_number.map(str::to_owned);
    let code = pair_code.map(str::to_owned);
    let detail = detail.map(str::to_owned);
    let action = action.to_string();
    tokio::task::spawn_blocking(move || -> StoreResult<()> {
        let mut conn =
            PgConnection::establish(&url).map_err(|e| StoreError::Connection(Box::new(e)))?;
        diesel::sql_query(
            "INSERT INTO biz.pair_history \
             (user_id, phone_number, wa_device_id, action, pair_code, detail) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind::<diesel::sql_types::BigInt, _>(user_id)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(phone)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Int4>, _>(wa_device_id)
        .bind::<diesel::sql_types::Text, _>(action)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(code)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(detail)
        .execute(&mut conn)
        .map_err(|e| StoreError::Database(Box::new(e)))?;
        Ok(())
    })
    .await
    .map_err(|e| StoreError::Database(Box::new(e)))?
}

/// One presence (online/offline) event for a contact, as persisted in
/// `biz.presence_event`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresenceEvent {
    pub owner_phone: String,
    pub contact_phone: String,
    /// `online` or `offline`.
    pub event_type: String,
    /// Unix seconds when the event occurred.
    pub ts: i64,
    /// `last_seen` carried by an offline event (absent for online).
    pub last_seen: Option<i64>,
}

/// Row adapter for `biz.presence_event` rows via raw SQL.
#[derive(QueryableByName)]
struct PresenceEventRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    owner_phone: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    contact_phone: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    event_type: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    ts: i64,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    last_seen: Option<i64>,
}

/// Insert a presence event. Idempotent on (owner, contact, type, ts) so a
/// re-delivered stanza or a reconnect race does not double-count.
///
/// Runs on its own connection (not the shared pool) like the other biz
/// helpers; presence writes are low-frequency and per-session, so a fresh
/// connection per insert is acceptable.
pub async fn record_presence_event(
    database_url: &str,
    owner_phone: &str,
    contact_phone: &str,
    event_type: &str,
    ts: i64,
    last_seen: Option<i64>,
) -> StoreResult<()> {
    let url = database_url.to_string();
    let owner = owner_phone.to_string();
    let contact = contact_phone.to_string();
    let kind = event_type.to_string();
    tokio::task::spawn_blocking(move || -> StoreResult<()> {
        let mut conn =
            PgConnection::establish(&url).map_err(|e| StoreError::Connection(Box::new(e)))?;
        diesel::sql_query(
            "INSERT INTO biz.presence_event (owner_phone, contact_phone, event_type, ts, last_seen) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT DO NOTHING",
        )
        .bind::<diesel::sql_types::Text, _>(&owner)
        .bind::<diesel::sql_types::Text, _>(&contact)
        .bind::<diesel::sql_types::Text, _>(&kind)
        .bind::<diesel::sql_types::BigInt, _>(ts)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::BigInt>, _>(last_seen)
        .execute(&mut conn)
        .map_err(|e| StoreError::Database(Box::new(e)))?;
        Ok(())
    })
    .await
    .map_err(|e| StoreError::Database(Box::new(e)))?
}

/// Query presence events for one owner + contact in a `[start, end]` time
/// window, oldest first. `start`/`end` are Unix seconds; the caller supplies
/// defaults.
pub async fn query_presence_events(
    database_url: &str,
    owner_phone: &str,
    contact_phone: &str,
    start: i64,
    end: i64,
) -> StoreResult<Vec<PresenceEvent>> {
    let url = database_url.to_string();
    let owner = owner_phone.to_string();
    let contact = contact_phone.to_string();
    tokio::task::spawn_blocking(move || -> StoreResult<Vec<PresenceEvent>> {
        let mut conn =
            PgConnection::establish(&url).map_err(|e| StoreError::Connection(Box::new(e)))?;
        let rows: Vec<PresenceEventRow> = diesel::sql_query(
            "SELECT owner_phone, contact_phone, event_type, ts, last_seen \
             FROM biz.presence_event \
             WHERE owner_phone = $1 AND contact_phone = $2 AND ts BETWEEN $3 AND $4 \
             ORDER BY ts",
        )
        .bind::<diesel::sql_types::Text, _>(&owner)
        .bind::<diesel::sql_types::Text, _>(&contact)
        .bind::<diesel::sql_types::BigInt, _>(start)
        .bind::<diesel::sql_types::BigInt, _>(end)
        .load(&mut conn)
        .map_err(|e| StoreError::Database(Box::new(e)))?;
        Ok(rows
            .into_iter()
            .map(|r| PresenceEvent {
                owner_phone: r.owner_phone,
                contact_phone: r.contact_phone,
                event_type: r.event_type,
                ts: r.ts,
                last_seen: r.last_seen,
            })
            .collect())
    })
    .await
    .map_err(|e| StoreError::Database(Box::new(e)))?
}
