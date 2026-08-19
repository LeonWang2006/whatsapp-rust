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

/// Row adapter for `SELECT id, phone_number FROM biz.wa_user ...` via raw SQL.
#[derive(QueryableByName)]
struct UserRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    id: i64,
    #[diesel(sql_type = diesel::sql_types::Text)]
    phone_number: String,
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
