//! Forwards WhatsApp `Event`s from a session to the Redis `wa-events` list.
//!
//! Business systems consume `wa-events` via `BRPOP` to receive updates from any
//! session running on any pod. This module is the only bridge between a live
//! session and the Redis event stream.
//!
//! In addition to the event stream, a freshly minted 8-char pairing code is
//! written to a per-JID key (`{prefix}:{jid}`, see [`pair_code_key`]) so a
//! client can poll it via the HTTP API instead of subscribing to `wa-events`.
//! The key lives only as long as the code is valid (`timeout`, ~180s) and is
//! cleared as soon as the code is superseded, the flow fails, or the device
//! logs in.
//!
//! The same events also drive a per-phone link-status key
//! ([`link_status_key`], [`LinkStatus`]) that tracks the whole pairing flow's
//! outcome — pairing / success / failed — so a client can tell whether a link
//! succeeded or what to surface on failure, and retry by calling `/link` again.

use log::warn;
use redis::aio::ConnectionManager;
use serde::Serialize;
use wacore::types::events::Event;

use crate::task::{EVENT_QUEUE, LinkStatus, LinkStatusKind, pair_code_key};

use crate::task::link_status_key;

#[derive(Serialize)]
struct EventEnvelope<'a> {
    jid: &'a str,
    pod_id: &'a str,
    ts: u64,
    event: &'a Event,
}

/// Per-session configuration the event bridge needs to forward an event.
/// Bundled so `forward_event_to_redis` doesn't grow an unwieldy argument list.
#[derive(Clone)]
pub struct EventForwardConfig<'a> {
    pub jid: &'a str,
    pub pod_id: &'a str,
    pub pair_code_key_prefix: &'a str,
    pub link_status_key_prefix: &'a str,
    /// Whether the device already had credentials before this session started
    /// (a resume rather than a fresh pairing).
    pub device_existed: bool,
    /// Bare phone number to key the pair-code/link-status keys by, when known.
    pub pair_phone: Option<&'a str>,
}

pub async fn forward_event_to_redis(
    redis: &mut ConnectionManager,
    cfg: EventForwardConfig<'_>,
    event: &Event,
) {
    let EventForwardConfig {
        jid,
        pod_id,
        pair_code_key_prefix,
        link_status_key_prefix,
        device_existed,
        pair_phone,
    } = cfg;
    let ts = wacore::time::now_secs().max(0) as u64;

    // Maintain the pollable pairing-code key alongside the event stream.
    sync_pair_code_key(redis, jid, pair_phone, event, pair_code_key_prefix).await;
    // Track the pairing flow's outcome for `GET /link-status` polling.
    sync_link_status(
        redis,
        jid,
        pair_phone,
        event,
        link_status_key_prefix,
        device_existed,
    )
    .await;

    let envelope = EventEnvelope {
        jid,
        pod_id,
        ts,
        event,
    };

    let payload = match serde_json::to_vec(&envelope) {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!("failed to serialize event for jid={jid}: {e}");
            return;
        }
    };

    let mut pipe = redis::pipe();
    pipe.atomic().lpush(EVENT_QUEUE, payload).ignore();
    if let Err(e) = pipe.query_async::<()>(redis).await {
        warn!("failed to LPUSH event to {EVENT_QUEUE} for jid={jid}: {e}");
    }
}

/// Keep the pollable pair-code key in sync with `event`.
///
/// A `PairingCode` event mints a fresh key with a TTL matching the code's
/// validity window; a `PairSuccess`, `PairingCodeError`, or `PairingCodeRefresh`
/// invalidates the current code, so the key is removed. Everything else leaves
/// it untouched.
///
/// The key is keyed by the bare phone number (`pair_phone`) when known, so a
/// client can poll it without the `@s.whatsapp.net` suffix; otherwise the full
/// JID is used.
async fn sync_pair_code_key(
    redis: &mut ConnectionManager,
    jid: &str,
    pair_phone: Option<&str>,
    event: &Event,
    pair_code_key_prefix: &str,
) {
    // `pair_phone` may be a different string than the JID's user part (the
    // task's phone number wins), but both key the same logical pairing flow.
    let key_part = pair_phone.unwrap_or(jid);
    let key = pair_code_key(pair_code_key_prefix, key_part);
    match event {
        Event::PairingCode(pc) => {
            // `timeout` is the ~180s window during which the phone must enter
            // the code; mirror it as the key's TTL so a stale code never
            // outlives its usefulness.
            let ttl = pc.timeout.as_secs().max(1) as i64;
            let mut pipe = redis::pipe();
            pipe.atomic()
                .set(&key, &pc.code)
                .ignore()
                .expire(&key, ttl)
                .ignore();
            if let Err(e) = pipe.query_async::<()>(redis).await {
                warn!("failed to store pair code for jid={jid}: {e}");
            }
        }
        Event::PairSuccess(_) | Event::PairingCodeError(_) | Event::PairingCodeRefresh(_) => {
            let mut pipe = redis::pipe();
            pipe.atomic().del(&key).ignore();
            if let Err(e) = pipe.query_async::<()>(redis).await {
                warn!("failed to clear pair code for jid={jid}: {e}");
            }
        }
        _ => {}
    }
}

/// Track the pairing flow's outcome under `wa-link-status:{phone}`.
///
/// The client polls this via `GET /link-status?phone=...` to learn whether a
/// link succeeded or what went wrong. Written from the session worker's event
/// callback alongside [`sync_pair_code_key`], using the same phone-keying rule.
///
/// - [`Event::PairingCode`] → `pairing` + the code (the flow is live).
/// - [`Event::PairSuccess`] → `success`, clearing any code.
/// - [`Event::PairingCodeError`] → `failed` + a reason the client can show.
/// - [`Event::PairingCodeRefresh`] → `pairing` again (the code was rotated;
///   the consumer re-requests, so the flow is still live).
/// - [`Event::Connected`] on a *resume* session (`device_existed`) → `success`:
///   an already-paired device coming back online is the desired outcome of a
///   `/link` call on it, and there is no [`Event::PairSuccess`] for a resume.
///
/// Success/failed are terminal: the key gets a long TTL so a slow client still
/// reads the outcome after the pairing session is gone.
async fn sync_link_status(
    redis: &mut ConnectionManager,
    jid: &str,
    pair_phone: Option<&str>,
    event: &Event,
    link_status_key_prefix: &str,
    device_existed: bool,
) {
    let key_part = pair_phone.unwrap_or(jid);
    let key = link_status_key(link_status_key_prefix, key_part);
    let updated_at = wacore::time::now_secs().max(0) as i64;
    let write = match event {
        Event::PairingCode(pc) => {
            let ttl = pc.timeout.as_secs().max(1) as i64;
            let status = LinkStatus {
                status: LinkStatusKind::Pairing,
                code: Some(pc.code.clone()),
                error: None,
                updated_at,
            };
            Some((status, ttl))
        }
        Event::PairSuccess(_) => {
            let status = LinkStatus {
                status: LinkStatusKind::Success,
                code: None,
                error: None,
                updated_at,
            };
            Some((status, crate::task::LINK_STATUS_TERMINAL_TTL_SECS))
        }
        // A resume device coming online is a successful link — the client asked
        // for the device to be online and it is.
        Event::Connected(_) if device_existed => {
            let status = LinkStatus {
                status: LinkStatusKind::Success,
                code: None,
                error: None,
                updated_at,
            };
            Some((status, crate::task::LINK_STATUS_TERMINAL_TTL_SECS))
        }
        Event::PairingCodeError(e) => {
            let error = match &e.rejection {
                Some(rej) => rej
                    .text()
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("unknown-{}", rej.code())),
                None => "no-server-answer".to_string(),
            };
            let status = LinkStatus {
                status: LinkStatusKind::Failed,
                code: None,
                error: Some(error),
                updated_at,
            };
            Some((status, crate::task::LINK_STATUS_TERMINAL_TTL_SECS))
        }
        Event::PairingCodeRefresh(_) => {
            let status = LinkStatus {
                status: LinkStatusKind::Pairing,
                code: None,
                error: None,
                updated_at,
            };
            Some((status, crate::task::LINK_STATUS_RESET_TTL_SECS))
        }
        _ => None,
    };
    let Some((status, ttl)) = write else { return };
    let body = match serde_json::to_vec(&status) {
        Ok(b) => b,
        Err(e) => {
            warn!("failed to serialize link status for jid={jid}: {e}");
            return;
        }
    };
    let mut pipe = redis::pipe();
    pipe.atomic()
        .set(&key, body)
        .ignore()
        .expire(&key, ttl)
        .ignore();
    if let Err(e) = pipe.query_async::<()>(redis).await {
        warn!("failed to write link status for jid={jid}: {e}");
    }
}
