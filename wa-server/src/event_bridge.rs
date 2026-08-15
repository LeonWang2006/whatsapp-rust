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

use log::warn;
use redis::aio::ConnectionManager;
use serde::Serialize;
use wacore::types::events::Event;

use crate::task::{EVENT_QUEUE, pair_code_key};

#[derive(Serialize)]
struct EventEnvelope<'a> {
    jid: &'a str,
    pod_id: &'a str,
    ts: u64,
    event: &'a Event,
}

pub async fn forward_event_to_redis(
    redis: &mut ConnectionManager,
    jid: &str,
    pod_id: &str,
    event: &Event,
    pair_code_key_prefix: &str,
) {
    let ts = wacore::time::now_secs().max(0) as u64;

    // Maintain the pollable pairing-code key alongside the event stream.
    sync_pair_code_key(redis, jid, event, pair_code_key_prefix).await;

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
async fn sync_pair_code_key(
    redis: &mut ConnectionManager,
    jid: &str,
    event: &Event,
    pair_code_key_prefix: &str,
) {
    let key = pair_code_key(pair_code_key_prefix, jid);
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
