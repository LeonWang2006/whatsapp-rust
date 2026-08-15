//! Forwards WhatsApp `Event`s from a session to the Redis `wa-events` list.
//!
//! Business systems consume `wa-events` via `BRPOP` to receive updates from any
//! session running on any pod. This module is the only bridge between a live
//! session and the Redis event stream.

use log::warn;
use redis::aio::ConnectionManager;
use serde::Serialize;
use wacore::types::events::Event;

use crate::task::EVENT_QUEUE;

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
) {
    let ts = wacore::time::now_secs().max(0) as u64;

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
