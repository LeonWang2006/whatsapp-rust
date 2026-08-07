//! Task and command types for the multi-session server.
//!
//! `TaskEnvelope` is the JSON shape consumed from the Redis `wa-queue` sharded
//! queues. `SessionCommand` is the in-process enum sent to a running session
//! worker over its mpsc channel.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Task type tag. Kept as a string-tagged enum so unknown future types survive
/// deserialization as `Unknown` rather than failing the whole queue consumer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskType {
    PairQr,
    PairCode,
    SendMessage,
    SendMedia,
    React,
    Disconnect,
    Logout,
    #[serde(other)]
    Unknown,
}

impl TaskType {
    /// Pairing tasks may spawn a brand-new session even when no registry hit.
    pub fn is_pairing(&self) -> bool {
        matches!(self, TaskType::PairQr | TaskType::PairCode)
    }
}

/// Envelope dequeued from `wa-queue:shardN` via `BRPOP`.
///
/// `payload` is intentionally a `serde_json::Value` so the dispatcher can
/// type-check per task type and forward opaque blobs to sessions without every
/// server build knowing every payload schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEnvelope {
    pub task_id: String,
    #[serde(rename = "type")]
    pub task_type: TaskType,
    pub jid: String,
    pub created_at: i64,
    pub payload: serde_json::Value,
}

/// Commands delivered to an already-running session worker.
#[derive(Debug, Clone)]
pub enum SessionCommand {
    /// Forward a dequeued task to the session's client.
    Task(TaskEnvelope),
    /// Tear the session down cleanly.
    Disconnect,
}

/// Default Redis queue config. Shards are addressed as `wa-queue:{i}`.
pub const QUEUE_PREFIX: &str = "wa-queue";
pub const EVENT_QUEUE: &str = "wa-events";
pub const REGISTRY_KEY: &str = "wa-registry";
pub const REGISTRY_TTL: Duration = Duration::from_secs(60);
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);

/// Per-pod inbox key: `wa-inbox:{pod_id}`. Cross-pod forwarded tasks land here.
pub fn inbox_key(pod_id: &str) -> String {
    format!("wa-inbox:{pod_id}")
}

/// Payload for `send_message` tasks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendMessagePayload {
    /// Target chat JID (user@s.whatsapp.net, group@g.us, etc.).
    pub to: String,
    /// Plaintext body. Builds a `conversation` or `extendedTextMessage`.
    pub text: String,
    /// JID + message ID to quote, if replying.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote: Option<QuoteTarget>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteTarget {
    pub chat_jid: String,
    pub message_id: String,
}

/// Payload for `send_media` tasks. Media bytes are expected to already be
/// uploaded by the producer and referenced by CDN fields; P3 may add inline
/// upload via a side-channel. For now the producer passes through a
/// pre-built message JSON fragment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendMediaPayload {
    pub to: String,
    /// Opaque pre-built `wa::Message` JSON. The server forwards it as-is.
    pub message_json: serde_json::Value,
}

/// Payload for `react` tasks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReactPayload {
    pub to: String,
    pub message_id: String,
    pub emoji: String,
    /// Group participant JID, required for group reactions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub participant: Option<String>,
}

/// Payload for `pair_code` tasks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairCodePayload {
    /// E.164 phone number, digits only.
    pub phone_number: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_envelope_roundtrip() {
        let env = TaskEnvelope {
            task_id: "t-1".into(),
            task_type: TaskType::SendMessage,
            jid: "5559998888@s.whatsapp.net".into(),
            created_at: 1700000000,
            payload: serde_json::json!({"text": "hi"}),
        };
        let json = serde_json::to_string(&env).unwrap();
        let back: TaskEnvelope = serde_json::from_str(&json).unwrap();
        assert_eq!(back.task_id, "t-1");
        assert_eq!(back.task_type, TaskType::SendMessage);
        assert_eq!(back.jid, "5559998888@s.whatsapp.net");
    }

    #[test]
    fn task_type_tag_deserialization() {
        // snake_case tags, matching the producer side.
        let json = "{\"task_id\":\"x\",\"type\":\"pair_qr\",\"jid\":\"j\",\"created_at\":0,\"payload\":null}";
        let env: TaskEnvelope = serde_json::from_str(json).unwrap();
        assert_eq!(env.task_type, TaskType::PairQr);
        assert!(env.task_type.is_pairing());
    }

    #[test]
    fn task_type_unknown_survives() {
        let json = "{\"task_id\":\"x\",\"type\":\"future_thing\",\"jid\":\"j\",\"created_at\":0,\"payload\":null}";
        let env: TaskEnvelope = serde_json::from_str(json).unwrap();
        assert_eq!(env.task_type, TaskType::Unknown);
        assert!(!env.task_type.is_pairing());
    }

    #[test]
    fn send_message_payload_roundtrip() {
        let p = SendMessagePayload {
            to: "5559998888@s.whatsapp.net".into(),
            text: "hi".into(),
            quote: Some(QuoteTarget {
                chat_jid: "5559998888@s.whatsapp.net".into(),
                message_id: "msg-1".into(),
            }),
        };
        let json = serde_json::to_string(&p).unwrap();
        let back: SendMessagePayload = serde_json::from_str(&json).unwrap();
        assert_eq!(back.text, "hi");
        assert!(back.quote.is_some());
    }

    #[test]
    fn react_payload_optional_participant() {
        let dm: ReactPayload = serde_json::from_str(
            "{\"to\":\"a@s.whatsapp.net\",\"message_id\":\"m\",\"emoji\":\"\u{1f44d}\"}",
        )
        .unwrap();
        assert!(dm.participant.is_none());

        let grp: ReactPayload = serde_json::from_str(
            "{\"to\":\"g@g.us\",\"message_id\":\"m\",\"emoji\":\"\u{1f44d}\",\"participant\":\"p@s.whatsapp.net\"}",
        )
        .unwrap();
        assert!(grp.participant.is_some());
    }
}
