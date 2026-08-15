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

/// Default prefix for the per-JID pair-code keys the HTTP API serves to
/// clients that poll for a freshly minted pairing code. Overridable via the
/// `PAIR_CODE_KEY_PREFIX` env var.
pub const PAIR_CODE_KEY_PREFIX: &str = "wa-pair-code";

/// Redis key that holds the current 8-char pairing code for a phone number.
///
/// The key is keyed by the bare phone number (`wa-pair-code:8618666206882`)
/// so the client can poll it without knowing the full `@s.whatsapp.net` JID.
pub fn pair_code_key(prefix: &str, phone_or_jid: &str) -> String {
    format!("{prefix}:{phone_or_jid}")
}

/// Extract the bare phone number from a `@s.whatsapp.net` JID, if it is one.
/// Other JIDs (groups `@g.us`, LIDs `@lid`) yield `None`, in which case the
/// caller falls back to the full JID for the key.
pub fn phone_from_jid(jid: &str) -> Option<&str> {
    let (user, domain) = jid.split_once('@')?;
    if domain == "s.whatsapp.net" && user.chars().all(|c| c.is_ascii_digit()) {
        Some(user)
    } else {
        None
    }
}

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
/// uploaded by the producer and referenced by CDN fields; for now the producer
/// passes through a pre-built message JSON fragment.
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
    fn pair_code_key_format() {
        // The API reads whatever key the event bridge writes; the prefix must
        // be the configurable knob, so pin the exact key shape here. Keyed by
        // bare phone number, not the full JID.
        assert_eq!(
            pair_code_key("wa-pair-code", "861866620688"),
            "wa-pair-code:861866620688"
        );
        assert_eq!(
            pair_code_key("custom", "861866620688"),
            "custom:861866620688"
        );
    }

    #[test]
    fn phone_from_jid_extracts_bare_number() {
        assert_eq!(
            phone_from_jid("861866620688@s.whatsapp.net"),
            Some("861866620688")
        );
        // Groups and non-numeric user parts have no phone number to key on.
        assert_eq!(phone_from_jid("1234@g.us"), None);
        assert_eq!(phone_from_jid("not-a-number@s.whatsapp.net"), None);
    }
}
