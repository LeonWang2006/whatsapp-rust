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

/// Default prefix for the per-phone link-status keys the API serves to clients
/// that poll for the pairing flow's outcome. Overridable via the
/// `LINK_STATUS_KEY_PREFIX` env var.
pub const LINK_STATUS_KEY_PREFIX: &str = "wa-link-status";

/// TTL for the terminal (success/failed) link status. Long enough that a slow
/// client still sees the outcome of a pairing flow it triggered.
pub const LINK_STATUS_TERMINAL_TTL_SECS: i64 = 24 * 3600;

/// TTL for the reset `pairing` status written by `POST /link` before any code
/// is minted. Generous so a slow handshake doesn't expire it before the worker
/// writes the real `pairing`+code state.
pub const LINK_STATUS_RESET_TTL_SECS: i64 = 300;

/// Redis key that holds the JSON [`LinkStatus`] for a phone number.
///
/// Keyed by the bare phone number (`wa-link-status:8618666206882`) like the
/// pair-code key, so the client polls it without the JID suffix.
pub fn link_status_key(prefix: &str, phone_or_jid: &str) -> String {
    format!("{prefix}:{phone_or_jid}")
}

/// Progress of a phone-number pairing flow, persisted to Redis by the session
/// worker as the flow advances and polled by the client via `GET /link-status`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkStatus {
    pub status: LinkStatusKind,
    /// The 8-char code while waiting for phone confirmation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Human-readable reason when `status` is `failed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Unix seconds when this status was written.
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LinkStatusKind {
    /// Code minted (or `/link` accepted), waiting for the phone to confirm.
    Pairing,
    /// Pairing succeeded; the device is online.
    Success,
    /// Pairing failed; the client should surface the error and may retry by
    /// calling `/link` again.
    Failed,
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

/// Number of `wa-queue` shards. Must match the producer side (`QUEUE_SHARDS`
/// in `server.rs`) and the shell producer (`demo-inject.sh`, crc32 % 16).
pub const QUEUE_SHARDS: usize = 16;

/// Which `wa-queue:{shard}` key a JID belongs to.
///
/// Same algorithm as the shell producer (`python binascii.crc32 % 16`) so an
/// API-side push lands on the same shard `demo-inject.sh` would use and the
/// same session stays shard-stable across producers.
pub fn shard_for_jid(jid: &str) -> usize {
    (crc32(jid.as_bytes()) % QUEUE_SHARDS as u32) as usize
}

/// Standard IEEE CRC-32 (poly 0xEDB88320, init 0xFFFFFFFF, final xor
/// 0xFFFFFFFF), the same variant Python's `binascii.crc32` returns.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    crc ^ 0xFFFF_FFFF
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

/// A contact's online/offline presence event, as persisted in
/// `biz.presence_event` and served by `GET /presence`.
///
/// Storage-agnostic: the PG factory maps its own `biz` row type onto this so
/// the server trait stays decoupled from any one backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresenceEvent {
    /// Owning account phone number (whose session observed the contact).
    pub owner_phone: String,
    /// Contact phone number (LID already normalized to PN where known).
    pub contact_phone: String,
    /// `online` or `offline`.
    pub event_type: String,
    /// Unix seconds when the event occurred.
    pub ts: i64,
    /// `last_seen` carried by an offline event (absent for online).
    pub last_seen: Option<i64>,
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
    fn link_status_roundtrip() {
        let status = LinkStatus {
            status: LinkStatusKind::Pairing,
            code: Some("39X486Z6".into()),
            error: None,
            updated_at: 1_700_000_000,
        };
        let json = serde_json::to_string(&status).unwrap();
        let back: LinkStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back.status, LinkStatusKind::Pairing);
        assert_eq!(back.code.as_deref(), Some("39X486Z6"));
        assert!(back.error.is_none());
        // Failed carries an error and no code.
        let failed = LinkStatus {
            status: LinkStatusKind::Failed,
            code: None,
            error: Some("rate-overlimit".into()),
            updated_at: 1_700_000_000,
        };
        let back: LinkStatus =
            serde_json::from_str(&serde_json::to_string(&failed).unwrap()).unwrap();
        assert_eq!(back.status, LinkStatusKind::Failed);
        assert!(back.code.is_none());
        assert_eq!(back.error.as_deref(), Some("rate-overlimit"));
    }

    #[test]
    fn link_status_key_format() {
        assert_eq!(
            link_status_key("wa-link-status", "861866620688"),
            "wa-link-status:861866620688"
        );
        assert_eq!(
            link_status_key("custom", "861866620688"),
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

    #[test]
    fn crc32_matches_python_binascii() {
        // Expected values from `python3 -c "import binascii;print(binascii.crc32(b'...'))"`.
        assert_eq!(crc32(b""), 0);
        assert_eq!(crc32(b"a"), 0xE8B7_BE43);
        assert_eq!(crc32(b"abc"), 0x3524_41C2);
        assert_eq!(crc32(b"8618666206882@s.whatsapp.net"), 0x5F5B_7685);
    }

    #[test]
    fn shard_for_jid_is_stable_and_in_range() {
        let jids = [
            "8618666206882@s.whatsapp.net",
            "15550000001@s.whatsapp.net",
            "447907841573@s.whatsapp.net",
            "1234@g.us",
        ];
        for j in &jids {
            let s = shard_for_jid(j);
            assert!(s < QUEUE_SHARDS, "shard {s} out of range for {j}");
            assert_eq!(shard_for_jid(j), s, "shard not stable for {j}");
        }
    }
}
