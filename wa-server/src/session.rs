//! Single-session worker.
//!
//! Each WhatsApp account runs as one `run_session` task on the shared tokio
//! runtime. The task owns a `Bot` (which owns a `Client`) and an mpsc receiver
//! for `SessionCommand`s forwarded by the dispatcher. Events from the session
//! are bridged to the Redis `wa-events` list.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use log::{error, info, warn};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use wacore::pair_code::PairCodeOptions;
use wacore::types::events::Event;
use wacore_binary::jid::Jid;
use waproto::whatsapp as wa;
use whatsapp_rust::bot::Bot;
use whatsapp_rust::buffa;

use crate::event_bridge::forward_event_to_redis;
use crate::redis_registry::{register_in_redis, spawn_heartbeat, unregister_in_redis};
use crate::registry::{SessionHandle, SessionRegistry};
use crate::storage_factory::StorageFactory;
use crate::task::{
    PairCodePayload, ReactPayload, SendMessagePayload, SessionCommand, TaskEnvelope, TaskType,
};

/// Shared server context handed to every session worker.
#[derive(Clone)]
pub struct ServerContext {
    pub registry: SessionRegistry,
    pub storage_factory: Arc<dyn StorageFactory>,
    /// Shared manager for non-blocking ops (registry, heartbeat, events, API).
    /// Blocking `BRPOP` consumers must NOT share this — they open dedicated
    /// connections from `redis_client` instead, so a blocking command can't
    /// head-of-line-block every other request on the multiplexed connection.
    pub redis: redis::aio::ConnectionManager,
    /// Raw client used by consumers to open a dedicated connection each.
    pub redis_client: redis::Client,
    pub pod_id: String,
    /// Hard cap on concurrent sessions per pod. 0 = unlimited.
    pub max_sessions: usize,
    /// Prefix for per-JID pair-code keys written to Redis for the API to serve.
    pub pair_code_key_prefix: String,
}

/// Build and run one session for `jid`. `first_task` (if present) is delivered
/// to the session as soon as the command loop starts, so a pairing task that
/// triggered session creation is not lost.
pub async fn run_session(ctx: ServerContext, jid: String, first_task: Option<TaskEnvelope>) {
    let cancel = CancellationToken::new();
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<SessionCommand>(64);

    // Resolve or lazily create the storage backend for this JID.
    let backend = match ctx.storage_factory.for_jid(&jid).await {
        Some(b) => b,
        None => match ctx.storage_factory.create_for_jid(&jid).await {
            Ok((_, b)) => b,
            Err(e) => {
                error!("failed to create device for jid={jid}: {e}");
                return;
            }
        },
    };

    // Claim the registry entry. If another pod already owns an unexpired
    // lease we should not have been dispatched here; defensively abort.
    if let Err(e) = register_in_redis(&mut ctx.redis.clone(), &jid, &ctx.pod_id).await {
        warn!("registry claim failed for jid={jid}: {e}; aborting session");
        return;
    }
    spawn_heartbeat(
        ctx.redis.clone(),
        jid.clone(),
        ctx.pod_id.clone(),
        cancel.clone(),
    );

    let event_redis = ctx.redis.clone();
    let event_jid = jid.clone();
    let event_pod = ctx.pod_id.clone();
    let event_cancel = cancel.clone();
    let event_factory = ctx.storage_factory.clone();
    let event_registry = ctx.registry.clone();
    let event_pair_prefix = ctx.pair_code_key_prefix.clone();
    // Key the pair-code Redis entry by the bare phone number when the session
    // was created by a pair_code task; otherwise fall back to the JID.
    let event_pair_phone = pair_phone_from_task(first_task.as_ref())
        .or_else(|| crate::task::phone_from_jid(&jid).map(str::to_owned));

    let mut builder = Bot::builder()
        .with_backend_arc(backend)
        .on_event(move |event, _client| {
            let mut redis = event_redis.clone();
            let jid = event_jid.clone();
            let pod = event_pod.clone();
            let cancel = event_cancel.clone();
            let factory = event_factory.clone();
            let registry = event_registry.clone();
            let pair_prefix = event_pair_prefix.clone();
            let pair_phone = event_pair_phone.clone();
            async move {
                // Forward every event to the Redis event stream first so the
                // business system learns about the logout/replacement and the
                // pairing code / QR stream the admin polls.
                forward_event_to_redis(
                    &mut redis,
                    &jid,
                    &pod,
                    &event,
                    &pair_prefix,
                    pair_phone.as_deref(),
                )
                .await;

                // Lifecycle events trigger session teardown.
                let (should_delete, label) = match &*event {
                    Event::LoggedOut(_) => (true, "logged_out"),
                    Event::StreamReplaced(_) => (false, "stream_replaced"),
                    _ => return,
                };
                info!("jid={jid} got {label}; tearing down session");
                if should_delete && let Err(e) = factory.delete_for_jid(&jid).await {
                    warn!("failed to delete device for jid={jid} on logout: {e}");
                }
                // Remove registry entry so dispatch stops routing to us.
                if let Some(h) = registry.get(&jid) {
                    registry.remove_if_matching(&jid, &h);
                }
                let mut r = redis.clone();
                unregister_in_redis(&mut r, &jid, &pod).await;
                cancel.cancel();
            }
        });

    // A pairing task that *creates* this session configures the library to
    // request the code automatically once the socket is up, instead of us
    // calling `pair_with_code` from the command loop before the socket exists
    // (which fails with `client is not connected`). The generated code is
    // dispatched as `Event::PairingCode` and bridged to `wa-events` above.
    if let Some(pair_options) = pair_code_options_from_task(first_task.as_ref()) {
        builder = builder.with_pair_code(pair_options);
    }

    // Optional HTTP proxy for environments that cannot reach WhatsApp directly
    // (WA_PROXY_URL, e.g. http://127.0.0.1:7890). Unset keeps the default.
    if let Some(proxy) = crate::proxy_transport::proxy_factory_from_env() {
        builder = builder.with_transport_factory(proxy);
    }

    let bot = builder.build().await;

    let bot = match bot {
        Ok(b) => b,
        Err(e) => {
            error!("failed to build bot for jid={jid}: {e}");
            cleanup_registry(&ctx, &jid).await;
            return;
        }
    };

    let handle = bot.spawn();
    let client = handle.client();

    let session_handle = Arc::new(SessionHandle {
        jid: jid.clone(),
        client: client.clone(),
        cmd_tx: cmd_tx.clone(),
        cancel: cancel.clone(),
    });
    ctx.registry.insert(session_handle.clone());

    // Deliver the task that triggered session creation, UNLESS it was a
    // `pair_code` task consumed above to configure `with_pair_code` — the
    // library already requested the code once the socket came up, so
    // re-running `handle_pair_code` would request a second, duplicate code.
    if let Some(task) = first_task {
        let already_paired_via_builder = task.task_type == TaskType::PairCode;
        if !already_paired_via_builder {
            let _ = cmd_tx.send(SessionCommand::Task(task)).await;
        }
    }

    // Command loop runs as a child task so it can process commands while the
    // bot's run future is being awaited below.
    let cmd_client = client.clone();
    let cmd_cancel = cancel.clone();
    let cmd_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cmd_cancel.cancelled() => break,
                cmd = cmd_rx.recv() => {
                    let Some(cmd) = cmd else { break; };
                    match cmd {
                        SessionCommand::Task(t) => handle_task(&cmd_client, t).await,
                        SessionCommand::Disconnect => {
                            info!("disconnect requested for jid");
                            cmd_client.disconnect().await;
                            break;
                        }
                    }
                }
            }
        }
    });

    let mut handle = handle;
    tokio::select! {
        _ = &mut handle => info!("bot run future completed for jid={jid}"),
        _ = cancel.cancelled() => {
            info!("session cancelled for jid={jid}");
            client.disconnect().await;
        }
    }

    cancel.cancel();
    let _ = cmd_task.await;
    cleanup_registry(&ctx, &jid).await;
    info!("session worker exited for jid={jid}");
}

/// Dispatch a single task to the live `Client`.
async fn handle_task(client: &Arc<whatsapp_rust::Client>, task: TaskEnvelope) {
    let task_id = task.task_id.clone();
    match task.task_type {
        TaskType::Disconnect => {
            client.disconnect().await;
        }
        TaskType::Logout => {
            // logout() dispatches LoggedOut internally; the session teardown
            // path (delete device + unregister) runs from the event handler.
            client.logout().await;
        }
        TaskType::SendMessage => {
            match serde_json::from_value::<SendMessagePayload>(task.payload.clone()) {
                Ok(p) => handle_send_message(client, &task_id, p).await,
                Err(e) => warn!("bad send_message payload for task={task_id}: {e}"),
            }
        }
        TaskType::SendMedia => {
            match serde_json::from_value::<crate::task::SendMediaPayload>(task.payload.clone()) {
                Ok(p) => handle_send_media(client, &task_id, p).await,
                Err(e) => warn!("bad send_media payload for task={task_id}: {e}"),
            }
        }
        TaskType::React => match serde_json::from_value::<ReactPayload>(task.payload.clone()) {
            Ok(p) => handle_react(client, &task_id, p).await,
            Err(e) => warn!("bad react payload for task={task_id}: {e}"),
        },
        TaskType::PairQr => {
            // QR pairing is automatic on connect; nothing extra to do. The
            // PairingQrCode event will be bridged to wa-events.
            info!(
                "pair_qr task for jid={} - QR flows automatically on connect",
                task.jid
            );
        }
        TaskType::PairCode => {
            match serde_json::from_value::<PairCodePayload>(task.payload.clone()) {
                Ok(p) => handle_pair_code(client, &task_id, p).await,
                Err(e) => warn!("bad pair_code payload for task={task_id}: {e}"),
            }
        }
        TaskType::Unknown => {
            warn!(
                "unknown task type for jid={} task={task_id}; dropped",
                task.jid
            );
        }
    }
}

async fn handle_send_message(
    client: &Arc<whatsapp_rust::Client>,
    task_id: &str,
    p: SendMessagePayload,
) {
    let to = match Jid::from_str(&p.to) {
        Ok(j) => j,
        Err(e) => {
            warn!("invalid target jid '{}' for task={task_id}: {e}", p.to);
            return;
        }
    };

    let msg = build_text_message(&p);

    match client.send_message(to, msg).await {
        Ok(r) => info!("sent message for task={task_id} msg_id={}", r.message_id),
        Err(e) => warn!("send_message failed for task={task_id}: {e}"),
    }
}

/// Build a `wa::Message` from a text payload. Pure function for testability.
pub(crate) fn build_text_message(p: &SendMessagePayload) -> wa::Message {
    use wacore::proto_helpers::MessageBuilderExt;
    if let Some(quote) = &p.quote {
        wa::Message {
            extended_text_message: buffa::MessageField::some(wa::message::ExtendedTextMessage {
                text: Some(p.text.clone()),
                context_info: buffa::MessageField::some(wa::ContextInfo {
                    stanza_id: Some(quote.message_id.clone()),
                    participant: Some(quote.chat_jid.clone()),
                    remote_jid: Some(quote.chat_jid.clone()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    } else {
        wa::Message::text(&p.text)
    }
}

async fn handle_send_media(
    client: &Arc<whatsapp_rust::Client>,
    task_id: &str,
    p: crate::task::SendMediaPayload,
) {
    let to = match Jid::from_str(&p.to) {
        Ok(j) => j,
        Err(e) => {
            warn!("invalid target jid '{}' for task={task_id}: {e}", p.to);
            return;
        }
    };
    // Placeholder: forward the opaque JSON as a text message. Media proto
    // deserialization is a follow-up.
    let text = format!("[media message: {}]", p.message_json);
    let msg = wa::Message {
        conversation: Some(text),
        ..Default::default()
    };
    match client.send_message(to, msg).await {
        Ok(r) => info!(
            "sent media placeholder for task={task_id} msg_id={}",
            r.message_id
        ),
        Err(e) => warn!("send_media failed for task={task_id}: {e}"),
    }
}

async fn handle_react(client: &Arc<whatsapp_rust::Client>, task_id: &str, p: ReactPayload) {
    let to = match Jid::from_str(&p.to) {
        Ok(j) => j,
        Err(e) => {
            warn!("invalid target jid '{}' for task={task_id}: {e}", p.to);
            return;
        }
    };
    let msg = build_reaction_message(&p);
    match client.send_message(to, msg).await {
        Ok(r) => info!("sent reaction for task={task_id} msg_id={}", r.message_id),
        Err(e) => warn!("react failed for task={task_id}: {e}"),
    }
}

/// Build a reaction `wa::Message`. Pure function for testability.
pub(crate) fn build_reaction_message(p: &ReactPayload) -> wa::Message {
    let key = wa::MessageKey {
        remote_jid: Some(p.to.clone()),
        id: Some(p.message_id.clone()),
        from_me: Some(false),
        participant: p.participant.clone(),
    };
    wa::Message {
        reaction_message: buffa::MessageField::some(wa::message::ReactionMessage {
            key: buffa::MessageField::some(key),
            text: Some(p.emoji.clone()),
            sender_timestamp_ms: Some(wacore::time::now_millis()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Extract pairing options from a task that created this session, if it is a
/// `pair_code` task. The library then requests the code automatically once the
/// socket is up; the resulting `Event::PairingCode` is bridged to `wa-events`.
fn pair_code_options_from_task(task: Option<&TaskEnvelope>) -> Option<PairCodeOptions> {
    let task = task?;
    if task.task_type != TaskType::PairCode {
        return None;
    }
    let payload = serde_json::from_value::<PairCodePayload>(task.payload.clone()).ok()?;
    Some(PairCodeOptions {
        phone_number: payload.phone_number,
        ..Default::default()
    })
}

/// The bare phone number a `pair_code` task carries, if this session was
/// created by one. Used to key the pair-code Redis entry by phone number.
fn pair_phone_from_task(task: Option<&TaskEnvelope>) -> Option<String> {
    let task = task?;
    if task.task_type != TaskType::PairCode {
        return None;
    }
    let payload = serde_json::from_value::<PairCodePayload>(task.payload.clone()).ok()?;
    Some(payload.phone_number)
}

async fn handle_pair_code(client: &Arc<whatsapp_rust::Client>, task_id: &str, p: PairCodePayload) {
    if client.is_logged_in() {
        info!("pair_code task={task_id} skipped - already logged in");
        return;
    }
    // `pair_with_code` sends an IQ over the live socket, so it must run after
    // the noise handshake. When a session was already running (e.g. the admin
    // retries pairing after a failure), the socket may be reconnecting; wait for
    // it rather than failing with `client is not connected`.
    if let Err(e) = client.wait_for_socket(Duration::from_secs(30)).await {
        warn!("pair_code task={task_id} socket not ready: {e}");
        return;
    }
    let options = PairCodeOptions {
        phone_number: p.phone_number,
        ..Default::default()
    };
    match client.pair_with_code(options).await {
        Ok(code) => info!("pair code generated for task={task_id}: {code}"),
        Err(e) => warn!("pair_with_code failed for task={task_id}: {e}"),
    }
}

async fn cleanup_registry(ctx: &ServerContext, jid: &str) {
    // Best-effort: remove the registry entry if it still points at us.
    if let Some(h) = ctx.registry.get(jid) {
        ctx.registry.remove_if_matching(jid, &h);
    }
    unregister_in_redis(&mut ctx.redis.clone(), jid, &ctx.pod_id).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::QuoteTarget;

    #[test]
    fn build_text_message_plain() {
        let p = SendMessagePayload {
            to: "5559998888@s.whatsapp.net".into(),
            text: "hello".into(),
            quote: None,
        };
        let msg = build_text_message(&p);
        assert_eq!(msg.conversation.as_deref(), Some("hello"));
        assert!(msg.extended_text_message.as_option().is_none());
    }

    #[test]
    fn build_text_message_quote() {
        let p = SendMessagePayload {
            to: "5559998888@s.whatsapp.net".into(),
            text: "reply".into(),
            quote: Some(QuoteTarget {
                chat_jid: "5559998888@s.whatsapp.net".into(),
                message_id: "msg-42".into(),
            }),
        };
        let msg = build_text_message(&p);
        assert!(msg.conversation.is_none());
        let etm = msg.extended_text_message.as_option().unwrap();
        assert_eq!(etm.text.as_deref(), Some("reply"));
        let ctx = etm.context_info.as_option().unwrap();
        assert_eq!(ctx.stanza_id.as_deref(), Some("msg-42"));
        assert_eq!(ctx.remote_jid.as_deref(), Some("5559998888@s.whatsapp.net"));
    }

    #[test]
    fn build_reaction_message_dm() {
        let p = ReactPayload {
            to: "5559998888@s.whatsapp.net".into(),
            message_id: "msg-1".into(),
            emoji: "\u{1f44d}".into(),
            participant: None,
        };
        let msg = build_reaction_message(&p);
        let rm = msg.reaction_message.as_option().unwrap();
        let key = rm.key.as_option().unwrap();
        assert_eq!(key.id.as_deref(), Some("msg-1"));
        assert!(key.participant.is_none());
        assert_eq!(rm.text.as_deref(), Some("\u{1f44d}"));
    }
}
