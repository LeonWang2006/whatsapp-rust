//! Task dispatch.
//!
//! The dispatcher is the bridge between a dequeued `TaskEnvelope` and the
//! session that should handle it. It consults the local `SessionRegistry`
//! first; on a miss it either spawns a new session (for pairing tasks) or
//! forwards to the owning pod via `wa-registry`.

use log::{info, warn};

use crate::redis_registry::lookup_pod;
use crate::registry::SessionRegistry;
use crate::session::{ServerContext, run_session};
use crate::task::{SessionCommand, TaskEnvelope, inbox_key};

/// Route `task` to the right session. Spawns a new session lazily for pairing
/// tasks; other task types on a missing local session are forwarded to the
/// owning pod via `wa-registry` + per-pod inbox queue.
pub async fn dispatch(ctx: &ServerContext, task: TaskEnvelope) {
    let jid = task.jid.clone();
    let is_pairing = task.task_type.is_pairing();

    // Fast path: session already live on this pod.
    if let Some(handle) = ctx.registry.get(&jid) {
        match handle.cmd_tx.send(SessionCommand::Task(task)).await {
            Ok(()) => return,
            Err(_) => {
                // Session died between get() and send(); fall through to slow
                // path. task was moved into send() so we can't forward it -
                // but we can rebuild a lookup to decide whether to respawn.
                ctx.registry.remove_if_matching(&jid, &handle);
            }
        }
        // If we get here the send failed and ate the task. We can't recover
        // it, so just return - the producer's ack timeout will requeue.
        warn!("cmd channel closed for jid={jid}; task lost (producer will requeue on timeout)");
        return;
    }

    // No local session. Decide based on task type and registry ownership.
    if is_pairing {
        // Respect the per-pod session cap. If full, drop and let another pod
        // pick it up (or the producer requeue on timeout).
        if ctx.max_sessions > 0 && ctx.registry.len() >= ctx.max_sessions {
            warn!(
                "pod at session cap ({}/{}); dropping pairing task for jid={jid}",
                ctx.registry.len(),
                ctx.max_sessions
            );
            return;
        }
        info!("spawning new session for jid={jid}");
        let ctx_clone = ctx.clone();
        tokio::spawn(async move {
            run_session(ctx_clone, jid, Some(task)).await;
        });
        return;
    }

    // Non-pairing task with no local session: consult wa-registry.
    let mut redis = ctx.redis.clone();
    match lookup_pod(&mut redis, &jid).await {
        Some(owner) if owner == ctx.pod_id => {
            // Registry says we own it but local map disagrees: respawn.
            info!("registry self-owned but no local session for jid={jid}; respawning");
            let ctx_clone = ctx.clone();
            tokio::spawn(async move {
                run_session(ctx_clone, jid, Some(task)).await;
            });
        }
        Some(owner) => {
            // Forward to the owning pod's inbox queue.
            let key = inbox_key(&owner);
            let payload = match serde_json::to_vec(&task) {
                Ok(b) => b,
                Err(e) => {
                    warn!("failed to serialize task for forward: {e}");
                    return;
                }
            };
            match redis::Cmd::lpush(&key, payload)
                .query_async::<()>(&mut redis)
                .await
            {
                Ok(()) => info!("forwarded task for jid={jid} to pod={owner} inbox"),
                Err(e) => warn!("failed to LPUSH inbox for pod={owner}: {e}"),
            }
        }
        None => {
            warn!("task for jid={jid} has no owner and is not a pairing task; dropped");
        }
    }
}

/// Build a `ServerContext` convenience constructor.
pub fn make_context(
    registry: SessionRegistry,
    storage_factory: std::sync::Arc<dyn crate::storage_factory::StorageFactory>,
    redis: redis::aio::ConnectionManager,
    redis_client: redis::Client,
    pod_id: String,
    max_sessions: usize,
) -> ServerContext {
    ServerContext {
        registry,
        storage_factory,
        redis,
        redis_client,
        pod_id,
        max_sessions,
    }
}
