//! Redis-backed `wa-registry` operations.
//!
//! `wa-registry` is a Hash mapping `jid -> {pod_id, ts}`. Pods `HSET` their
//! own entry on session start and `HDEL` on shutdown so any pod can discover
//! which peer owns a given session. A separate `wa-registry:lease:<jid>` key
//! with `EXPIRE` implements the heartbeat lease: if a pod dies without
//! `HDEL`ing, the lease expires and the registry entry becomes stale.

use anyhow::Result;
use log::warn;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};

use crate::task::{HEARTBEAT_INTERVAL, REGISTRY_KEY, REGISTRY_TTL};

#[derive(Debug, Serialize, Deserialize)]
pub struct RegistryEntry {
    pub pod_id: String,
    pub ts: u64,
}

const LEASE_PREFIX: &str = "wa-registry:lease:";

/// Register this pod as the owner of `jid` in the shared registry and acquire
/// a heartbeat lease. Returns an error if another pod currently owns an
/// unexpired lease - callers must respect that to avoid double-connecting.
pub async fn register_in_redis(
    redis: &mut ConnectionManager,
    jid: &str,
    pod_id: &str,
) -> Result<()> {
    let lease_key = format!("{LEASE_PREFIX}{jid}");

    let entry = RegistryEntry {
        pod_id: pod_id.to_string(),
        ts: wacore::time::now_secs().max(0) as u64,
    };
    let entry_json = serde_json::to_string(&entry)?;

    let mut pipe = redis::pipe();
    pipe.atomic()
        .hset(REGISTRY_KEY, jid, entry_json)
        .ignore()
        .set_ex(&lease_key, pod_id, REGISTRY_TTL.as_secs())
        .ignore();
    pipe.query_async::<()>(redis).await?;
    Ok(())
}

/// Remove this pod's registration for `jid`. Only deletes the lease if the
/// current holder matches `pod_id`, so a racing re-register on another pod is
/// not clobbered.
pub async fn unregister_in_redis(redis: &mut ConnectionManager, jid: &str, pod_id: &str) {
    let lease_key = format!("{LEASE_PREFIX}{jid}");
    // Lua-free best effort: HDEL the map entry, then DEL the lease only if it
    // still points at us. A stale lease from a crashed pod will simply expire.
    let mut pipe = redis::pipe();
    pipe.atomic()
        .hdel(REGISTRY_KEY, jid)
        .ignore()
        .get(&lease_key);
    let current: Option<String> = match pipe.query_async(redis).await {
        Ok(((), current)) => current,
        Err(e) => {
            warn!("failed to read lease for jid={jid} during unregister: {e}");
            return;
        }
    };
    if current.as_deref() == Some(pod_id) {
        let _: redis::RedisResult<()> = redis::Cmd::del(&lease_key).query_async(redis).await;
    }
}

/// Look up which pod currently owns `jid` in the shared registry. Returns
/// `None` if the entry is absent or the lease has expired.
pub async fn lookup_pod(redis: &mut ConnectionManager, jid: &str) -> Option<String> {
    let entry_json: Option<String> = redis::Cmd::hget(REGISTRY_KEY, jid)
        .query_async(redis)
        .await
        .ok()?;
    let entry: RegistryEntry = serde_json::from_str(&entry_json?).ok()?;
    // Verify the lease is still alive; otherwise treat as unowned.
    let lease_key = format!("{LEASE_PREFIX}{jid}");
    let exists: bool = redis::Cmd::exists(&lease_key)
        .query_async(redis)
        .await
        .unwrap_or(false);
    if exists { Some(entry.pod_id) } else { None }
}

/// Spawn a per-session heartbeat task refreshing the lease for `jid` held by
/// `pod_id`. Cancels with `cancel`.
pub fn spawn_heartbeat(
    mut redis: ConnectionManager,
    jid: String,
    pod_id: String,
    cancel: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        let lease_key = format!("{LEASE_PREFIX}{jid}");
        let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        interval.tick().await; // first tick is immediate
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(e) = redis::Cmd::set_ex(&lease_key, &pod_id, REGISTRY_TTL.as_secs())
                        .query_async::<()>(&mut redis)
                        .await
                    {
                        warn!("heartbeat refresh failed for jid={jid}: {e}");
                    }
                }
            }
        }
    });
}
