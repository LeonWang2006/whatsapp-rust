//! Multi-session server.
//!
//! `Server` owns the shared tokio runtime, a Redis `ConnectionManager`, a
//! `SessionRegistry`, and a `StorageFactory`. Its `run()` loop does `BRPOP`
//! against the sharded `wa-queue:{i}` keys, hands each dequeued task to
//! `dispatch`, and exits cleanly on `CancellationToken`.
//!
//! Pod model: every pod runs one `Server`. Pods are interchangeable (k8s
//! `Deployment`); session ownership is coordinated via `wa-registry` so any
//! pod can rebuild any session from PostgreSQL.

pub mod dispatcher;
pub mod event_bridge;
pub mod registry;
pub mod session;
pub mod task;

use std::sync::Arc;

use log::{error, info, warn};
use tokio_util::sync::CancellationToken;

use crate::server::dispatcher::dispatch;
use crate::server::registry::SessionRegistry;
use crate::server::task::{QUEUE_PREFIX, TaskEnvelope, inbox_key};
use crate::store::redis_registry::unregister_in_redis;

/// Number of `wa-queue` shards. Must match the producer side. Sharding lets
/// us parallelize `BRPOP` consumers and keeps one slow session from head-of-
/// lining others.
const QUEUE_SHARDS: usize = 16;

pub struct Server {
    storage_factory: Arc<dyn wacore::store::StorageFactory>,
    redis: redis::aio::ConnectionManager,
    pod_id: String,
    registry: SessionRegistry,
    shutdown: CancellationToken,
    /// Hard cap on concurrent sessions per pod. 0 = unlimited.
    max_sessions: usize,
}

impl Server {
    pub fn new(
        storage_factory: Arc<dyn wacore::store::StorageFactory>,
        redis: redis::aio::ConnectionManager,
        pod_id: String,
    ) -> Self {
        Self {
            storage_factory,
            redis,
            pod_id,
            registry: SessionRegistry::new(),
            shutdown: CancellationToken::new(),
            max_sessions: 0,
        }
    }

    /// Set the max concurrent sessions per pod. 0 = unlimited.
    pub fn with_max_sessions(mut self, max: usize) -> Self {
        self.max_sessions = max;
        self
    }

    pub fn registry(&self) -> &SessionRegistry {
        &self.registry
    }

    pub fn pod_id(&self) -> &str {
        &self.pod_id
    }

    /// True when the pod can accept new sessions.
    pub fn is_ready(&self) -> bool {
        if self.max_sessions == 0 {
            return true;
        }
        self.registry.len() < self.max_sessions
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Main consumer loop. Runs until `shutdown` is cancelled, then drains
    /// live sessions.
    pub async fn run(self) {
        let ctx = dispatcher::make_context(
            self.registry.clone(),
            self.storage_factory.clone(),
            self.redis.clone(),
            self.pod_id.clone(),
            self.max_sessions,
        );

        // One BRPOP task per shard. Each pulls tasks off its own shard key.
        let mut consumer_handles = Vec::new();
        for shard in 0..QUEUE_SHARDS {
            let ctxc = ctx.clone();
            let cancel = self.shutdown.clone();
            let key = format!("{QUEUE_PREFIX}:{shard}");
            consumer_handles.push(tokio::spawn(async move {
                shard_consumer(ctxc, key, cancel).await;
            }));
        }

        // Cross-pod inbox consumer: drains tasks forwarded by other pods.
        {
            let ctxc = ctx.clone();
            let cancel = self.shutdown.clone();
            let key = inbox_key(&self.pod_id);
            consumer_handles.push(tokio::spawn(async move {
                inbox_consumer(ctxc, key, cancel).await;
            }));
        }

        // Wait for shutdown signal.
        self.shutdown.cancelled().await;
        info!("shutdown signaled; stopping consumers");

        for h in consumer_handles {
            let _ = h.await;
        }

        // Disconnect all live sessions and release registry leases.
        let sessions = self.registry.snapshot();
        info!("disconnecting {} live session(s)", sessions.len());
        for s in &sessions {
            s.cancel.cancel();
        }
        // Give each disconnect a 10s budget; a hung session must not block
        // pod termination (k8s sends SIGKILL after terminationGracePeriod).
        for s in &sessions {
            let disconnect_fut = s.client.disconnect();
            let timeout_fut =
                tokio::time::timeout(std::time::Duration::from_secs(10), disconnect_fut);
            match timeout_fut.await {
                Ok(()) => {}
                Err(_) => warn!("disconnect timed out for jid={}; forcing cleanup", s.jid),
            }
            let mut r = self.redis.clone();
            unregister_in_redis(&mut r, &s.jid, &self.pod_id).await;
        }

        info!("server exited cleanly");
    }
}

/// Consume one shard until `cancel` fires. Uses blocking `BRPOP` with a short
/// timeout so the cancel is responsive.
async fn shard_consumer(
    ctx: crate::server::session::ServerContext,
    key: String,
    cancel: CancellationToken,
) {
    let mut redis = ctx.redis.clone();
    info!("consuming shard key={key}");
    loop {
        if cancel.is_cancelled() {
            break;
        }
        // 5s timeout: when no message arrives Redis returns nil, which
        // deserializes to None. That is normal, not an error.
        let result: redis::RedisResult<Option<(String, Vec<u8>)>> =
            redis::Cmd::brpop(&key, 5f64).query_async(&mut redis).await;
        match result {
            Ok(Some((_k, payload))) => {
                let task: TaskEnvelope = match serde_json::from_slice(&payload) {
                    Ok(t) => t,
                    Err(e) => {
                        warn!("dropping malformed task on {key}: {e}");
                        continue;
                    }
                };
                dispatch(&ctx, task).await;
            }
            Ok(None) => {} // timeout, no message
            Err(e) => {
                if !cancel.is_cancelled() {
                    error!("BRPOP error on {key}: {e}; backing off");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }
    info!("shard consumer for {key} exiting");
}

/// Consume the per-pod inbox queue. Other pods `LPUSH` tasks here when they
/// receive a task for a session we own. We `BRPOP` and dispatch locally.
async fn inbox_consumer(
    ctx: crate::server::session::ServerContext,
    key: String,
    cancel: CancellationToken,
) {
    let mut redis = ctx.redis.clone();
    info!("consuming pod inbox key={key}");
    loop {
        if cancel.is_cancelled() {
            break;
        }
        let result: redis::RedisResult<Option<(String, Vec<u8>)>> =
            redis::Cmd::brpop(&key, 5f64).query_async(&mut redis).await;
        match result {
            Ok(Some((_k, payload))) => {
                let task: TaskEnvelope = match serde_json::from_slice(&payload) {
                    Ok(t) => t,
                    Err(e) => {
                        warn!("dropping malformed inbox task on {key}: {e}");
                        continue;
                    }
                };
                // Inbox tasks are already routed to us as the owner; dispatch
                // directly (the registry lookup in dispatch() will hit).
                dispatch(&ctx, task).await;
            }
            Ok(None) => {} // timeout, no message
            Err(e) => {
                if !cancel.is_cancelled() {
                    error!("BRPOP error on inbox {key}: {e}; backing off");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }
    info!("inbox consumer for {key} exiting");
}
