//! `wa-server` binary entrypoint.
//!
//! Reads configuration from the environment (`.env` supported via the shell),
//! builds the storage factory, connects to Redis, and runs the shard
//! consumers + HTTP API.

use std::net::SocketAddr;
use std::sync::Arc;

use log::{error, info, warn};
use tokio_util::sync::CancellationToken;
use wa_server::in_memory_factory::InMemoryStorageFactory;
use wa_server::server::Server;
use wa_server::storage_factory::StorageFactory;
use wa_server::task::{LINK_STATUS_KEY_PREFIX, PAIR_CODE_KEY_PREFIX};

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_or_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn init_logging() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(|buf, record| {
            use std::io::Write;
            writeln!(
                buf,
                "{} [{:<5}] [{}] - {}",
                wacore::time::now_utc().format("%H:%M:%S"),
                record.level(),
                record.target(),
                record.args()
            )
        })
        .init();
}

/// Adapts [`whatsapp_rust_postgres_storage::PostgresStorageFactory`]'s inherent
/// methods to [`wa_server::StorageFactory`]. This lives in the server crate (not
/// the storage crate) so the storage crate never depends on the server crate.
struct PostgresStorageFactoryAdapter(whatsapp_rust_postgres_storage::PostgresStorageFactory);

#[async_trait::async_trait]
impl StorageFactory for PostgresStorageFactoryAdapter {
    async fn for_jid(&self, jid: &str) -> Option<Arc<dyn wacore::store::traits::Backend>> {
        self.0.for_jid(jid).await
    }

    async fn for_device_id(
        &self,
        device_id: i32,
    ) -> Option<Arc<dyn wacore::store::traits::Backend>> {
        self.0.backend_for_device_id(device_id)
    }

    async fn create_for_jid(
        &self,
        jid: &str,
    ) -> anyhow::Result<(i32, Arc<dyn wacore::store::traits::Backend>)> {
        self.0.create_for_jid(jid).await
    }

    async fn delete_for_jid(&self, jid: &str) -> anyhow::Result<()> {
        self.0.delete_for_jid(jid).await
    }

    async fn all_jids(&self) -> anyhow::Result<Vec<String>> {
        self.0.all_jids().await.map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn biz_user_id_by_phone(&self, phone: &str) -> anyhow::Result<Option<i64>> {
        self.0
            .biz_user_by_phone(phone)
            .await
            .map(|u| u.map(|u| u.id))
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn biz_contacts_for_user(&self, user_id: i64) -> anyhow::Result<Vec<String>> {
        self.0
            .biz_contacts_for_user(user_id)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn record_presence_event(
        &self,
        owner_phone: &str,
        contact_phone: &str,
        event_type: &str,
        ts: i64,
        last_seen: Option<i64>,
    ) -> anyhow::Result<()> {
        self.0
            .record_presence_event(owner_phone, contact_phone, event_type, ts, last_seen)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn query_presence_events(
        &self,
        owner_phone: &str,
        contact_phone: &str,
        start: i64,
        end: i64,
    ) -> anyhow::Result<Vec<wa_server::task::PresenceEvent>> {
        self.0
            .query_presence_events(owner_phone, contact_phone, start, end)
            .await
            .map(|evts| {
                evts.into_iter()
                    .map(|e| wa_server::task::PresenceEvent {
                        owner_phone: e.owner_phone,
                        contact_phone: e.contact_phone,
                        event_type: e.event_type,
                        ts: e.ts,
                        last_seen: e.last_seen,
                    })
                    .collect()
            })
            .map_err(|e| anyhow::anyhow!("{e}"))
    }
}

#[tokio::main]
async fn main() {
    init_logging();

    let redis_url = env_or("REDIS_URL", "redis://127.0.0.1:6379");
    let pod_id = env_or("POD_ID", "pod-1");
    let api_addr = env_or("API_ADDR", "0.0.0.0:8080");
    let max_sessions = env_or_parse("MAX_SESSIONS", 0usize);
    let pair_code_key_prefix = env_or("PAIR_CODE_KEY_PREFIX", PAIR_CODE_KEY_PREFIX);
    let link_status_key_prefix = env_or("LINK_STATUS_KEY_PREFIX", LINK_STATUS_KEY_PREFIX);
    let restore_on_start = env_or_parse("RESTORE_ON_START", true);

    info!(
        "starting wa-server pod={pod_id} api={api_addr} max_sessions={max_sessions} redis={redis_url} restore_on_start={restore_on_start}"
    );

    // Connect to Redis.
    let client = match redis::Client::open(redis_url.as_str()) {
        Ok(c) => c,
        Err(e) => {
            error!("failed to parse REDIS_URL: {e}");
            std::process::exit(1);
        }
    };
    let redis = match client.get_connection_manager().await {
        Ok(m) => m,
        Err(e) => {
            error!("failed to connect to Redis: {e}");
            std::process::exit(1);
        }
    };

    // Storage factory. PostgreSQL when DATABASE_URL is set (production), else
    // in-memory for local smoke tests without a database.
    let storage_factory: Arc<dyn StorageFactory> = match std::env::var("DATABASE_URL") {
        Ok(url) => {
            let factory = whatsapp_rust_postgres_storage::PostgresStorageFactory::new(url);
            info!("storage: postgres (DATABASE_URL set); running migrations");
            if let Err(e) = factory.run_migrations().await {
                error!("postgres migration failed: {e}");
                std::process::exit(1);
            }
            Arc::new(PostgresStorageFactoryAdapter(factory))
        }
        Err(_) => {
            warn!("DATABASE_URL not set; using in-memory storage (state lost on restart)");
            Arc::new(InMemoryStorageFactory::new())
        }
    };

    let shutdown = CancellationToken::new();
    let shutdown_for_signal = shutdown.clone();
    let shutdown_for_api = shutdown.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        info!("shutdown signal received; stopping server");
        shutdown_for_signal.cancel();
    });

    // Build the server. Clone its registry out up-front so the API shares the
    // same in-process session map the shard consumers write to. The raw client
    // is carried into `ServerContext` so each blocking consumer can open its
    // own dedicated connection.
    let server = Server::new(
        storage_factory.clone(),
        redis.clone(),
        client.clone(),
        pod_id.clone(),
        pair_code_key_prefix.clone(),
        link_status_key_prefix.clone(),
    )
    .with_max_sessions(max_sessions)
    .with_shutdown(shutdown.clone());
    let api_registry = server.registry().clone();

    let server_task = tokio::spawn(async move {
        server.run().await;
    });

    // Restore previously-paired devices so a freshly-started pod reconnects
    // without waiting for an external task to target them. Spawned *before* the
    // API so sessions are already coming online when the pod reports ready.
    // `run_session` registers each JID in `wa-registry` first and aborts when
    // another pod holds the lease, so multi-pod double-connection is prevented
    // by construction. Disabled with RESTORE_ON_START=false (e.g. a fresh pod
    // that must not grab every device in the fleet on startup).
    let restore_ctx = wa_server::session::ServerContext {
        registry: api_registry.clone(),
        storage_factory: storage_factory.clone(),
        redis: redis.clone(),
        redis_client: client.clone(),
        pod_id: pod_id.clone(),
        max_sessions,
        pair_code_key_prefix: pair_code_key_prefix.clone(),
        link_status_key_prefix: link_status_key_prefix.clone(),
    };
    if restore_on_start {
        match storage_factory.all_jids().await {
            Ok(jids) => {
                info!("restore: {} paired device(s)", jids.len());
                for jid in jids {
                    info!("restore: respawning session for jid={jid}");
                    let ctx = restore_ctx.clone();
                    tokio::spawn(async move {
                        wa_server::session::run_session(ctx, jid, None).await;
                    });
                }
            }
            Err(e) => warn!("restore: failed to enumerate paired devices: {e}"),
        }
    } else {
        info!("restore: disabled (RESTORE_ON_START=false)");
    }

    // Run the API on its own task, sharing the server's registry so `/status`
    // reflects live sessions and local dispatch hits the same command channels.
    let ctx = wa_server::session::ServerContext {
        registry: api_registry,
        storage_factory,
        redis,
        redis_client: client,
        pod_id,
        max_sessions,
        pair_code_key_prefix,
        link_status_key_prefix,
    };
    let api_addr: SocketAddr = match api_addr.parse() {
        Ok(a) => a,
        Err(e) => {
            error!("invalid API_ADDR {api_addr:?}: {e}");
            std::process::exit(1);
        }
    };
    let api_task = tokio::spawn(async move {
        let api = wa_server::Api::new(ctx);
        let api_shutdown = shutdown_for_api.clone();
        if let Err(e) = api.start(api_addr, api_shutdown).await {
            error!("API server error: {e}");
        }
    });

    // Wait for shutdown.
    shutdown.cancelled().await;
    info!("shutdown: waiting for server + api tasks to finish");
    let _ = server_task.await;
    let _ = api_task.await;
    info!("wa-server exited cleanly");
}

async fn wait_for_shutdown_signal() {
    use tokio::signal;
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("received Ctrl+C"),
        _ = terminate => info!("received SIGTERM"),
    }
}
