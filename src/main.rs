use chrono::{Local, Utc};
use log::{error, info};
use std::sync::Arc;
use wacore::proto_helpers::MessageExt;
use wacore::types::events::Event;
use waproto::whatsapp as wa;
use whatsapp_rust::TokioRuntime;
use whatsapp_rust::bot::{Bot, MessageContext};
use whatsapp_rust::pair_code::PairCodeOptions;
use whatsapp_rust::store::SqliteStore;
use whatsapp_rust_tokio_transport::TokioWebSocketTransportFactory;
use whatsapp_rust_ureq_http_client::UreqHttpClient;

const PING_TRIGGER: &str = "🦀ping";
const PONG_TEXT: &str = "🏓 Pong!";
const REACTION_EMOJI: &str = "🏓";

// Usage:
//   cargo run                                      # QR code pairing only
//   cargo run -- --phone 15551234567               # Pair code + QR code (concurrent)
//   cargo run -- -p 15551234567                    # Short form
//   cargo run -- -p 15551234567 --code MYCODE12    # Custom 8-char pair code
//   cargo run -- -p 15551234567 -c MYCODE12        # Short form

fn main() {
    // Load .env if present. Missing file is fine (e.g. production uses real env vars).
    let _ = dotenvy::dotenv();

    let args: Vec<String> = std::env::args().collect();
    let phone_number = parse_arg(&args, "--phone", "-p");
    let custom_code = parse_arg(&args, "--code", "-c");
    let server_mode = args.iter().any(|a| a == "--server") || std::env::var("REDIS_URL").is_ok();

    if let Some(ref phone) = phone_number {
        eprintln!("Phone number provided: {}", phone);
        if let Some(ref code) = custom_code {
            eprintln!("Custom pair code: {}", code);
        }
        eprintln!("Will use pair code authentication (concurrent with QR)");
    }
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(|buf, record| {
            use std::io::Write;
            writeln!(
                buf,
                "{} [{:<5}] [{}] - {}",
                Local::now().format("%H:%M:%S"),
                record.level(),
                record.target(),
                record.args()
            )
        })
        .init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to build tokio runtime");

    if server_mode {
        rt.block_on(run_server());
    } else {
        rt.block_on(run_single(phone_number, custom_code));
    }
}

#[cfg(feature = "postgres-storage")]
async fn run_server() {
    use log::error;
    use std::sync::Arc;
    use whatsapp_rust::server::Server;
    use whatsapp_rust_postgres_storage::PostgresStorageFactory;

    let database_url = match std::env::var("DATABASE_URL") {
        Ok(v) => v,
        Err(_) => {
            error!("DATABASE_URL is required in server mode");
            return;
        }
    };
    let redis_url = match std::env::var("REDIS_URL") {
        Ok(v) => v,
        Err(_) => {
            error!("REDIS_URL is required in server mode");
            return;
        }
    };
    let pod_id = std::env::var("POD_ID").unwrap_or_else(|_| format!("pod-{}", std::process::id()));
    let max_sessions: usize = std::env::var("MAX_SESSIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    info!("pod_id={pod_id} starting in server mode (max_sessions={max_sessions})");

    let factory = PostgresStorageFactory::new(database_url.clone());
    if let Err(e) = factory.run_migrations().await {
        error!("failed to run migrations: {e}");
        return;
    }

    let redis_client = redis::Client::open(redis_url.as_str()).expect("invalid REDIS_URL");
    let redis_conn = redis::aio::ConnectionManager::new(redis_client)
        .await
        .expect("failed to connect to Redis");

    let server = Server::new(
        Arc::new(factory) as Arc<dyn wacore::store::StorageFactory>,
        redis_conn,
        pod_id,
    )
    .with_max_sessions(max_sessions);

    let shutdown = server.shutdown_token();
    #[cfg(feature = "signal")]
    {
        tokio::select! {
            _ = server.run() => info!("server exited"),
            _ = tokio::signal::ctrl_c() => {
                info!("received Ctrl+C, shutting down");
                shutdown.cancel();
            }
        }
    }
    #[cfg(not(feature = "signal"))]
    {
        server.run().await;
    }
}

#[cfg(not(feature = "postgres-storage"))]
async fn run_server() {
    log::error!("server mode requires the `postgres-storage` feature");
}

async fn run_single(phone_number: Option<String>, custom_code: Option<String>) {
    let backend = match SqliteStore::new("whatsapp.db").await {
        Ok(store) => Arc::new(store),
        Err(e) => {
            error!("Failed to create SQLite backend: {}", e);
            return;
        }
    };
    info!("SQLite backend initialized successfully.");

    let transport_factory = TokioWebSocketTransportFactory::new();
    let http_client = UreqHttpClient::new();

    let mut builder = Bot::builder()
        .with_backend(backend)
        .with_transport_factory(transport_factory)
        .with_http_client(http_client)
        .with_runtime(TokioRuntime);

    if let Some(phone) = phone_number {
        builder = builder.with_pair_code(PairCodeOptions {
            phone_number: phone,
            custom_code,
            ..Default::default()
        });
    }

    let mut bot = builder
        .on_event(move |event, client| async move {
            match event {
                Event::PairingQrCode { code, timeout } => {
                    info!("----------------------------------------");
                    info!(
                        "QR code received (valid for {} seconds):",
                        timeout.as_secs()
                    );
                    info!("\n{}\n", code);
                    info!("----------------------------------------");
                }
                Event::PairingCode { code, timeout } => {
                    info!("========================================");
                    info!("PAIR CODE (valid for {} seconds):", timeout.as_secs());
                    info!("Enter this code on your phone:");
                    info!("WhatsApp > Linked Devices > Link a Device");
                    info!("> Link with phone number instead");
                    info!("");
                    info!("    >>> {} <<<", code);
                    info!("");
                    info!("========================================");
                }
                Event::Message(msg, info) => {
                    let ctx = MessageContext {
                        message: msg,
                        info,
                        client,
                    };
                    handle_message(&ctx).await;
                }
                Event::Connected(_) => info!("✅ Bot connected successfully!"),
                Event::LoggedOut(_) => error!("❌ Bot was logged out!"),
                _ => {}
            }
        })
        .build()
        .await
        .expect("Failed to build bot");

    let client = bot.client();

    let bot_handle = match bot.run().await {
        Ok(handle) => handle,
        Err(e) => {
            error!("Bot failed to start: {}", e);
            return;
        }
    };

    #[cfg(feature = "signal")]
    {
        tokio::select! {
            _ = bot_handle => {}
            _ = tokio::signal::ctrl_c() => {
                info!("Received Ctrl+C, shutting down...");
                client.disconnect().await;
            }
        }
    }

    #[cfg(not(feature = "signal"))]
    {
        bot_handle
            .await
            .expect("Bot task should complete without panicking");
    }
}

async fn handle_message(ctx: &MessageContext) {
    if let Some(reply) = build_media_pong(&ctx.message) {
        info!("Received media ping from {}", ctx.info.source.sender);
        if let Err(e) = ctx.send_message(reply).await {
            error!("Failed to send media pong: {}", e);
        }
        return;
    }

    if ctx.message.text_content() == Some(PING_TRIGGER) {
        handle_text_ping(ctx).await;
    }
}

async fn handle_text_ping(ctx: &MessageContext) {
    info!("Received text ping, sending pong...");

    let key = wa::MessageKey {
        remote_jid: Some(ctx.info.source.chat.to_string()),
        id: Some(ctx.info.id.clone()),
        from_me: Some(ctx.info.source.is_from_me),
        participant: ctx
            .info
            .source
            .is_group
            .then(|| ctx.info.source.sender.to_string()),
    };
    let reaction = wa::Message {
        reaction_message: Some(wa::message::ReactionMessage {
            key: Some(key),
            text: Some(REACTION_EMOJI.to_string()),
            sender_timestamp_ms: Some(Utc::now().timestamp_millis()),
            ..Default::default()
        }),
        ..Default::default()
    };
    if let Err(e) = ctx.send_message(reaction).await {
        error!("Failed to send reaction: {}", e);
    }

    let start = std::time::Instant::now();
    let context_info = ctx.build_quote_context();
    let reply = wa::Message {
        extended_text_message: Some(Box::new(wa::message::ExtendedTextMessage {
            text: Some(PONG_TEXT.to_string()),
            context_info: Some(Box::new(context_info)),
            ..Default::default()
        })),
        ..Default::default()
    };

    let sent = match ctx.send_message(reply).await {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to send pong: {}", e);
            return;
        }
    };

    let duration = format!("{:.2?}", start.elapsed());
    info!(
        "Send took {}. Editing message {}...",
        duration, &sent.message_id
    );

    let edit = wa::Message {
        extended_text_message: Some(Box::new(wa::message::ExtendedTextMessage {
            text: Some(format!("{PONG_TEXT}\n`{duration}`")),
            ..Default::default()
        })),
        ..Default::default()
    };
    if let Err(e) = ctx.edit_message(sent.message_id.clone(), edit).await {
        error!("Failed to edit message {}: {}", sent.message_id, e);
    }
}

/// Reuses the original CDN blob, only swaps the caption. Instant regardless of file size.
fn build_media_pong(message: &wa::Message) -> Option<wa::Message> {
    let base = message.get_base_message();

    if let Some(img) = &base.image_message
        && img.caption.as_deref() == Some(PING_TRIGGER)
    {
        return Some(wa::Message {
            image_message: Some(Box::new(wa::message::ImageMessage {
                caption: Some(PONG_TEXT.to_string()),
                ..*img.clone()
            })),
            ..Default::default()
        });
    }
    if let Some(vid) = &base.video_message
        && vid.caption.as_deref() == Some(PING_TRIGGER)
    {
        return Some(wa::Message {
            video_message: Some(Box::new(wa::message::VideoMessage {
                caption: Some(PONG_TEXT.to_string()),
                ..*vid.clone()
            })),
            ..Default::default()
        });
    }
    None
}

fn parse_arg(args: &[String], long: &str, short: &str) -> Option<String> {
    let long_prefix = format!("{}=", long);
    let mut iter = args.iter().skip(1);
    while let Some(arg) = iter.next() {
        if arg == long || arg == short {
            return iter.next().cloned();
        }
        if let Some(value) = arg.strip_prefix(&long_prefix) {
            return Some(value.to_string());
        }
    }
    None
}
