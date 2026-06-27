// Comprehensive demo showcasing whatsapp-rust library features
// This example demonstrates:
// - QR code pairing
// - Sending and receiving messages
// - Contact checks and info fetching
// - Group operations
// - Chat state indicators (typing, recording, etc.)
// - Message reactions and replies
// - Chat management (archive, mute, pin, etc.)
// - Blocking contacts
// - Presence and online status

use chrono::Local;
use log::{error, info, warn};
use std::sync::Arc;
use wacore::proto_helpers::MessageExt;
use wacore::types::events::Event;
use waproto::whatsapp as wa;
use whatsapp_rust::TokioRuntime;
use whatsapp_rust::bot::{Bot, MessageContext};
use whatsapp_rust::store::SqliteStore;
use whatsapp_rust_tokio_transport::TokioWebSocketTransportFactory;
use whatsapp_rust_ureq_http_client::UreqHttpClient;

fn main() {
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

    rt.block_on(async {
        if let Err(e) = run_demo().await {
            error!("Demo failed: {}", e);
        }
    });
}

async fn run_demo() -> anyhow::Result<()> {
    info!("Starting WhatsApp Rust Comprehensive Demo");

    // Initialize SQLite backend for persistent sessions
    let backend = Arc::new(SqliteStore::new("whatsapp_demo.db").await.map_err(|e| {
        error!("Failed to create SQLite backend: {}", e);
        e
    })?);
    info!("SQLite backend initialized");

    // Setup WebSocket transport with proxy configuration
    let transport_factory =
        TokioWebSocketTransportFactory::new().with_proxy("http://127.0.0.1:7890");
    let http_client = UreqHttpClient::new();

    // Build the bot with event handler
    let mut bot = Bot::builder()
        .with_backend(backend)
        .with_transport_factory(transport_factory)
        .with_http_client(http_client)
        .with_runtime(TokioRuntime)
        .on_event(move |event, client| async move {
            if let Err(e) = handle_event(event, client).await {
                error!("Error in event handler: {}", e);
            }
        })
        .build()
        .await
        .map_err(|e| {
            error!("Failed to build bot: {}", e);
            anyhow::anyhow!("{}", e)
        })?;

    info!("Bot configured successfully");

    // Run the bot (blocking until disconnection)
    let bot_handle = bot.run().await?;
    bot_handle.await?;

    Ok(())
}

// Event handler demonstrating various operations
async fn handle_event(
    event: Event,
    client: Arc<whatsapp_rust::client::Client>,
) -> anyhow::Result<()> {
    match event {
        // Connection Events
        Event::PairingQrCode { code, timeout } => {
            info!("QR Code for pairing (expires in {}s):", timeout.as_secs());
            println!("{}", code);
        }

        Event::PairingCode { code, timeout } => {
            info!(
                "Pairing code for device linking (expires in {}s): {}",
                timeout.as_secs(),
                code
            );
        }

        Event::Connected(_) => {
            info!("Bot connected to WhatsApp servers");
            demo_connection_operations(&client).await;
        }

        Event::LoggedOut(_) => {
            error!("Bot was logged out from WhatsApp");
        }

        Event::Disconnected(_) => {
            warn!("Bot disconnected (will auto-reconnect)");
        }

        // Message Events
        Event::Message(msg, info) => {
            let direction = if info.source.is_from_me {
                "SENT"
            } else {
                "RECEIVED"
            };
            info!(
                "Message [{}]: from={}, message_id={}",
                direction, info.source.sender, info.id
            );

            let ctx = MessageContext {
                message: msg.clone(),
                info: info.clone(),
                client: client.clone(),
            };

            // Handle text messages with example commands
            if let Some(text) = msg.text_content() {
                if let Err(e) = handle_text_message(&text, &ctx).await {
                    error!("Error handling text message: {}", e);
                }
            }

            // Handle reactions
            if msg.reaction_message.is_some() {
                info!("This is a reaction/emoji message");
            }
        }

        Event::Receipt(receipt) => {
            info!(
                "Receipt from {:?}: {:?}",
                receipt.source.sender, receipt.r#type
            );
        }

        // Group Events
        Event::GroupUpdate(group_update) => {
            info!("Group update for: {}", group_update.group_jid);
        }

        Event::JoinedGroup(_group) => {
            info!("Joined a new group");
        }

        // Presence Events
        Event::Presence(presence_info) => {
            let status = if presence_info.unavailable {
                "offline"
            } else {
                "online"
            };
            info!("Presence from {}: {}", presence_info.from, status);
        }

        Event::ChatPresence(chat_presence) => {
            info!(
                "Chat presence from {:?}: {:?}",
                chat_presence.source.sender, chat_presence.state
            );
        }

        Event::PictureUpdate(pic_update) => {
            info!("Picture updated for: {}", pic_update.jid);
        }

        _ => {
            // Handle other events silently
        }
    }
    Ok(())
}

// Handle text message commands
async fn handle_text_message(text: &str, ctx: &MessageContext) -> anyhow::Result<()> {
    let args: Vec<&str> = text.trim().split_whitespace().collect();
    if args.is_empty() {
        return Ok(());
    }

    match args[0] {
        // Message Commands
        "ping" => {
            info!("Received ping command");
            ctx.send_text_reply("Pong!").await?;
        }

        "echo" => {
            let message = args[1..].join(" ");
            ctx.send_text_reply(&format!("Echo: {}", message)).await?;
        }

        // Chat Management Commands
        "mute" => {
            info!("Muting chat indefinitely");
            match ctx
                .client
                .chat_actions()
                .mute_chat(&ctx.info.source.chat)
                .await
            {
                Ok(_) => {
                    ctx.send_text_reply("Chat muted").await?;
                }
                Err(e) => error!("Failed to mute chat: {}", e),
            }
        }

        "unmute" => {
            info!("Unmuting chat");
            match ctx
                .client
                .chat_actions()
                .unmute_chat(&ctx.info.source.chat)
                .await
            {
                Ok(_) => {
                    ctx.send_text_reply("Chat unmuted").await?;
                }
                Err(e) => error!("Failed to unmute chat: {}", e),
            }
        }

        "archive" => {
            info!("Archiving chat");
            match ctx
                .client
                .chat_actions()
                .archive_chat(&ctx.info.source.chat, None)
                .await
            {
                Ok(_) => {
                    ctx.send_text_reply("Chat archived").await?;
                }
                Err(e) => error!("Failed to archive chat: {}", e),
            }
        }

        "unarchive" => {
            info!("Unarchiving chat");
            match ctx
                .client
                .chat_actions()
                .unarchive_chat(&ctx.info.source.chat, None)
                .await
            {
                Ok(_) => {
                    ctx.send_text_reply("Chat unarchived").await?;
                }
                Err(e) => error!("Failed to unarchive chat: {}", e),
            }
        }

        "pin" => {
            info!("Pinning chat");
            match ctx
                .client
                .chat_actions()
                .pin_chat(&ctx.info.source.chat)
                .await
            {
                Ok(_) => {
                    ctx.send_text_reply("Chat pinned").await?;
                }
                Err(e) => error!("Failed to pin chat: {}", e),
            }
        }

        "unpin" => {
            info!("Unpinning chat");
            match ctx
                .client
                .chat_actions()
                .unpin_chat(&ctx.info.source.chat)
                .await
            {
                Ok(_) => {
                    ctx.send_text_reply("Chat unpinned").await?;
                }
                Err(e) => error!("Failed to unpin chat: {}", e),
            }
        }

        // Presence Commands
        "online" => {
            info!("Setting online presence");
            if let Err(e) = ctx.client.presence().set_available().await {
                error!("Failed to set online: {}", e);
            }
        }

        "offline" => {
            info!("Setting offline presence");
            if let Err(e) = ctx.client.presence().set_unavailable().await {
                error!("Failed to set offline: {}", e);
            }
        }

        // Chat State Commands
        "typing" => {
            info!("Sending typing indicator");
            if let Err(e) = ctx
                .client
                .chatstate()
                .send_composing(&ctx.info.source.chat)
                .await
            {
                error!("Failed to send typing: {}", e);
            }
        }

        "recording" => {
            info!("Sending recording indicator");
            if let Err(e) = ctx
                .client
                .chatstate()
                .send_recording(&ctx.info.source.chat)
                .await
            {
                error!("Failed to send recording: {}", e);
            }
        }

        // Contact Commands
        "whoami" => {
            info!("Getting bot info");
            let push_name = ctx.client.get_push_name().await;
            if let (Some(pn), Some(lid)) = (ctx.client.get_pn().await, ctx.client.get_lid().await) {
                let reply = format!("Bot Info:\nName: {}\nPN: {}\nLID: {}", push_name, pn, lid);
                ctx.send_text_reply(&reply).await?;
            } else {
                ctx.send_text_reply("Could not retrieve bot info").await?;
            }
        }

        // Group Commands
        "groupinfo" => {
            if ctx.info.source.chat.server != "g.us" {
                ctx.send_text_reply("This command only works in groups")
                    .await?;
                return Ok(());
            }

            info!("Fetching group info for {}", ctx.info.source.chat);
            match ctx.client.groups().query_info(&ctx.info.source.chat).await {
                Ok(group_info) => {
                    let participant_count = group_info.participants.len();
                    let reply = format!("Group Info:\nParticipants: {}", participant_count);
                    ctx.send_text_reply(&reply).await?;
                }
                Err(e) => {
                    error!("Failed to fetch group info: {}", e);
                    ctx.send_text_reply("Failed to fetch group info").await?;
                }
            }
        }

        "groups" => {
            info!("Fetching list of participating groups");
            match ctx.client.groups().get_participating().await {
                Ok(groups_map) => {
                    let count = groups_map.len();
                    let reply = if count > 0 {
                        let group_list = groups_map
                            .iter()
                            .take(5)
                            .map(|(_, g)| format!("- {}", g.subject))
                            .collect::<Vec<_>>()
                            .join("\n");
                        format!("Your groups ({}):\n{}", count, group_list)
                    } else {
                        "You are not in any groups".to_string()
                    };
                    ctx.send_text_reply(&reply).await?;
                }
                Err(e) => {
                    error!("Failed to fetch groups: {}", e);
                    ctx.send_text_reply("Failed to fetch groups").await?;
                }
            }
        }

        // Help Command
        "help" => {
            let help_text = r#"Available Commands:

Messages:
  ping - Test bot responsiveness
  echo <text> - Echo back your message

Chat Management:
  mute - Mute chat indefinitely
  unmute - Unmute chat
  archive - Archive chat
  unarchive - Unarchive chat
  pin - Pin chat
  unpin - Unpin chat

Presence:
  online - Set status to online
  offline - Set status to offline

Chat State:
  typing - Send typing indicator
  recording - Send recording indicator

Contacts & Groups:
  whoami - Get bot information
  groupinfo - Get current group info
  groups - List your groups

Help:
  help - Show this message"#;
            ctx.send_text_reply(help_text).await?;
        }

        _ => {
            info!("Unknown command: {}", args[0]);
            ctx.send_text_reply("Unknown command. Type 'help' for available commands.")
                .await?;
        }
    }
    Ok(())
}

// Demonstrate operations that can be done when bot connects
async fn demo_connection_operations(client: &Arc<whatsapp_rust::client::Client>) {
    info!("Demonstrating connection-time operations...");

    // Get bot's own information
    let push_name = client.get_push_name().await;
    if let (Some(pn), Some(lid)) = (client.get_pn().await, client.get_lid().await) {
        info!("Bot logged in as: {} (PN: {}, LID: {})", push_name, pn, lid);
    } else {
        warn!("Could not retrieve complete bot information");
        info!("Bot name: {}", push_name);
    }

    // Set online presence
    if let Err(e) = client.presence().set_available().await {
        warn!("Failed to set online presence: {}", e);
    } else {
        info!("Presence set to online");
    }
}

// Helper trait for convenient message sending
trait MessageContextExt {
    async fn send_text_reply(&self, text: &str) -> anyhow::Result<String>;
}

impl MessageContextExt for MessageContext {
    async fn send_text_reply(&self, text: &str) -> anyhow::Result<String> {
        let reply_message = wa::Message {
            conversation: Some(text.to_string()),
            ..Default::default()
        };

        match self.send_message(reply_message).await {
            Ok(send_result) => Ok(send_result.message_id),
            Err(e) => Err(e),
        }
    }
}
