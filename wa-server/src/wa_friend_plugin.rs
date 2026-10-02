use log::{info, warn};
use reqwest::Client as HttpClient;
use std::sync::Arc;
use wacore::types::events::{Event, EventHandler, EventInterest};
use whatsapp_rust::plugins::{
    ClientPlugin, PluginCapability, PluginContext, PluginFuture, PluginManifest,
};

pub struct WaFriendBridgePlugin {
    webhook_url: String,
    http_client: HttpClient,
}

impl WaFriendBridgePlugin {
    pub fn new(webhook_url: String, http_client: HttpClient) -> Self {
        Self {
            webhook_url,
            http_client,
        }
    }
}

struct WaFriendEventHandler {
    webhook_url: String,
    http_client: HttpClient,
}

impl EventHandler for WaFriendEventHandler {
    fn handle_event(&self, event: Arc<Event>) {
        let webhook_url = self.webhook_url.clone();
        let http_client = self.http_client.clone();

        // Since handle_event is synchronous, we spawn a task for the async HTTP call.
        tokio::spawn(async move {
            let payload = serde_json::json!({
                "event": format!("{:?}", event),
                "details": format!("{:?}", event),
            });

            if let Err(e) = http_client.post(&webhook_url).json(&payload).send().await {
                warn!("wa-friend-bridge: failed to send event webhook: {e}");
            }
        });
    }

    fn interest(&self) -> EventInterest {
        EventInterest::ALL
    }
}

impl ClientPlugin for WaFriendBridgePlugin {
    type Api = ();

    fn manifest(&self) -> PluginManifest {
        PluginManifest::new("wa-friend-bridge").with_capability(PluginCapability::CoreEvents)
    }

    async fn install(&self, context: PluginContext) -> anyhow::Result<Arc<Self::Api>> {
        let core_events = context
            .core_events
            .ok_or_else(|| anyhow::anyhow!("core events missing"))?;

        let handler = Arc::new(WaFriendEventHandler {
            webhook_url: self.webhook_url.clone(),
            http_client: self.http_client.clone(),
        });

        core_events.subscribe(EventInterest::ALL, handler)?;

        info!("wa-friend-bridge: plugin installed and subscribed to all core events");

        Ok(Arc::new(()))
    }
}
