//! HTTP CONNECT proxy transport for WhatsApp WebSocket connections.
//!
//! Some deployments sit behind an HTTP proxy (e.g. a local Clash on
//! `127.0.0.1:7890`) and cannot reach `g.whatsapp.net` directly. This
//! [`TransportFactory`] dials the proxy, issues `CONNECT host:443`, then runs
//! TLS + the WebSocket upgrade over the tunnelled stream — the same shape the
//! default [`TokioWebSocketTransportFactory`] produces, so it drops straight
//! into `BotBuilder::with_transport_factory`.
//!
//! The proxy URL comes from the `WA_PROXY_URL` env var (e.g.
//! `http://127.0.0.1:7890`). When unset, [`ProxyTransportFactory::new`]
//! returns `None` and the caller keeps the default transport.

use std::sync::Arc;

use anyhow::anyhow;
use log::info;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use wacore::net::{TransportFactory, WHATSAPP_WEB_WS_URL};

/// Build a proxy transport factory from `WA_PROXY_URL`, or `None` when the env
/// var is absent/empty (the caller then uses the default direct transport).
pub fn proxy_factory_from_env() -> Option<ProxyTransportFactory> {
    let url = std::env::var("WA_PROXY_URL").ok()?;
    let url = url.trim();
    if url.is_empty() {
        return None;
    }
    info!("wa-server: using HTTP proxy {url} for WhatsApp transport");
    Some(ProxyTransportFactory::new(url.to_string()))
}

/// A [`TransportFactory`] that reaches WhatsApp through an HTTP CONNECT proxy.
pub struct ProxyTransportFactory {
    proxy_url: String,
}

impl ProxyTransportFactory {
    pub fn new(proxy_url: String) -> Self {
        Self { proxy_url }
    }
}

#[async_trait::async_trait]
impl TransportFactory for ProxyTransportFactory {
    async fn create_transport(
        &self,
    ) -> Result<
        (
            Arc<dyn wacore::net::Transport>,
            async_channel::Receiver<wacore::net::TransportEvent>,
        ),
        anyhow::Error,
    > {
        let uri: http::Uri = WHATSAPP_WEB_WS_URL
            .parse()
            .map_err(|e| anyhow!("failed to parse WhatsApp WS URL {WHATSAPP_WEB_WS_URL}: {e}"))?;
        let host = uri
            .host()
            .ok_or_else(|| anyhow!("WhatsApp WS URL has no host: {WHATSAPP_WEB_WS_URL}"))?;
        let port = uri.port_u16().unwrap_or(443);

        let tunnel = connect_via_http_proxy(&self.proxy_url, host, port).await?;
        let tls = wrap_tls(tunnel, host).await?;

        let builder = tokio_websockets::ClientBuilder::from_uri(uri);
        let (ws, _) = builder
            .connect_on(tls)
            .await
            .map_err(|e| anyhow!("WebSocket upgrade through proxy failed: {e}"))?;

        Ok(whatsapp_rust_tokio_transport::from_websocket(ws))
    }
}

/// Connect to `host:port` through an HTTP CONNECT proxy at `proxy_url`.
async fn connect_via_http_proxy(
    proxy_url: &str,
    host: &str,
    port: u16,
) -> Result<TcpStream, anyhow::Error> {
    let proxy_uri: http::Uri = proxy_url
        .parse()
        .map_err(|e| anyhow!("failed to parse WA_PROXY_URL {proxy_url:?}: {e}"))?;
    let proxy_host = proxy_uri
        .host()
        .ok_or_else(|| anyhow!("WA_PROXY_URL has no host: {proxy_url:?}"))?;
    let proxy_port = proxy_uri.port_u16().unwrap_or(80);

    let mut stream = TcpStream::connect((proxy_host, proxy_port))
        .await
        .map_err(|e| anyhow!("failed to connect to proxy {proxy_host}:{proxy_port}: {e}"))?;

    let connect_req = format!(
        "CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\nProxy-Connection: keep-alive\r\n\r\n"
    );
    stream
        .write_all(connect_req.as_bytes())
        .await
        .map_err(|e| anyhow!("failed to write CONNECT to proxy: {e}"))?;
    stream
        .flush()
        .await
        .map_err(|e| anyhow!("failed to flush CONNECT to proxy: {e}"))?;

    // Read the proxy's response line + headers. A 200 means the tunnel is up.
    let mut buf = [0u8; 4096];
    let n = stream
        .read(&mut buf)
        .await
        .map_err(|e| anyhow!("failed to read proxy CONNECT response: {e}"))?;
    let response = String::from_utf8_lossy(&buf[..n]).to_lowercase();
    if !response.starts_with("http/1.1 200") && !response.starts_with("http/1.0 200") {
        return Err(anyhow!(
            "proxy refused CONNECT {host}:{port}: {:?}",
            String::from_utf8_lossy(&buf[..n])
        ));
    }

    Ok(stream)
}

/// Wrap the tunnelled TCP stream in TLS for `host`.
async fn wrap_tls(
    stream: TcpStream,
    host: &str,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, anyhow::Error> {
    let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|e| anyhow!("invalid TLS server name {host}: {e}"))?;

    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    connector
        .connect(server_name, stream)
        .await
        .map_err(|e| anyhow!("TLS handshake to {host} through proxy failed: {e}"))
}
