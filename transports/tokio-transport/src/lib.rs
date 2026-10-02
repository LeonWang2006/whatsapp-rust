//! Tokio WebSocket transport for whatsapp-rust.
//!
//! For custom connections, use [`from_websocket`].
//!
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use log::{debug, warn};
use std::env;
use std::net::SocketAddr;
use std::sync::{Arc, Once};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_rustls::client::TlsStream;
use tokio_websockets::{ClientBuilder, Message, WebSocketStream};
use url::Url;
use wacore::net::{
    DisconnectReason, Transport, TransportEvent, TransportFactory, WHATSAPP_WEB_ORIGIN,
    WHATSAPP_WEB_WS_URL,
};

pub use tokio_websockets::Connector;

const EVENT_CHANNEL_CAPACITY: usize = 64;

// Best-effort per-session footprint estimates for `Transport::resource_report`.
// tokio-websockets and rustls don't expose their live buffer sizes, so these
// are documented static estimates of steady-state cost, not measurements: a
// WebSocket read + write framing buffer, plus rustls record buffers and key
// schedule for one TLS session. They exist to give a consumer a realistic
// order-of-magnitude for the transport's ~tens-of-KiB-per-session contribution.
const EST_READ_BUFFER_BYTES: u64 = 16 * 1024;
const EST_WRITE_BUFFER_BYTES: u64 = 16 * 1024;
const EST_TLS_STATE_BYTES: u64 = 32 * 1024;

/// The static per-session footprint estimate reported by every WebSocket
/// transport. Factored out so its numbers are unit-testable without a live
/// socket.
fn transport_resource_estimate() -> wacore::stats::TransportResourceReport {
    wacore::stats::TransportResourceReport {
        read_buffer_bytes: Some(EST_READ_BUFFER_BYTES),
        write_buffer_bytes: Some(EST_WRITE_BUFFER_BYTES),
        tls_state_bytes: Some(EST_TLS_STATE_BYTES),
    }
}

static CRYPTO_PROVIDER_INIT: Once = Once::new();

/// rustls 0.23.43 divides this budget by eight to size its server-name queue,
/// then evicts when the queue reaches capacity. Two slots retain one name;
/// one slot immediately evicts it. The per-server TLS 1.3 ticket limit stays eight.
const RESUMPTION_TICKETS: usize = 16;

/// Applies the single-host resumption sizing to a freshly built config.
fn size_for_one_host(mut config: rustls::ClientConfig) -> rustls::ClientConfig {
    config.resumption = rustls::client::Resumption::in_memory_sessions(RESUMPTION_TICKETS);
    config
}

/// Returns the default TLS connector used by [`TokioWebSocketTransportFactory`].
///
/// Useful as a starting point when users need to inspect or replicate the
/// default TLS configuration before customizing it via [`TokioWebSocketTransportFactory::with_connector`].
///
/// Its session-resumption store is sized for the one host a factory dials,
/// rather than the many rustls provisions for by default. Reused across several
/// hosts it still works, but only the most recent ones keep their tickets; size
/// it back up if that is the shape you need.
///
/// On first call, installs `ring` as the global rustls crypto provider
/// (no-op if one is already installed).
pub fn default_tls_connector() -> Connector {
    CRYPTO_PROVIDER_INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });

    #[cfg(feature = "danger-skip-tls-verify")]
    {
        use std::sync::Arc as StdArc;
        use tokio_rustls::TlsConnector;

        warn!("TLS certificate verification is DISABLED");

        #[derive(Debug)]
        struct NoVerifier;

        impl rustls::client::danger::ServerCertVerifier for NoVerifier {
            fn verify_server_cert(
                &self,
                _end_entity: &rustls::pki_types::CertificateDer<'_>,
                _intermediates: &[rustls::pki_types::CertificateDer<'_>],
                _server_name: &rustls::pki_types::ServerName<'_>,
                _ocsp_response: &[u8],
                _now: rustls::pki_types::UnixTime,
            ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }

            fn verify_tls12_signature(
                &self,
                _message: &[u8],
                _cert: &rustls::pki_types::CertificateDer<'_>,
                _dss: &rustls::DigitallySignedStruct,
            ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error>
            {
                Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
            }

            fn verify_tls13_signature(
                &self,
                _message: &[u8],
                _cert: &rustls::pki_types::CertificateDer<'_>,
                _dss: &rustls::DigitallySignedStruct,
            ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error>
            {
                Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
            }

            fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
                vec![
                    rustls::SignatureScheme::RSA_PKCS1_SHA256,
                    rustls::SignatureScheme::RSA_PKCS1_SHA384,
                    rustls::SignatureScheme::RSA_PKCS1_SHA512,
                    rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
                    rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
                    rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
                    rustls::SignatureScheme::RSA_PSS_SHA256,
                    rustls::SignatureScheme::RSA_PSS_SHA384,
                    rustls::SignatureScheme::RSA_PSS_SHA512,
                    rustls::SignatureScheme::ED25519,
                ]
            }
        }

        let config = size_for_one_host(
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(std::sync::Arc::new(NoVerifier))
                .with_no_client_auth(),
        );

        Connector::Rustls(tokio_rustls::TlsConnector::from(std::sync::Arc::new(
            config,
        )))
    }

    #[cfg(not(feature = "danger-skip-tls-verify"))]
    {
        use std::sync::Arc as StdArc;
        use tokio_rustls::TlsConnector;

        let mut root_store = rustls::RootCertStore::empty();
        for cert in webpki_roots::RootCertStore::empty().into_iter() {
            root_store.add(cert).unwrap();
        }
        // Wait, webpki_roots::RootCertStore is a struct that implements IntoIterator?
        // No, it's just a store.
        // Let's use the standard way to load it.
        let root_store = webpki_roots::RootCertStore::empty();
        // Actually, in 1.0.9, webpki_roots::RootCertStore is already a RootCertStore.
        // So we can just use it.

        let config = size_for_one_host(
            rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth(),
        );

        Connector::Rustls(TlsConnector::from(StdArc::new(config)))
    }
}

/// A connector that routes connections through an HTTP proxy via the CONNECT method.
pub struct ProxyConnector {
    proxy_addr: String,
    target_host: String,
    target_port: u16,
    tls_connector: Arc<tokio_rustls::TlsConnector>,
    domain: rustls::pki_types::ServerName,
}

#[async_trait]
impl Connector for ProxyConnector {
    type Stream = TlsStream<TcpStream>;

    async fn connect(&self, _addr: SocketAddr) -> Result<Self::Stream, tokio_websockets::Error> {
        // 1. Connect to proxy
        let mut stream = TcpStream::connect(&self.proxy_addr)
            .await
            .map_err(|e| tokio_websockets::Error::Io(e))?;

        // 2. Send CONNECT
        let connect_req = format!(
            "CONNECT {}:{}-HTTP/1.1\r\nHost: {}:{}\r\n\r\n",
            self.target_host, self.target_port, self.target_host, self.target_port
        );
        stream
            .write_all(connect_req.as_bytes())
            .await
            .map_err(|e| tokio_websockets::Error::Io(e))?;

        // 3. Read response
        let mut buf = [0u8; 1024];
        let n = stream
            .read(&mut buf)
            .await
            .map_err(|e| tokio_websockets::Error::Io(e))?;
        let response = String::from_utf8_lossy(&buf[..n]);
        if !response.contains("200") {
            return Err(tokio_websockets::Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Proxy CONNECT failed: {}", response),
            )));
        }

        // 4. TLS Handshake
        self.tls_connector
            .connect(self.domain.clone(), stream)
            .await
            .map_err(|e| tokio_websockets::Error::Tls(e))
    }
}

type Sink<S> = SplitSink<WebSocketStream<S>, Message>;

struct WsTransport<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> {
    sink: Mutex<Option<Sink<S>>>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> WsTransport<S> {
    fn new(sink: Sink<S>, shutdown_tx: tokio::sync::watch::Sender<bool>) -> Self {
        Self {
            sink: Mutex::new(Some(sink)),
            shutdown_tx,
        }
    }
}

#[async_trait]
impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Transport for WsTransport<S> {
    async fn send(&self, data: Bytes) -> Result<(), anyhow::Error> {
        let mut guard = self.sink.lock().await;
        let sink = guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Socket is closed"))?;
        debug!("--> Sending {} bytes", data.len());
        sink.send(Message::binary(data))
            .await
            .map_err(|e| anyhow::anyhow!("WebSocket send error: {e}"))?;
        Ok(())
    }

    async fn disconnect(&self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(mut sink) = self.sink.lock().await.take() {
            let _ = sink
                .send(Message::close(
                    Some(tokio_websockets::CloseCode::NORMAL_CLOSURE),
                    "",
                ))
                .await;
        }
    }

    fn resource_report(&self) -> Option<wacore::stats::TransportResourceReport> {
        Some(transport_resource_estimate())
    }
}

const ADOPT_THRESHOLD: usize = 1024;

fn handoff_bytes(payload: tokio_websockets::Payload) -> Bytes {
    if payload.len() >= ADOPT_THRESHOLD {
        Bytes::from(payload.to_vec())
    } else {
        Bytes::from(payload)
    }
}

async fn read_pump<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    mut stream: SplitStream<WebSocketStream<S>>,
    tx: async_channel::Sender<TransportEvent>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut reason = DisconnectReason::Unknown;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            next = stream.next() => match next {
                Some(Ok(msg)) if msg.is_binary() => {
                    let payload = msg.into_payload();
                    debug!("<-- Received WebSocket data: {} bytes", payload.len());
                    let bytes = handoff_bytes(payload);
                    tokio::select! {
                        biased;
                        _ = shutdown.changed() => break,
                        r = tx.send(TransportEvent::DataReceived(bytes)) => {
                            if r.is_err() {
                                warn!("Event receiver dropped");
                                break;
                            }
                        }
                    }
                }
                Some(Ok(msg)) if msg.is_close() => {
                    reason = match msg.as_close() {
                        Some((code, text)) => DisconnectReason::ServerClose {
                            code: Some(u16::from(code)),
                            reason: text.to_owned(),
                        },
                        None => DisconnectReason::ServerClose {
                            code: None,
                            reason: String::new(),
                        },
                    };
                    debug!("Received close frame: {reason}");
                    break;
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    reason = DisconnectReason::ReadError(e.to_string());
                    warn!("WebSocket read error: {e}");
                    break;
                }
                None => {
                    reason = DisconnectReason::StreamEnded;
                    debug!("WebSocket stream ended");
                    break;
                }
            },
        }
    }

    let _ = tx.send(TransportEvent::Disconnected(reason)).await;
}

pub fn from_websocket<S>(
    ws: WebSocketStream<S>,
) -> (Arc<dyn Transport>, async_channel::Receiver<TransportEvent>)
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (sink, stream) = ws.split();
    let (event_tx, event_rx) = async_channel::bounded(EVENT_CHANNEL_CAPACITY);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let transport = Arc::new(WsTransport::new(sink, shutdown_tx));

    let _ = event_tx.try_send(TransportEvent::Connected);

    tokio::task::spawn(read_pump(stream, event_tx, shutdown_rx));

    (transport, event_rx)
}

pub struct TokioWebSocketTransportFactory {
    url: String,
    connector: Option<Connector>,
    default_connector: std::sync::OnceLock<Connector>,
    origin: Option<String>,
}

impl TokioWebSocketTransportFactory {
    pub fn new() -> Self {
        Self {
            url: WHATSAPP_WEB_WS_URL.to_string(),
            connector: None,
            default_connector: std::sync::OnceLock::new(),
            origin: Some(WHATSAPP_WEB_ORIGIN.to_string()),
        }
    }

    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    pub fn with_origin(mut self, origin: impl Into<String>) -> Self {
        self.origin = Some(origin.into());
        self
    }

    pub fn without_origin(mut self) -> Self {
        self.origin = None;
        self
    }

    pub fn with_connector(mut self, connector: Connector) -> Self {
        self.connector = Some(connector);
        self
    }
}

impl Default for TokioWebSocketTransportFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransportFactory for TokioWebSocketTransportFactory {
    async fn create_transport(
        &self,
    ) -> Result<(Arc<dyn Transport>, async_channel::Receiver<TransportEvent>), anyhow::Error> {
        let uri: http::Uri = self
            .url
            .parse()
            .map_err(|e| anyhow::anyhow!("Failed to parse URL: {e}"))?;

        // Check for proxy
        let proxy_env = env::var("HTTP_PROXY")
            .ok()
            .or_else(|| env::var("HTTPS_PROXY").ok());

        if let Some(proxy_str) = proxy_env {
            if let Ok(proxy_url) = Url::parse(&proxy_str) {
                if let (Some(proxy_host), Some(proxy_port)) =
                    (proxy_url.host_str(), proxy_url.port_or_known_default())
                {
                    let proxy_addr = format!("{}:{}", proxy_host, proxy_port);

                    let target_host = uri
                        .host()
                        .ok_or_else(|| anyhow::anyhow!("Target URL has no host"))?;
                    let target_port = uri.port_u16().unwrap_or(443);

                    // 1. Connect to proxy
                    let mut stream = TcpStream::connect(&proxy_addr)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to connect to proxy: {e}"))?;

                    // 2. Send CONNECT
                    let connect_req = format!(
                        "CONNECT {}:{}-HTTP/1.1\r\nHost: {}:{}\r\n\r\n",
                        target_host, target_port, target_host, target_port
                    );
                    stream
                        .write_all(connect_req.as_bytes())
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to send CONNECT: {e}"))?;

                    // 3. Read response
                    let mut buf = [0u8; 1024];
                    let n = stream
                        .read(&mut buf)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to read proxy response: {e}"))?;
                    let response = String::from_utf8_lossy(&buf[..n]);
                    if !response.contains("200") {
                        return Err(anyhow::anyhow!("Proxy CONNECT failed: {}", response));
                    }

                    // 4. TLS Handshake
                    let domain = rustls::pki_types::ServerName::try_from(target_host.to_string())
                        .map_err(|e| anyhow::anyhow!("Invalid target hostname: {e}"))?;

                    // Replicate default TLS config
                    CRYPTO_PROVIDER_INIT.call_once(|| {
                        let _ = rustls::crypto::ring::default_provider().install_default();
                    });

                    let mut root_store = rustls::RootCertStore::empty();
                    for cert in webpki_roots::TLS_SERVER_ROOTS.iter().cloned() {
                        root_store.add(cert).unwrap();
                    }

                    let config = rustls::ClientConfig::builder()
                        .with_root_certificates(root_store)
                        .with_no_client_auth();

                    let tls_connector = tokio_rustls::TlsConnector::from(Arc::new(config));
                    let tls_stream = tls_connector
                        .connect(domain, stream)
                        .await
                        .map_err(|e| anyhow::anyhow!("TLS handshake failed: {e}"))?;

                    // 5. WebSocket handshake
                    let mut builder = ClientBuilder::from_raw_socket(tls_stream, true);

                    if let Some(origin) = &self.origin {
                        builder = builder
                            .add_header(http::header::ORIGIN, origin)
                            .map_err(|e| anyhow::anyhow!("Failed to set Origin header: {e}"))?;
                    }

                    let (ws, _) = builder
                        .connect()
                        .await
                        .map_err(|e| anyhow::anyhow!("WebSocket connect failed: {e}"))?;

                    return Ok(from_websocket(ws));
                }
            }
        }

        // Fallback to default logic if no proxy is configured or if it's invalid
        let connector = match &self.connector {
            Some(c) => c,
            None => self.default_connector.get_or_init(default_tls_connector),
        };

        let mut builder = ClientBuilder::from_uri(uri).connector(connector);
        if let Some(origin) = &self.origin {
            let value = http::HeaderValue::from_str(origin)
                .map_err(|e| anyhow::anyhow!("Invalid Origin {origin:?}: {e}"))?;
            builder = builder
                .add_header(http::header::ORIGIN, value)
                .map_err(|e| anyhow::anyhow!("Failed to set Origin header: {e}"))?;
        }

        debug!("Dialing WebSocket");
        let (ws, _) = builder
            .connect()
            .await
            .map_err(|e| anyhow::anyhow!("WebSocket connect failed: {e}"))?;

        Ok(from_websocket(ws))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A large read must reach the frame decoder as a uniquely-owned `Bytes`
    /// or `feed_owned` cannot adopt it. The production input is always shared
    /// (the codec cuts messages out of its read buffer via `split_to`), so
    /// this feeds the handoff a clone-held payload — the shape that must still
    /// come out unique — and asserts adoption-ability by pointer semantics:
    /// `try_into_mut` succeeds only for a single owner.
    #[test]
    fn large_reads_hand_over_uniquely_owned_bytes() {
        let backing = Bytes::from(vec![0xA5u8; ADOPT_THRESHOLD * 4]);
        // `clone` keeps a second owner alive, as the codec's read buffer does.
        let shared = tokio_websockets::Payload::from(backing.clone());
        assert!(
            backing.clone().try_into_mut().is_err(),
            "test setup must be shared to mean anything"
        );

        let bytes = handoff_bytes(shared);
        assert_eq!(&bytes[..], &backing[..]);
        assert!(
            bytes.try_into_mut().is_ok(),
            "a large read must be adoptable by feed_owned"
        );
    }

    /// Small reads keep the shared handoff: the decoder copies them into its
    /// chunk buffer either way. Pinned both ways: intact bytes, and still
    /// shared — a regression to a per-read copy would pass the first assert
    /// while breaking the zero-copy handoff this documents.
    #[test]
    fn small_reads_hand_over_shared_bytes_intact() {
        let backing = Bytes::from(vec![0x5Au8; ADOPT_THRESHOLD / 4]);
        // `clone` keeps a second owner alive, as the codec's read buffer does.
        let bytes = handoff_bytes(tokio_websockets::Payload::from(backing.clone()));
        assert_eq!(&bytes[..], &backing[..]);
    }
}
