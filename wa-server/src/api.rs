//! HTTP API surface.
//!
//! Endpoints let the business system push tasks straight into the local
//! dispatch path (no Redis round-trip) and inspect pod/session health:
//!
//! - `GET /health` — liveness. 200 as long as the process is up.
//! - `GET /ready`  — readiness. 200 when the pod can accept sessions.
//! - `GET /status` — JSON pod + session summary.
//! - `GET /pair-code?jid=...` — poll the current 8-char pairing code from its
//!   Redis key; 200 with `{jid, code}` while a code is live, 404 when none is.
//! - `POST /send`  — build a `send_message` task and dispatch it.
//! - `POST /react` — build a `react` task and dispatch it.
//! - `POST /pair`  — build a `pair_code` task and dispatch it.
//!
//! `/send`, `/react`, `/pair` are synchronous dispatch: they enqueue into the
//! local session's command channel (or spawn a session for pairing tasks) and
//! return an accepted/queued ack. They do not await the WhatsApp send result;
//! a full request/response coupling would require per-task result channels,
//! which is a follow-up.

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server, StatusCode};
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::net::SocketAddr;

use crate::session::ServerContext;
use crate::task::{PairCodePayload, ReactPayload, SendMessagePayload, TaskEnvelope, TaskType};

#[derive(Clone)]
pub struct Api {
    ctx: ServerContext,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    pod_id: String,
    sessions: usize,
    max_sessions: usize,
    ready: bool,
}

#[derive(Debug, Serialize)]
struct AckResponse {
    accepted: bool,
    task_id: String,
    jid: String,
}

#[derive(Debug, Serialize)]
struct GenericError {
    #[serde(rename = "error")]
    message: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct SendRequest {
    /// Session owner JID.
    jid: String,
    /// Target chat JID (user@s.whatsapp.net, group@g.us, etc.).
    to: String,
    text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    quote_chat_jid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    quote_message_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReactRequest {
    jid: String,
    to: String,
    message_id: String,
    emoji: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    participant: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PairRequest {
    jid: String,
    phone_number: String,
}

enum ApiError {
    NotFound,
    #[allow(dead_code)]
    InternalServerError,
    BadRequest(String),
}

impl Api {
    pub fn new(ctx: ServerContext) -> Self {
        Self { ctx }
    }

    /// Start the API server on the specified address. Shuts down gracefully
    /// when `shutdown` fires (same token as the shard consumers).
    pub async fn start(
        self,
        addr: SocketAddr,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), hyper::Error> {
        let ctx = self.ctx;

        let make_svc = make_service_fn(move |_conn| {
            let ctx = ctx.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req| {
                    let ctx = ctx.clone();
                    Self::router(ctx, req)
                }))
            }
        });

        let server = Server::bind(&addr).serve(make_svc);
        let graceful = server.with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        });
        info!("API server listening on http://{addr}");
        graceful.await
    }

    async fn router(ctx: ServerContext, req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let path = req.uri().path().to_string();
        let method = req.method().clone();
        let response = match (method.as_str(), path.as_str()) {
            ("GET", "/health") => Self::handle_health(),
            ("GET", "/ready") => Self::handle_ready(&ctx),
            ("GET", "/status") => Self::handle_status(&ctx),
            ("GET", "/pair-code") => Self::handle_pair_code(ctx, req).await,
            ("POST", "/send") => Self::handle_send(ctx, req).await,
            ("POST", "/react") => Self::handle_react(ctx, req).await,
            ("POST", "/pair") => Self::handle_pair(ctx, req).await,
            _ => Self::handle_not_found(),
        };
        Ok(response)
    }

    fn handle_health() -> Response<Body> {
        json_response(StatusCode::OK, &serde_json::json!({ "status": "ok" }))
    }

    fn handle_ready(ctx: &ServerContext) -> Response<Body> {
        // Readiness is about capacity, not emptiness: a fresh pod with no
        // sessions yet is still ready to take work.
        let ready = ctx.max_sessions == 0 || ctx.registry.len() < ctx.max_sessions;
        if ready {
            json_response(StatusCode::OK, &serde_json::json!({ "status": "ready" }))
        } else {
            json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                &serde_json::json!({ "status": "not_ready" }),
            )
        }
    }

    fn handle_status(ctx: &ServerContext) -> Response<Body> {
        let body = StatusResponse {
            pod_id: ctx.pod_id.clone(),
            sessions: ctx.registry.len(),
            max_sessions: ctx.max_sessions,
            ready: ctx.max_sessions == 0 || ctx.registry.len() < ctx.max_sessions,
        };
        json_response(StatusCode::OK, &body)
    }

    async fn handle_send(ctx: ServerContext, req: Request<Body>) -> Response<Body> {
        let parsed: Result<SendRequest, serde_json::Error> =
            serde_json::from_slice(&body_bytes(req).await);
        let request = match parsed {
            Ok(r) => r,
            Err(e) => return Self::error_to_response(ApiError::BadRequest(e.to_string())),
        };
        let quote = match (request.quote_chat_jid, request.quote_message_id) {
            (Some(chat_jid), Some(message_id)) => Some(crate::task::QuoteTarget {
                chat_jid,
                message_id,
            }),
            _ => None,
        };
        let payload = serde_json::to_value(SendMessagePayload {
            to: request.to,
            text: request.text,
            quote,
        })
        .unwrap_or(serde_json::Value::Null);

        let (task, jid) = build_envelope(request.jid, TaskType::SendMessage, payload);
        dispatch_and_ack(ctx, task, jid)
    }

    async fn handle_react(ctx: ServerContext, req: Request<Body>) -> Response<Body> {
        let parsed: Result<ReactRequest, serde_json::Error> =
            serde_json::from_slice(&body_bytes(req).await);
        let request = match parsed {
            Ok(r) => r,
            Err(e) => return Self::error_to_response(ApiError::BadRequest(e.to_string())),
        };
        let payload = serde_json::to_value(ReactPayload {
            to: request.to,
            message_id: request.message_id,
            emoji: request.emoji,
            participant: request.participant,
        })
        .unwrap_or(serde_json::Value::Null);

        let (task, jid) = build_envelope(request.jid, TaskType::React, payload);
        dispatch_and_ack(ctx, task, jid)
    }

    async fn handle_pair(ctx: ServerContext, req: Request<Body>) -> Response<Body> {
        let parsed: Result<PairRequest, serde_json::Error> =
            serde_json::from_slice(&body_bytes(req).await);
        let request = match parsed {
            Ok(r) => r,
            Err(e) => return Self::error_to_response(ApiError::BadRequest(e.to_string())),
        };
        let payload = serde_json::to_value(PairCodePayload {
            phone_number: request.phone_number,
        })
        .unwrap_or(serde_json::Value::Null);

        let (task, jid) = build_envelope(request.jid, TaskType::PairCode, payload);
        dispatch_and_ack(ctx, task, jid)
    }

    /// Poll the current pairing code for `jid` from its Redis key.
    ///
    /// The code is written by the session's event bridge when `Event::PairingCode`
    /// fires (see [`crate::event_bridge`]). 404 means no code is currently live
    /// (still pairing, expired, or already logged in); the client polls again.
    async fn handle_pair_code(ctx: ServerContext, req: Request<Body>) -> Response<Body> {
        let jid = req
            .uri()
            .query()
            .and_then(|q| query_param(q, "jid"))
            .filter(|j| !j.is_empty());
        let Some(jid) = jid else {
            return Self::error_to_response(ApiError::BadRequest(
                "missing required query param: jid".to_string(),
            ));
        };
        let key = crate::task::pair_code_key(&ctx.pair_code_key_prefix, &jid);
        let mut redis = ctx.redis.clone();
        match redis::Cmd::get(&key)
            .query_async::<Option<String>>(&mut redis)
            .await
        {
            Ok(Some(code)) => json_response(
                StatusCode::OK,
                &serde_json::json!({ "jid": jid, "code": code }),
            ),
            Ok(None) => json_response(
                StatusCode::NOT_FOUND,
                &serde_json::json!({ "error": "no pairing code currently live" }),
            ),
            Err(e) => {
                error!("failed to GET pair code for jid={jid}: {e}");
                Self::error_to_response(ApiError::InternalServerError)
            }
        }
    }

    fn handle_not_found() -> Response<Body> {
        Self::error_to_response(ApiError::NotFound)
    }

    fn error_to_response(error: ApiError) -> Response<Body> {
        let (status, message) = match error {
            ApiError::NotFound => (StatusCode::NOT_FOUND, "Endpoint not found".to_string()),
            ApiError::InternalServerError => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".to_string(),
            ),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
        };
        json_response(status, &GenericError { message })
    }
}

fn build_envelope(
    jid: String,
    task_type: TaskType,
    payload: serde_json::Value,
) -> (TaskEnvelope, String) {
    let task_id = format!("api-{}", uuid_short());
    let created_at = wacore::time::now_millis();
    (
        TaskEnvelope {
            task_id,
            jid: jid.clone(),
            task_type,
            created_at,
            payload,
        },
        jid,
    )
}

fn uuid_short() -> String {
    // No `uuid` dependency in the crate's normal deps; derive a pseudo-unique
    // suffix from the clock + a monotonic counter. Good enough for task ids.
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:x}-{:x}", wacore::time::now_millis() as u64, n)
}

/// Dispatch a task into the local session path and return an ack. For pairing
/// tasks the dispatcher spawns a session; for others it forwards via the
/// registry (which may hop to another pod over the inbox).
fn dispatch_and_ack(ctx: ServerContext, task: TaskEnvelope, jid: String) -> Response<Body> {
    let task_id = task.task_id.clone();
    let ctx2 = ctx.clone();
    tokio::spawn(async move {
        crate::dispatcher::dispatch(&ctx2, task).await;
    });
    let body = AckResponse {
        accepted: true,
        task_id,
        jid,
    };
    json_response(StatusCode::ACCEPTED, &body)
}

async fn body_bytes(req: Request<Body>) -> Vec<u8> {
    hyper::body::to_bytes(req.into_body())
        .await
        .map(|b| b.to_vec())
        .unwrap_or_default()
}

/// Read a percent-decoded query parameter from a raw query string.
fn query_param(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        if k != key {
            return None;
        }
        Some(percent_decode(v))
    })
}

/// Minimal percent-decoder for query values (`%40` -> `@`).
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push(((h << 4) | l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn json_response<T: Serialize>(status: StatusCode, body: &T) -> Response<Body> {
    match serde_json::to_vec(body) {
        Ok(bytes) => Response::builder()
            .status(status)
            .header("Content-Type", "application/json")
            .body(Body::from(bytes))
            .unwrap_or_else(|_| Response::new(Body::from("{}"))),
        Err(e) => {
            error!("failed to serialize JSON response: {e}");
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from("{\"error\":\"serialization failure\"}"))
                .unwrap_or_else(|_| Response::new(Body::from("{}")))
        }
    }
}
