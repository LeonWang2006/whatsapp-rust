//! HTTP API surface.
//!
//! Endpoints let the business system push tasks straight into the local
//! dispatch path (no Redis round-trip) and inspect pod/session health:
//!
//! - `GET /health` — liveness. 200 as long as the process is up.
//! - `GET /ready`  — readiness. 200 when the pod can accept sessions.
//! - `GET /status` — JSON pod + session summary.
//! - `GET /pair-code?phone=...` — poll the current 8-char pairing code from its
//!   Redis key (`{prefix}:{phone}`); 200 with `{phone, code}` while a code is
//!   live, 404 when none is. `?jid=...` is also accepted and derives the phone.
//! - `GET /link-status?phone=...` — poll the pairing flow's outcome from its
//!   Redis key (`{link_prefix}:{phone}`): `{status: pairing|success|failed}`,
//!   with `code` while pairing and `error` on failure. 404 when no flow is
//!   known for the phone. `?jid=...` is also accepted.
//! - `GET /presence?phone=...&contact=...&start=&end=` — contact online/offline
//!   events within a time window (Unix secs, defaults 24h), plus derived online
//!   intervals and total online seconds. Backed by PG `biz.presence_event`.
//! - `POST /send`  — build a `send_message` task and dispatch it.
//! - `POST /react` — build a `react` task and dispatch it.
//! - `POST /pair`  — build a `pair_code` task and dispatch it.
//! - `POST /link`  — business link action: push a `pair_code` task onto the
//!   shared Redis `wa-queue` so any pod's server loop consumes it (the
//!   multi-pod path; `/pair` stays for local single-pod debugging).
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
use crate::task::{
    LinkStatus, LinkStatusKind, PairCodePayload, PresenceEvent, ReactPayload, SendMessagePayload,
    TaskEnvelope, TaskType, shard_for_jid,
};

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

/// Business `link` request: a client asks the server to pair a contact's phone
/// number with WhatsApp and obtain the 8-char pairing code. The code is pushed
/// onto the shared Redis `wa-queue` (multi-pod), and the code is later polled
/// via `GET /pair-code?phone=...`.
#[derive(Debug, Serialize, Deserialize)]
struct LinkRequest {
    /// Business user id (`biz.wa_user.id`). Optional — used for audit only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    user_id: Option<i64>,
    /// Client device uuid (`biz.wa_user.device_uuid`). Optional.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    device_uuid: Option<String>,
    /// Contact phone number (E.164, digits only) to pair.
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
            ("GET", "/link-status") => Self::handle_link_status(ctx, req).await,
            ("GET", "/presence") => Self::handle_presence(ctx, req).await,
            ("POST", "/send") => Self::handle_send(ctx, req).await,
            ("POST", "/react") => Self::handle_react(ctx, req).await,
            ("POST", "/pair") => Self::handle_pair(ctx, req).await,
            ("POST", "/link") => Self::handle_link(ctx, req).await,
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

    /// Business `link` action: push a `pair_code` task onto the shared Redis
    /// `wa-queue` for any pod to consume.
    ///
    /// Unlike `/pair` (which dispatches into the local process), this is the
    /// multi-pod path: the API pushes onto `wa-queue:{shard}` and whichever
    /// pod's shard consumer picks it up requests the 8-char code. The code is
    /// then polled via `GET /pair-code?phone=...` and the flow's outcome via
    /// `GET /link-status?phone=...`.
    ///
    /// The per-phone link-status key is reset to `pairing` here so a stale
    /// terminal state from a previous flow (e.g. a `failed`) does not linger
    /// after the client initiates a fresh link.
    async fn handle_link(ctx: ServerContext, req: Request<Body>) -> Response<Body> {
        let parsed: Result<LinkRequest, serde_json::Error> =
            serde_json::from_slice(&body_bytes(req).await);
        let request = match parsed {
            Ok(r) => r,
            Err(e) => return Self::error_to_response(ApiError::BadRequest(e.to_string())),
        };
        if request.phone_number.trim().is_empty() {
            return Self::error_to_response(ApiError::BadRequest(
                "missing required field: phone_number".to_string(),
            ));
        }
        let phone = request.phone_number.trim().to_string();
        let jid = format!("{phone}@s.whatsapp.net");
        let payload = serde_json::to_value(PairCodePayload {
            phone_number: phone.clone(),
        })
        .unwrap_or(serde_json::Value::Null);
        let (task, jid) = build_envelope(jid, TaskType::PairCode, payload);
        let task_id = task.task_id.clone();

        // Push onto the sharded queue, not the local dispatch path.
        let shard = shard_for_jid(&jid);
        let key = format!("{}:{shard}", crate::task::QUEUE_PREFIX);
        let mut conn = match ctx.redis_client.get_multiplexed_tokio_connection().await {
            Ok(c) => c,
            Err(e) => {
                error!("link: failed to open redis connection: {e}");
                return Self::error_to_response(ApiError::InternalServerError);
            }
        };
        let payload_bytes = match serde_json::to_vec(&task) {
            Ok(b) => b,
            Err(e) => {
                error!("link: failed to serialize task: {e}");
                return Self::error_to_response(ApiError::InternalServerError);
            }
        };
        // Reset the link-status key to `pairing` so a previous terminal state
        // doesn't linger. Use the phone-keyed status key (same shape the worker
        // writes); set with a generous TTL — the worker's real `pairing`+code
        // write will supersede it as soon as the code is minted.
        let status_key = crate::task::link_status_key(&ctx.link_status_key_prefix, &phone);
        let reset_status = LinkStatus {
            status: LinkStatusKind::Pairing,
            code: None,
            error: None,
            updated_at: wacore::time::now_secs().max(0) as i64,
        };
        let reset_body = serde_json::to_vec(&reset_status).unwrap_or_default();
        let mut pipe = redis::pipe();
        pipe.atomic()
            .lpush(&key, payload_bytes)
            .ignore()
            .set(&status_key, reset_body)
            .ignore()
            .expire(&status_key, crate::task::LINK_STATUS_RESET_TTL_SECS)
            .ignore();
        match pipe.query_async::<()>(&mut conn).await {
            Ok(()) => {
                info!(
                    "link: pushed pair_code task={task_id} phone={phone} to {key}; reset link status"
                );
                let body = AckResponse {
                    accepted: true,
                    task_id,
                    jid,
                };
                json_response(StatusCode::ACCEPTED, &body)
            }
            Err(e) => {
                error!("link: failed to LPUSH to {key}: {e}");
                Self::error_to_response(ApiError::InternalServerError)
            }
        }
    }
    ///
    /// The key is `{prefix}:{phone}` (see [`crate::task::pair_code_key`]).
    /// Accepts either `?phone=861866620688` or `?jid=861866620688@s.whatsapp.net`
    /// (the JID form derives the phone automatically). 404 means no code is
    /// currently live (still pairing, expired, or already logged in); the
    /// client polls again.
    async fn handle_pair_code(ctx: ServerContext, req: Request<Body>) -> Response<Body> {
        let query = req.uri().query().unwrap_or_default();
        let phone = query_param(query, "phone")
            .filter(|p| !p.is_empty())
            .or_else(|| {
                let jid = query_param(query, "jid")?;
                crate::task::phone_from_jid(&jid).map(str::to_owned)
            });
        let Some(phone) = phone else {
            return Self::error_to_response(ApiError::BadRequest(
                "missing required query param: phone (or jid)".to_string(),
            ));
        };
        let key = crate::task::pair_code_key(&ctx.pair_code_key_prefix, &phone);
        let mut redis = ctx.redis.clone();
        match redis::Cmd::get(&key)
            .query_async::<Option<String>>(&mut redis)
            .await
        {
            Ok(Some(code)) => json_response(
                StatusCode::OK,
                &serde_json::json!({ "phone": phone, "code": code }),
            ),
            Ok(None) => json_response(
                StatusCode::NOT_FOUND,
                &serde_json::json!({ "error": "no pairing code currently live" }),
            ),
            Err(e) => {
                error!("failed to GET pair code for phone={phone}: {e}");
                Self::error_to_response(ApiError::InternalServerError)
            }
        }
    }

    /// Poll the pairing flow's outcome for a phone number from its Redis key.
    ///
    /// The key is `{link_prefix}:{phone}` (see [`crate::task::link_status_key`]).
    /// Accepts either `?phone=861866620688` or `?jid=861866620688@s.whatsapp.net`
    /// (the JID form derives the phone automatically). 200 with
    /// `{phone, status, code?, error?, updated_at}` while a flow is known;
    /// 404 when none is (no link initiated, or the terminal state expired).
    ///
    /// `status` is one of `pairing` / `success` / `failed`. On `failed` the
    /// client shows `error` and may retry by calling `POST /link` again.
    async fn handle_link_status(ctx: ServerContext, req: Request<Body>) -> Response<Body> {
        let query = req.uri().query().unwrap_or_default();
        let phone = query_param(query, "phone")
            .filter(|p| !p.is_empty())
            .or_else(|| {
                let jid = query_param(query, "jid")?;
                crate::task::phone_from_jid(&jid).map(str::to_owned)
            });
        let Some(phone) = phone else {
            return Self::error_to_response(ApiError::BadRequest(
                "missing required query param: phone (or jid)".to_string(),
            ));
        };
        let key = crate::task::link_status_key(&ctx.link_status_key_prefix, &phone);
        let mut redis = ctx.redis.clone();
        match redis::Cmd::get(&key)
            .query_async::<Option<String>>(&mut redis)
            .await
        {
            Ok(Some(raw)) => match serde_json::from_str::<LinkStatus>(&raw) {
                Ok(status) => json_response(
                    StatusCode::OK,
                    &serde_json::json!({
                        "phone": phone,
                        "status": status.status,
                        "code": status.code,
                        "error": status.error,
                        "updated_at": status.updated_at,
                    }),
                ),
                Err(e) => {
                    error!("link-status for phone={phone} is corrupt: {e}");
                    Self::error_to_response(ApiError::InternalServerError)
                }
            },
            Ok(None) => json_response(
                StatusCode::NOT_FOUND,
                &serde_json::json!({ "error": "no link status for this phone" }),
            ),
            Err(e) => {
                error!("failed to GET link status for phone={phone}: {e}");
                Self::error_to_response(ApiError::InternalServerError)
            }
        }
    }

    fn handle_not_found() -> Response<Body> {
        Self::error_to_response(ApiError::NotFound)
    }

    /// Query a contact's online/offline presence events for an owner within a
    /// `[start, end]` time window (Unix seconds), and derive online intervals.
    ///
    /// Params: `phone` (owner, required), `contact` (required), `start`/`end`
    /// (optional, default 24h back to now). Response:
    ///
    /// ```json
    /// {
    ///   "phone": "...", "contact": "...", "start": ..., "end": ...,
    ///   "events": [{ "type": "online", "ts": ..., "last_seen": null }],
    ///   "intervals": [{ "online_ts": ..., "offline_ts": ..., "duration_secs": ... }],
    ///   "total_online_secs": ...
    /// }
    /// ```
    ///
    /// Intervals are paired by scanning events oldest-first: an `online` opens
    /// a window, the next `offline` closes it. A leading `offline` is ignored
    /// (window started before the query range); a trailing `online` with no
    /// `offline` is reported with `offline_ts: null` and excluded from
    /// `total_online_secs` (the contact may still be online).
    async fn handle_presence(ctx: ServerContext, req: Request<Body>) -> Response<Body> {
        let query = req.uri().query().unwrap_or_default();
        let phone = query_param(query, "phone").filter(|p| !p.is_empty());
        let contact = query_param(query, "contact").filter(|p| !p.is_empty());
        let Some(phone) = phone else {
            return Self::error_to_response(ApiError::BadRequest(
                "missing required query param: phone".to_string(),
            ));
        };
        let Some(contact) = contact else {
            return Self::error_to_response(ApiError::BadRequest(
                "missing required query param: contact".to_string(),
            ));
        };
        let now = wacore::time::now_secs().max(0);
        let start = query_param(query, "start")
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(now - 24 * 3600);
        let end = query_param(query, "end")
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(now);
        if start > end {
            return Self::error_to_response(ApiError::BadRequest(
                "start must be <= end".to_string(),
            ));
        }
        let events = match ctx
            .storage_factory
            .query_presence_events(&phone, &contact, start, end)
            .await
        {
            Ok(e) => e,
            Err(e) => {
                error!("presence: query failed phone={phone} contact={contact}: {e}");
                return Self::error_to_response(ApiError::InternalServerError);
            }
        };

        // Pair online/offline events into intervals.
        let (intervals, total_online_secs) = pair_presence_intervals(&events);

        let event_json: Vec<serde_json::Value> = events
            .iter()
            .map(|e| {
                serde_json::json!({ "type": e.event_type, "ts": e.ts, "last_seen": e.last_seen })
            })
            .collect();
        json_response(
            StatusCode::OK,
            &serde_json::json!({
                "phone": phone,
                "contact": contact,
                "start": start,
                "end": end,
                "events": event_json,
                "intervals": intervals,
                "total_online_secs": total_online_secs,
            }),
        )
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

/// A paired online/offline window for one contact.
#[derive(Debug, PartialEq, Eq, Serialize)]
struct PresenceInterval {
    online_ts: i64,
    /// `None` when the contact was still online at the end of the window.
    offline_ts: Option<i64>,
    /// `None` (like `offline_ts`) when the window never closed.
    duration_secs: Option<i64>,
}

/// Pair a contact's online/offline events (already sorted by `ts` ascending)
/// into online windows, and sum the closed windows' duration.
///
/// An `online` opens a window, the next `offline` closes it. A leading
/// `offline` is dropped (the window started before the query range); a
/// trailing `online` with no closer is returned with `offline_ts`/`duration`
/// `None` and excluded from the total (the contact may still be online). A
/// duplicate `online` while a window is open keeps the earliest start.
fn pair_presence_intervals(events: &[PresenceEvent]) -> (Vec<PresenceInterval>, i64) {
    let mut intervals: Vec<PresenceInterval> = Vec::new();
    let mut open_online: Option<i64> = None;
    for ev in events {
        match ev.event_type.as_str() {
            "online" => {
                open_online.get_or_insert(ev.ts);
            }
            "offline" => {
                if let Some(started) = open_online.take() {
                    intervals.push(PresenceInterval {
                        online_ts: started,
                        offline_ts: Some(ev.ts),
                        duration_secs: Some((ev.ts - started).max(0)),
                    });
                }
            }
            _ => {}
        }
    }
    if let Some(started) = open_online {
        intervals.push(PresenceInterval {
            online_ts: started,
            offline_ts: None,
            duration_secs: None,
        });
    }
    let total = intervals.iter().filter_map(|i| i.duration_secs).sum();
    (intervals, total)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(event_type: &str, ts: i64) -> PresenceEvent {
        PresenceEvent {
            owner_phone: "owner".into(),
            contact_phone: "contact".into(),
            event_type: event_type.into(),
            ts,
            last_seen: None,
        }
    }

    #[test]
    fn pairs_online_offline_into_intervals() {
        let events = vec![ev("online", 100), ev("offline", 200)];
        let (intervals, total) = pair_presence_intervals(&events);
        assert_eq!(
            intervals,
            vec![PresenceInterval {
                online_ts: 100,
                offline_ts: Some(200),
                duration_secs: Some(100),
            }]
        );
        assert_eq!(total, 100);
    }

    #[test]
    fn leading_offline_is_ignored() {
        // offline before any online means the window started outside the range.
        let events = vec![ev("offline", 50), ev("online", 100), ev("offline", 200)];
        let (intervals, total) = pair_presence_intervals(&events);
        assert_eq!(intervals.len(), 1);
        assert_eq!(intervals[0].online_ts, 100);
        assert_eq!(intervals[0].offline_ts, Some(200));
        assert_eq!(total, 100);
    }

    #[test]
    fn trailing_online_has_no_closer() {
        let events = vec![ev("online", 100), ev("offline", 200), ev("online", 300)];
        let (intervals, total) = pair_presence_intervals(&events);
        assert_eq!(intervals.len(), 2);
        assert_eq!(
            intervals[1],
            PresenceInterval {
                online_ts: 300,
                offline_ts: None,
                duration_secs: None,
            }
        );
        // Closed window only.
        assert_eq!(total, 100);
    }

    #[test]
    fn duplicate_online_keeps_earliest_start() {
        let events = vec![ev("online", 100), ev("online", 150), ev("offline", 200)];
        let (intervals, _) = pair_presence_intervals(&events);
        assert_eq!(intervals.len(), 1);
        assert_eq!(intervals[0].online_ts, 100);
        assert_eq!(intervals[0].duration_secs, Some(100));
    }

    #[test]
    fn empty_and_all_offline() {
        let (intervals, total) = pair_presence_intervals(&[]);
        assert!(intervals.is_empty());
        assert_eq!(total, 0);

        let (intervals, total) = pair_presence_intervals(&[ev("offline", 100)]);
        assert!(intervals.is_empty());
        assert_eq!(total, 0);
    }
}
