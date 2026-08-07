//! API module for exposing public interfaces and endpoints.

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;

/// API server instance for handling HTTP requests.
pub struct Api {
    // Configuration and state can be added here
}

#[derive(Debug, Serialize, Deserialize)]
struct HealthResponse {
    status: String,
}

#[derive(Debug, Serialize)]
struct GenericError {
    #[serde(rename = "error")]
    message: String,
}

#[derive(Debug)]
enum ApiError {
    NotFound,
    InternalServerError,
    #[allow(dead_code)]
    BadRequest(String),
}

impl Default for Api {
    fn default() -> Self {
        Self::new()
    }
}

impl Api {
    /// Create a new API instance.
    pub fn new() -> Self {
        Self {}
    }

    /// Start the API server on the specified address.
    pub async fn start(&self, addr: &str) -> Result<(), hyper::Error> {
        let addr = addr.parse().expect("Invalid address");

        let make_svc =
            make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(Self::router)) });

        let server = Server::bind(&addr).serve(make_svc);

        println!("API server listening on http://{}", addr);
        server.await
    }

    /// Route incoming requests to appropriate handlers.
    async fn router(req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let response = match (req.method(), req.uri().path()) {
            (&Method::GET, "/health") => Self::handle_health(),
            (&Method::GET, "/ready") => Self::handle_ready(),
            (&Method::GET, "/metrics") => Self::handle_metrics(),
            _ => Self::handle_not_found(),
        };

        Ok(response)
    }

    /// Health check endpoint handler.
    fn handle_health() -> Response<Body> {
        let response = HealthResponse {
            status: "ok".to_string(),
        };

        match serde_json::to_string(&response) {
            Ok(body) => Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
            Err(_) => Self::error_to_response(ApiError::InternalServerError),
        }
    }

    /// Readiness probe. Returns 200 when the process can serve traffic.
    /// P4 will wire in real redis/session-count checks.
    fn handle_ready() -> Response<Body> {
        let response = HealthResponse {
            status: "ready".to_string(),
        };
        match serde_json::to_string(&response) {
            Ok(body) => Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
            Err(_) => Self::error_to_response(ApiError::InternalServerError),
        }
    }

    /// Prometheus-style metrics endpoint. P4 will replace this with a real
    /// metrics registry; for now it exposes a minimal text format so k8s
    /// scrape configs don't 404.
    fn handle_metrics() -> Response<Body> {
        let body = "# HELP wa_sessions_active placeholder\n# TYPE wa_sessions_active gauge\nwa_sessions_active 0\n";
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain; version=0.0.4")
            .body(Body::from(body))
            .unwrap()
    }

    /// Handle requests to undefined endpoints.
    fn handle_not_found() -> Response<Body> {
        Self::error_to_response(ApiError::NotFound)
    }

    /// Convert API errors to HTTP responses.
    fn error_to_response(error: ApiError) -> Response<Body> {
        let (status, message) = match error {
            ApiError::NotFound => (StatusCode::NOT_FOUND, "Endpoint not found".to_string()),
            ApiError::InternalServerError => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".to_string(),
            ),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
        };

        let error_response = GenericError { message };

        let body = serde_json::to_string(&error_response)
            .unwrap_or_else(|_| "{\"error\": \"Failed to serialize error\"}".to_string());

        Response::builder()
            .status(status)
            .header("Content-Type", "application/json")
            .body(Body::from(body))
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::{Client, Uri};

    #[tokio::test]
    async fn test_health_endpoint() {
        let api = Api::new();
        let _handle = tokio::spawn(async move {
            let _ = api.start("127.0.0.1:8080").await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let client = Client::new();
        let uri: Uri = "http://127.0.0.1:8080/health".parse().unwrap();
        let response = client.get(uri).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        let health: HealthResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(health.status, "ok");
    }

    #[tokio::test]
    async fn test_not_found() {
        let api = Api::new();
        let _handle = tokio::spawn(async move {
            let _ = api.start("127.0.0.1:8081").await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let client = Client::new();
        let uri: Uri = "http://127.0.0.1:8081/nonexistent".parse().unwrap();
        let response = client.get(uri).await.unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_ready_endpoint() {
        let api = Api::new();
        let _handle = tokio::spawn(async move {
            let _ = api.start("127.0.0.1:8082").await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let client = Client::new();
        let uri: Uri = "http://127.0.0.1:8082/ready".parse().unwrap();
        let response = client.get(uri).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_metrics_endpoint() {
        let api = Api::new();
        let _handle = tokio::spawn(async move {
            let _ = api.start("127.0.0.1:8083").await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let client = Client::new();
        let uri: Uri = "http://127.0.0.1:8083/metrics".parse().unwrap();
        let response = client.get(uri).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = String::from_utf8(
            hyper::body::to_bytes(response.into_body())
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(body.contains("wa_sessions_active"));
    }
}
