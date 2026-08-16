//! `wa-server`: multi-pod WhatsApp session server.
//!
//! Consumes tasks from the sharded Redis `wa-queue:{i}` lists, runs one
//! session worker per WhatsApp account, and exposes an HTTP API. Intended to
//! run as one process per k8s pod.

pub mod api;
pub mod dispatcher;
pub mod event_bridge;
pub mod in_memory_factory;
pub mod platform;
pub mod proxy_transport;
pub mod redis_registry;
pub mod registry;
pub mod server;
pub mod session;
pub mod storage_factory;
pub mod task;

pub use api::Api;
pub use in_memory_factory::InMemoryStorageFactory;
pub use registry::SessionRegistry;
pub use server::Server;
pub use session::ServerContext;
pub use storage_factory::StorageFactory;
pub use task::{TaskEnvelope, TaskType};
