//! PostgreSQL storage backend for whatsapp-rust.
//!
//! Multi-account, multi-pod shared storage. All account-specific tables are
//! partitioned by `device_id`, letting many WhatsApp sessions share one
//! database. Unlike the SQLite backend, PG handles concurrent writes natively
//! so there is no process-wide serialization semaphore.

mod postgres_store;
mod schema;
mod storage_factory;

pub use postgres_store::PostgresStore;
pub use storage_factory::PostgresStorageFactory;
