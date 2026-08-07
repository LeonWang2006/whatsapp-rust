# WhatsApp-Rust

Rust implementation of the WhatsApp protocol, inspired by **whatsmeow** (Go), **Baileys** (TypeScript), and real **WhatsApp Web** behavior. Covers QR pairing, E2E encrypted messaging (1-on-1 + group), media upload/download, and connection management.

## Crate Structure

- **wacore** - Platform-agnostic core: binary protocol, crypto, IQ types, state traits. No Tokio dependency.
- **waproto** - Protobuf definitions (`whatsapp.proto`) compiled via prost. No feature logic here.
- **whatsapp-rust** - Main client: Tokio runtime, SQLite persistence (Diesel), high-level API.
- **whatsapp-rust-sqlite-storage** - SQLite backend (single-process, default for local/dev).
- **whatsapp-rust-postgres-storage** - PostgreSQL backend (multi-pod shared storage, enable via `postgres-storage` feature).
- **StorageFactory** trait (`wacore/src/store/storage_factory.rs`) - resolves JID -> Backend for multi-session deployments. Implemented by `PostgresStorageFactory`.
- **Server module** (`src/server/`) - multi-pod queue consumer. `Server::run()` does `BRPOP` on sharded `wa-queue:{i}` keys, dispatches tasks to per-session workers. Enabled via `postgres-storage` feature.
- **Session lifecycle** (`src/server/session.rs`) - `Event::LoggedOut` deletes device credentials (`factory.delete_for_jid`) then tears down; `Event::StreamReplaced` tears down only (credentials preserved for re-pairing). Both unregister from `wa-registry` and cancel the session task. Events are forwarded to `wa-events` before teardown so the business system learns of the logout/replacement.
- **Session cap** - `Server::with_max_sessions(n)` / `MAX_SESSIONS` env var bounds concurrent sessions per pod. Dispatcher drops new pairing tasks when full; existing sessions keep running.
- **Graceful shutdown** - `Server::run()` cancels all live sessions on shutdown signal and gives each `client.disconnect()` a 10s budget before forcing cleanup (prevents hung sessions from blocking k8s pod termination).

## Build & Verify

```bash
cargo fmt --all
cargo clippy --all --tests
cargo test --all
cargo clippy --features postgres-storage --all --tests   # server module + PG factory
cargo test -p whatsapp-rust-postgres-storage -- --ignored  # requires live PG
cargo test -p e2e-tests          # requires mock server running
```

## Rust Style

- **Collapsible if**: Always use let-chains (`if let Some(x) = foo && let Some(y) = x.bar { ... }`) instead of nested `if let` blocks. Clippy's `collapsible_if` lint will reject the nested form.
- **No real PII in tests**: Use fictitious phone numbers and JIDs in test code. Never commit real user numbers.

## Critical Conventions

- **State**: Never modify Device state directly. Use `DeviceCommand` + `PersistenceManager::process_command()`. Read via `get_device_snapshot()`.
- **Async**: All I/O uses Tokio. Wrap blocking I/O (`ureq`) and heavy CPU work in `tokio::task::spawn_blocking`.
- **Concurrency**: `session_locks` serializes per-sender Signal encrypt/decrypt. `message_enqueue_locks` serializes per-chat incoming message processing. Outgoing sends are not per-chat locked (matches WA Web).
- **Errors**: `thiserror` for typed errors, `anyhow` for multi-failure functions. No `.unwrap()` outside tests.
- **Protocol**: Cross-reference **whatsmeow**, **Baileys**, and captured WhatsApp Web JS (`docs/captured-js/`) to verify implementations.
- **IQ Requests**: Use `client.execute(Spec::new(&jid)).await?` pattern. IqSpec constructors take `&Jid` not `Jid`.
- **New features**: Expose via `src/features/mod.rs`, re-export in `src/lib.rs`.
- **Session lifecycle**: `Event::LoggedOut` and `Event::StreamReplaced` are handled inside the `Bot::on_event` closure in `run_session`. They cancel the session `CancellationToken`, which propagates to the bot run future and command loop. Never call `factory.delete_for_jid()` outside the `LoggedOut` path - `StreamReplaced` keeps credentials so the same JID can re-pair.
- **Cross-pod routing**: Non-pairing tasks with no local session are forwarded to the owning pod's `wa-inbox:{pod_id}` queue (looked up via `wa-registry`). The owning pod's `inbox_consumer` `BRPOP`s and dispatches locally.
- **Redis leases**: `wa-registry:lease:{jid}` (60s TTL) refreshed every 20s by a heartbeat task. Lease expiry marks the registry entry stale; another pod may claim the JID.

## Detailed Docs

Read these when working on the relevant area:

- `agent_docs/protocol_architecture.md` — ProtocolNode, IqSpec, derive macros, node parsing
- `agent_docs/feature_implementation.md` — Step-by-step feature implementation flow
- `agent_docs/e2e_testing.md` — E2E test patterns, file organization, event-driven waiting
- `agent_docs/debugging.md` — evcxr REPL, binary protocol debugging

When adding comments to the code, dont be so verbose, also only explain why, not what
