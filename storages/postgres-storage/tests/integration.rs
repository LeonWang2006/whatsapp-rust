//! Integration tests for `PostgresStore` and `PostgresStorageFactory`.
//!
//! These require a live PostgreSQL instance, so they are `#[ignore]` by default.
//! Run them with a `DATABASE_URL` pointing at a disposable test database:
//!
//! ```bash
//! DATABASE_URL="postgres://postgres:123456@localhost:5432/mydb" \
//!     cargo test -p whatsapp-rust-postgres-storage --test integration -- --ignored
//! ```
//!
//! Migrations are run once per test process from a dedicated OS thread (Diesel
//! migrations are not safe to run from inside an existing Tokio runtime), and
//! each test creates its own isolated device row so tests never interfere.

use std::sync::{Arc, Once};

use wacore::appstate::hash::HashState;
use wacore::appstate::processor::AppStateMutationMAC;
use wacore::reporting_token::MESSAGE_SECRET_SIZE;
use wacore::store::traits::{
    AppSyncStore, Backend, DeviceInfo, DeviceListRecord, DeviceStore, LidPnMappingEntry,
    MsgSecretEntry, MsgSecretStore, PendingInboundRow, ProtocolStore, SignalStore, TcTokenEntry,
};
use whatsapp_rust_postgres_storage::{PostgresStorageFactory, PostgresStore};

fn db_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        log::warn!("DATABASE_URL not set; PostgresStore integration tests will fail");
        "postgres://postgres@localhost:5432/wa_test".to_string()
    })
}

/// Diesel's `run_pending_migrations` must not be driven from inside an
/// existing Tokio runtime, and two migrations must never run concurrently.
/// The first test to reach this guard spawns a dedicated OS thread owning its
/// own current-thread runtime; every later test skips.
static MIGRATION_GUARD: Once = Once::new();

fn ensure_migrations() {
    MIGRATION_GUARD.call_once(|| {
        let url = db_url();
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build migration runtime");
            rt.block_on(async {
                PostgresStore::connect(&url)
                    .await
                    .expect("initial migration");
            });
        });
        handle.join().expect("migration thread panicked");
    });
}

/// Open a store bound to a freshly created device row (device_id > 0), so
/// tests run against an isolated slice of every table.
async fn create_test_store() -> PostgresStore {
    ensure_migrations();
    let bootstrap = PostgresStore::new(&db_url()).expect("open store");
    let device_id = bootstrap.create().await.expect("create device row");
    PostgresStore::new_for_device(&db_url(), device_id).expect("open per-device store")
}

/// A backend coerced to `Arc<dyn Backend>` so trait tests read like production.
async fn create_test_backend() -> Arc<dyn Backend> {
    Arc::new(create_test_store().await)
}

fn dummy_jid(seed: u64) -> String {
    // Fictitious, non-PII phone number derived from the test's seed.
    format!("1555{:010}@s.whatsapp.net", seed % 1_000_000_000)
}

// ---------------------------------------------------------------------------
// DeviceStore
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn test_device_create_exists_save_load() {
    let store = create_test_store().await;
    assert!(store.exists().await.unwrap());

    // Save mutates a field, then load must round-trip it.
    let mut device = store.load().await.unwrap().expect("device present");
    device.push_name = "test-pod".to_string();
    device.next_pre_key_id = 42;
    device.server_has_prekeys = true;
    device.lid_migrated = true;
    device.read_receipts_disabled = true;
    store.save(&device).await.expect("save");

    let loaded = store.load().await.unwrap().expect("device present");
    assert_eq!(loaded.push_name, "test-pod");
    assert_eq!(loaded.next_pre_key_id, 42);
    assert!(loaded.server_has_prekeys);
    assert!(loaded.lid_migrated);
    assert!(loaded.read_receipts_disabled);
}

#[tokio::test]
#[ignore]
async fn test_resource_report_has_zero_memory() {
    let backend = create_test_backend().await;
    let report = backend.resource_report().await;
    assert_eq!(report.memory_bytes, Some(0));
}

// ---------------------------------------------------------------------------
// SignalStore
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn test_identity_put_load_delete() {
    let store = create_test_store().await;
    let addr = "15550000001:0";

    store.put_identity(addr, [0xabu8; 32]).await.unwrap();
    assert_eq!(store.load_identity(addr).await.unwrap(), Some([0xabu8; 32]));

    store.delete_identity(addr).await.unwrap();
    assert_eq!(store.load_identity(addr).await.unwrap(), None);
}

#[tokio::test]
#[ignore]
async fn test_session_put_get_delete() {
    let store = create_test_store().await;
    let addr = "15550000002:2";
    let record = b"session-blob".to_vec();

    store.put_session(addr, &record).await.unwrap();
    assert_eq!(
        store.get_session(addr).await.unwrap(),
        Some(bytes::Bytes::from(record.clone()))
    );
    assert!(store.has_session(addr).await.unwrap());

    store.delete_session(addr).await.unwrap();
    assert_eq!(store.get_session(addr).await.unwrap(), None);
}

#[tokio::test]
#[ignore]
async fn test_prekey_store_load_batch_remove() {
    let store = create_test_store().await;

    store.store_prekey(1, b"k1", true).await.unwrap();
    store.store_prekey(2, b"k2", false).await.unwrap();
    assert_eq!(
        store.load_prekey(1).await.unwrap(),
        Some(bytes::Bytes::from_static(b"k1"))
    );
    assert_eq!(
        store.load_prekey(2).await.unwrap(),
        Some(bytes::Bytes::from_static(b"k2"))
    );

    let batch = store.load_prekeys_batch(&[1, 2, 3]).await.unwrap();
    let ids: Vec<u32> = batch.iter().map(|(id, _)| *id).collect();
    assert!(ids.contains(&1) && ids.contains(&2) && !ids.contains(&3));

    store.remove_prekey(1).await.unwrap();
    assert_eq!(store.load_prekey(1).await.unwrap(), None);
    assert_eq!(store.get_max_prekey_id().await.unwrap(), 2);
}

#[tokio::test]
#[ignore]
async fn test_signed_prekey_store_load() {
    let store = create_test_store().await;
    store
        .store_signed_prekey(7, b"signed-prekey-blob")
        .await
        .unwrap();
    assert_eq!(
        store.load_signed_prekey(7).await.unwrap(),
        Some(b"signed-prekey-blob".to_vec())
    );
    let all = store.load_all_signed_prekeys().await.unwrap();
    assert!(all.iter().any(|(id, _)| *id == 7));

    store.remove_signed_prekey(7).await.unwrap();
    assert_eq!(store.load_signed_prekey(7).await.unwrap(), None);
}

#[tokio::test]
#[ignore]
async fn test_sender_key_put_get() {
    let store = create_test_store().await;
    let addr = "15550000003:1";

    store
        .put_sender_key(addr, b"sender-key-blob")
        .await
        .unwrap();
    assert_eq!(
        store.get_sender_key(addr).await.unwrap(),
        Some(b"sender-key-blob".to_vec())
    );

    store.delete_sender_key(addr).await.unwrap();
    assert_eq!(store.get_sender_key(addr).await.unwrap(), None);
}

// ---------------------------------------------------------------------------
// AppSyncStore
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn test_app_state_sync_key_roundtrip() {
    let store = create_test_store().await;
    let key = wacore::store::traits::AppStateSyncKey {
        key_data: b"key-data".to_vec(),
        fingerprint: b"fp".to_vec(),
        timestamp: 1_700_000_000,
    };

    store.set_sync_key(b"key-id-1", key.clone()).await.unwrap();
    let loaded = store.get_sync_key(b"key-id-1").await.unwrap().unwrap();
    assert_eq!(loaded.key_data, b"key-data");
    assert_eq!(loaded.timestamp, 1_700_000_000);

    assert_eq!(
        store.get_latest_sync_key_id().await.unwrap(),
        Some(b"key-id-1".to_vec())
    );
}

#[tokio::test]
#[ignore]
async fn test_app_state_version_roundtrip() {
    let store = create_test_store().await;
    let state = HashState {
        version: 7,
        hash: {
            let mut h = [0u8; 128];
            h[0] = 0x42;
            h
        },
        ..Default::default()
    };

    store
        .set_version("critical_block", state.clone())
        .await
        .unwrap();
    let loaded = store.get_version("critical_block").await.unwrap();
    assert_eq!(loaded.version, 7);
    assert_eq!(loaded.hash[0], 0x42);
}

#[tokio::test]
#[ignore]
async fn test_app_state_mutation_macs_roundtrip() {
    let store = create_test_store().await;
    let mutations = vec![
        AppStateMutationMAC {
            index_mac: b"idx-1".to_vec(),
            value_mac: b"val-1".to_vec(),
        },
        AppStateMutationMAC {
            index_mac: b"idx-2".to_vec(),
            value_mac: b"val-2".to_vec(),
        },
    ];

    store
        .put_mutation_macs("critical_block", 5, &mutations)
        .await
        .unwrap();

    assert_eq!(
        store
            .get_mutation_mac("critical_block", b"idx-1")
            .await
            .unwrap(),
        Some(b"val-1".to_vec())
    );

    // Batch fetch.
    let batch = store
        .get_mutation_macs("critical_block", &[[0x00u8; 32], [0x01u8; 32]])
        .await
        .unwrap();
    assert!(batch.is_empty());

    store
        .delete_mutation_macs("critical_block", &[b"idx-1".to_vec(), b"idx-2".to_vec()])
        .await
        .unwrap();
    assert_eq!(
        store
            .get_mutation_mac("critical_block", b"idx-1")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
#[ignore]
async fn test_app_state_clear_mutation_macs() {
    let store = create_test_store().await;
    let mutations = vec![AppStateMutationMAC {
        index_mac: b"idx-1".to_vec(),
        value_mac: b"val-1".to_vec(),
    }];
    store
        .put_mutation_macs("critical_block", 1, &mutations)
        .await
        .unwrap();

    store.clear_mutation_macs("critical_block").await.unwrap();
    assert_eq!(
        store
            .get_mutation_mac("critical_block", b"idx-1")
            .await
            .unwrap(),
        None
    );
}

// ---------------------------------------------------------------------------
// ProtocolStore
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn test_sender_key_devices_set_get_clear() {
    let store = create_test_store().await;
    let group = "120363000000000000@g.us";

    store
        .set_sender_key_status(group, &[("15550000010:0", true), ("15550000011:1", false)])
        .await
        .unwrap();

    let devices = store.get_sender_key_devices(group).await.unwrap();
    assert_eq!(devices.len(), 2);
    let has: std::collections::HashMap<&str, bool> =
        devices.iter().map(|(j, b)| (j.as_str(), *b)).collect();
    assert_eq!(has.get("15550000010:0"), Some(&true));
    assert_eq!(has.get("15550000011:1"), Some(&false));

    // Upsert overwrites.
    store
        .set_sender_key_status(group, &[("15550000011:1", true)])
        .await
        .unwrap();
    let devices = store.get_sender_key_devices(group).await.unwrap();
    let v11: bool = devices
        .iter()
        .find(|(j, _)| j == "15550000011:1")
        .map(|(_, b)| *b)
        .unwrap();
    assert!(v11);

    // Row-scoped delete.
    store
        .delete_sender_key_device_rows(&["15550000010:0"])
        .await
        .unwrap();
    let devices = store.get_sender_key_devices(group).await.unwrap();
    assert_eq!(devices.len(), 1);

    store.clear_all_sender_key_devices().await.unwrap();
    assert!(
        store
            .get_sender_key_devices(group)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
#[ignore]
async fn test_lid_pn_mapping_roundtrip() {
    let store = create_test_store().await;
    let entry = LidPnMappingEntry {
        lid: "100000012345678".to_string(),
        phone_number: "15550000099".to_string(),
        created_at: 1_700_000_000,
        updated_at: 1_700_000_100,
        learning_source: "usync".to_string(),
    };

    store.put_lid_mapping(&entry).await.unwrap();

    let by_lid = store.get_lid_mapping("100000012345678").await.unwrap();
    assert_eq!(by_lid.unwrap().phone_number, "15550000099");
    let by_pn = store.get_pn_mapping("15550000099").await.unwrap();
    assert_eq!(by_pn.unwrap().lid, "100000012345678");

    let all = store.get_all_lid_mappings().await.unwrap();
    assert!(all.iter().any(|e| e.lid == "100000012345678"));
}

#[tokio::test]
#[ignore]
async fn test_base_key_collision_detection() {
    let store = create_test_store().await;
    let addr = "15550000012:0";

    store
        .save_base_key(addr, "msg-1", b"base-key-1")
        .await
        .unwrap();
    assert!(
        store
            .has_same_base_key(addr, "msg-1", b"base-key-1")
            .await
            .unwrap()
    );
    assert!(
        !store
            .has_same_base_key(addr, "msg-1", b"base-key-2")
            .await
            .unwrap()
    );

    store.delete_base_key(addr, "msg-1").await.unwrap();
    assert!(
        !store
            .has_same_base_key(addr, "msg-1", b"base-key-1")
            .await
            .unwrap()
    );
}

#[tokio::test]
#[ignore]
async fn test_device_registry_save_and_get() {
    let store = create_test_store().await;
    let record = DeviceListRecord {
        user: "15550000013".to_string(),
        devices: vec![
            DeviceInfo::new(0, None),
            DeviceInfo::new(1, Some(42)).with_hosting(true),
        ],
        timestamp: 1_234_567_890,
        phash: Some("2:abcdef".to_string()),
        raw_id: Some(9),
    };

    store.update_device_list(record).await.unwrap();
    let loaded = store.get_devices("15550000013").await.unwrap().unwrap();

    assert_eq!(loaded.user, "15550000013");
    assert_eq!(loaded.devices.len(), 2);
    assert_eq!(loaded.devices[1].device_id, 1);
    assert_eq!(loaded.devices[1].key_index, Some(42));
    assert!(loaded.devices[1].is_hosted);
    assert_eq!(loaded.phash.as_deref(), Some("2:abcdef"));
    assert_eq!(loaded.raw_id, Some(9));

    store.delete_devices("15550000013").await.unwrap();
    assert!(store.get_devices("15550000013").await.unwrap().is_none());
}

#[tokio::test]
#[ignore]
async fn test_group_metadata_roundtrip() {
    let store = create_test_store().await;
    let group = "120363000000000001@g.us";

    store
        .put_group_metadata(group, b"serialized-group-blob")
        .await
        .unwrap();
    assert_eq!(
        store.get_group_metadata(group).await.unwrap(),
        Some(b"serialized-group-blob".to_vec())
    );

    store.delete_group_metadata(group).await.unwrap();
    assert_eq!(store.get_group_metadata(group).await.unwrap(), None);
}

#[tokio::test]
#[ignore]
async fn test_tc_token_put_get_delete_expired() {
    let store = create_test_store().await;
    let entry = TcTokenEntry {
        token: b"token-blob".to_vec(),
        token_timestamp: 1_700_000_000,
        sender_timestamp: Some(1_700_000_050),
    };

    store.put_tc_token("15550000014:0", &entry).await.unwrap();
    let loaded = store.get_tc_token("15550000014:0").await.unwrap().unwrap();
    assert_eq!(loaded.token, b"token-blob");
    assert_eq!(loaded.sender_timestamp, Some(1_700_000_050));

    assert!(
        store
            .get_all_tc_token_jids()
            .await
            .unwrap()
            .iter()
            .any(|j| j == "15550000014:0")
    );

    // Both token and sender bucket expired -> row is removed.
    let deleted = store
        .delete_expired_tc_tokens(1_800_000_000, 1_800_000_000)
        .await
        .unwrap();
    assert!(deleted >= 1);
    assert!(store.get_tc_token("15550000014:0").await.unwrap().is_none());
}

#[tokio::test]
#[ignore]
async fn test_tc_token_sender_timestamp_atomicity() {
    let store = create_test_store().await;
    let jid = "15550000015:0";

    // Sender-only write inserts a placeholder row.
    store
        .touch_tc_token_sender_timestamp(jid, 1_700_000_100)
        .await
        .unwrap();
    let entry = store.get_tc_token(jid).await.unwrap().unwrap();
    assert_eq!(entry.sender_timestamp, Some(1_700_000_100));

    // A later sender timestamp advances; an earlier one does not regress.
    store
        .touch_tc_token_sender_timestamp(jid, 1_700_000_200)
        .await
        .unwrap();
    store
        .touch_tc_token_sender_timestamp(jid, 1_700_000_050)
        .await
        .unwrap();
    let entry = store.get_tc_token(jid).await.unwrap().unwrap();
    assert_eq!(entry.sender_timestamp, Some(1_700_000_200));

    // Receiver write preserves the sender bucket and newer-wins on the token.
    store
        .store_received_tc_token(jid, b"real-token", 1_700_000_150)
        .await
        .unwrap();
    let entry = store.get_tc_token(jid).await.unwrap().unwrap();
    assert_eq!(entry.token, b"real-token");
    assert_eq!(entry.sender_timestamp, Some(1_700_000_200));
}

#[tokio::test]
#[ignore]
async fn test_sent_message_store_and_take() {
    let store = create_test_store().await;

    store
        .store_sent_message("15550000016@s.whatsapp.net", "msg-1", b"payload-v1")
        .await
        .unwrap();
    store
        .store_sent_message("15550000016@s.whatsapp.net", "msg-1", b"payload-v2")
        .await
        .unwrap();

    let taken = store
        .take_sent_message("15550000016@s.whatsapp.net", "msg-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(taken, b"payload-v2".to_vec());

    // Second take is consumed.
    assert_eq!(
        store
            .take_sent_message("15550000016@s.whatsapp.net", "msg-1")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
#[ignore]
async fn test_pending_inbound_batch() {
    let store = create_test_store().await;
    let rows = [
        PendingInboundRow {
            chat: "15550000017@s.whatsapp.net",
            sender: "15550000017:0",
            id: "stanza-1",
            message: b"encrypted-bytes",
        },
        PendingInboundRow {
            chat: "15550000017@s.whatsapp.net",
            sender: "15550000018:0",
            id: "stanza-2",
            message: b"more-bytes",
        },
    ];

    store.store_pending_inbound_batch(&rows).await.unwrap();
    assert_eq!(
        store
            .get_pending_inbound("15550000017@s.whatsapp.net", "15550000017:0", "stanza-1")
            .await
            .unwrap(),
        Some(b"encrypted-bytes".to_vec())
    );

    store
        .delete_pending_inbound("15550000017@s.whatsapp.net", "15550000017:0", "stanza-1")
        .await
        .unwrap();
    assert_eq!(
        store
            .get_pending_inbound("15550000017@s.whatsapp.net", "15550000017:0", "stanza-1")
            .await
            .unwrap(),
        None
    );
}

// ---------------------------------------------------------------------------
// MsgSecretStore
// ---------------------------------------------------------------------------

fn msg_secret_entry(chat: &str, sender: &str, msg_id: &str, ts: i64) -> MsgSecretEntry {
    MsgSecretEntry {
        chat: Arc::from(chat),
        sender: Arc::from(sender),
        msg_id: Arc::from(msg_id),
        secret: [0x42u8; MESSAGE_SECRET_SIZE],
        expires_at: 0,
        message_ts: ts,
    }
}

#[tokio::test]
#[ignore]
async fn test_msg_secret_roundtrip_with_ts() {
    let store = create_test_store().await;
    let entry = msg_secret_entry(
        "15550000019@s.whatsapp.net",
        "15550000019:0",
        "msg-secret-1",
        1_700_000_000,
    );
    store.put_msg_secrets(vec![entry]).await.unwrap();

    let (secret, ts) = store
        .get_msg_secret_with_ts(
            "15550000019@s.whatsapp.net",
            "15550000019:0",
            "msg-secret-1",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(secret, vec![0x42u8; MESSAGE_SECRET_SIZE]);
    assert_eq!(ts, 1_700_000_000);

    // get_msg_secret returns just the secret.
    assert_eq!(
        store
            .get_msg_secret(
                "15550000019@s.whatsapp.net",
                "15550000019:0",
                "msg-secret-1"
            )
            .await
            .unwrap(),
        Some(vec![0x42u8; MESSAGE_SECRET_SIZE])
    );
}

#[tokio::test]
#[ignore]
async fn test_msg_secret_merge_semantics() {
    let store = create_test_store().await;

    // First write: short window, known parent ts.
    store
        .put_msg_secrets(vec![msg_secret_entry(
            "15550000020@s.whatsapp.net",
            "15550000020:0",
            "m-1",
            100,
        )])
        .await
        .unwrap();

    // Re-persist with a later expires_at and later parent ts.
    let mut entry = msg_secret_entry("15550000020@s.whatsapp.net", "15550000020:0", "m-1", 200);
    entry.expires_at = 50;
    store.put_msg_secrets(vec![entry]).await.unwrap();

    let (_, ts) = store
        .get_msg_secret_with_ts("15550000020@s.whatsapp.net", "15550000020:0", "m-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ts, 200, "later parent ts wins");

    // Expiry never shrinks: a fresh 0 (never) write must not get expired away.
    store
        .put_msg_secrets(vec![msg_secret_entry(
            "15550000020@s.whatsapp.net",
            "15550000020:0",
            "m-1",
            300,
        )])
        .await
        .unwrap();
    let deleted = store.delete_expired_msg_secrets(1_000_000).await.unwrap();
    assert_eq!(deleted, 0, "expires_at=0 rows never expire");
}

// ---------------------------------------------------------------------------
// Factory: jid -> device_id mapping (the multi-pod shared-store contract)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn test_storage_factory_jid_mapping() {
    ensure_migrations();
    let url = db_url();
    let factory = PostgresStorageFactory::new(url.clone());

    let jid = dummy_jid(7);

    // Initially absent.
    assert!(factory.for_jid(&jid).await.is_none());

    // Create resolves to a real device_id and backend.
    let (device_id, backend) = factory.create_for_jid(&jid).await.unwrap();
    assert!(device_id > 0);
    assert!(backend.exists().await.unwrap());

    // for_jid and backend_for_device_id now both resolve.
    assert!(factory.for_jid(&jid).await.is_some());
    assert!(factory.backend_for_device_id(device_id).is_some());

    // Idempotent: a second create returns the same device_id.
    let (device_id2, _backend2) = factory.create_for_jid(&jid).await.unwrap();
    assert_eq!(device_id, device_id2);

    // The factory-created backend persists to the same tables the store reads.
    let factory_backend = factory.for_jid(&jid).await.unwrap();
    let mut device = factory_backend
        .load()
        .await
        .unwrap()
        .expect("device present");
    device.push_name = "factory-pod".to_string();
    factory_backend.save(&device).await.unwrap();
    let loaded = backend.load().await.unwrap().unwrap();
    assert_eq!(loaded.push_name, "factory-pod");

    // delete cascades.
    factory.delete_for_jid(&jid).await.unwrap();
    assert!(factory.for_jid(&jid).await.is_none());
}

#[tokio::test]
#[ignore]
async fn test_multiple_devices_isolated() {
    ensure_migrations();
    let url = db_url();

    let bootstrap = PostgresStore::new(&url).unwrap();
    let id_a = bootstrap.create().await.unwrap();
    let id_b = bootstrap.create().await.unwrap();
    let store_a = PostgresStore::new_for_device(&url, id_a).unwrap();
    let store_b = PostgresStore::new_for_device(&url, id_b).unwrap();

    store_a
        .put_identity("15550000030:0", [0x11u8; 32])
        .await
        .unwrap();
    assert_eq!(store_b.load_identity("15550000030:0").await.unwrap(), None);

    store_b
        .put_identity("15550000030:0", [0x22u8; 32])
        .await
        .unwrap();
    assert_eq!(
        store_a.load_identity("15550000030:0").await.unwrap(),
        Some([0x11u8; 32])
    );
    assert_eq!(
        store_b.load_identity("15550000030:0").await.unwrap(),
        Some([0x22u8; 32])
    );
}
