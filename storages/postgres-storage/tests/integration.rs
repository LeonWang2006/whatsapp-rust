//! Integration tests for PostgresStore.
//!
//! These require a live PostgreSQL instance. They are `#[ignore]` by default
//! so `cargo test` doesn't fail in environments without PG. Run with:
//!
//! ```bash
//! DATABASE_URL=postgres://wa:wa@localhost:5432/wa_test \
//!     cargo test -p whatsapp-rust-postgres-storage -- --ignored
//! ```
//!
//! Each test creates an isolated device row to avoid cross-test interference.

use wacore::appstate::hash::HashState;
use wacore::appstate::processor::AppStateMutationMAC;
use wacore::store::traits::*;
use whatsapp_rust_postgres_storage::PostgresStore;

fn db_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        eprintln!("DATABASE_URL not set; skipping PostgresStore integration tests");
        "postgres://localhost/wa_test".to_string()
    })
}

// Migrations are not safe to run concurrently and must not be driven from
// inside an existing tokio runtime. The first test to hit this guard spawns a
// dedicated OS thread that owns its own runtime; subsequent tests skip it.
static MIGRATION_GUARD: std::sync::Once = std::sync::Once::new();

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

async fn create_test_store() -> PostgresStore {
    ensure_migrations();
    let store = PostgresStore::new(&db_url())
        .await
        .expect("Failed to create test store");
    let _ = store.create().await.expect("create device row");
    store
}

#[tokio::test]
#[ignore]
async fn test_device_create_and_exists() {
    let store = create_test_store().await;
    assert!(store.exists().await.unwrap());
}

#[tokio::test]
#[ignore]
async fn test_device_save_and_load() {
    let store = create_test_store().await;
    let mut device = wacore::store::Device::new();
    device.push_name = "test-pod".to_string();
    store.save(&device).await.expect("save failed");

    let loaded = store.load().await.expect("load failed").expect("present");
    assert_eq!(loaded.push_name, "test-pod");
}

#[tokio::test]
#[ignore]
async fn test_identity_put_load_delete() {
    let store = create_test_store().await;
    let addr = "1234567890:1";
    let key = [0xabu8; 32];

    store.put_identity(addr, key).await.unwrap();
    let loaded = store.load_identity(addr).await.unwrap();
    assert_eq!(loaded, Some(key.to_vec()));

    store.delete_identity(addr).await.unwrap();
    let loaded = store.load_identity(addr).await.unwrap();
    assert_eq!(loaded, None);
}

#[tokio::test]
#[ignore]
async fn test_session_put_get_delete() {
    let store = create_test_store().await;
    let addr = "9876543210:2";
    let record = b"session-blob".to_vec();

    store.put_session(addr, &record).await.unwrap();
    let loaded = store.get_session(addr).await.unwrap();
    assert_eq!(loaded, Some(record.clone()));

    store.delete_session(addr).await.unwrap();
    assert_eq!(store.get_session(addr).await.unwrap(), None);
}

#[tokio::test]
#[ignore]
async fn test_prekey_store_load_remove() {
    let store = create_test_store().await;
    let record = b"prekey-blob".to_vec();

    store.store_prekey(42, &record, false).await.unwrap();
    let loaded = store.load_prekey(42).await.unwrap();
    assert_eq!(loaded, Some(record.clone()));

    store.remove_prekey(42).await.unwrap();
    assert_eq!(store.load_prekey(42).await.unwrap(), None);
}

#[tokio::test]
#[ignore]
async fn test_prekeys_batch() {
    let store = create_test_store().await;
    let keys = vec![
        (1u32, b"k1".to_vec()),
        (2u32, b"k2".to_vec()),
        (3u32, b"k3".to_vec()),
    ];
    store.store_prekeys_batch(&keys, true).await.unwrap();

    assert_eq!(store.load_prekey(1).await.unwrap(), Some(b"k1".to_vec()));
    assert_eq!(store.load_prekey(2).await.unwrap(), Some(b"k2".to_vec()));
    assert_eq!(store.load_prekey(3).await.unwrap(), Some(b"k3".to_vec()));

    let max_id = store.get_max_prekey_id().await.unwrap();
    assert_eq!(max_id, 3);
}

#[tokio::test]
#[ignore]
async fn test_signed_prekey_store_load() {
    let store = create_test_store().await;
    let record = b"signed-prekey-blob".to_vec();

    store.store_signed_prekey(7, &record).await.unwrap();
    let loaded = store.load_signed_prekey(7).await.unwrap();
    assert_eq!(loaded, Some(record.clone()));

    let all = store.load_all_signed_prekeys().await.unwrap();
    assert!(all.iter().any(|(id, r)| *id == 7 && r == &record));

    store.remove_signed_prekey(7).await.unwrap();
    assert_eq!(store.load_signed_prekey(7).await.unwrap(), None);
}

#[tokio::test]
#[ignore]
async fn test_sender_key_devices_set_get_clear() {
    let store = create_test_store().await;
    let group = "group@g.us";

    store
        .set_sender_key_status(group, &[("111:0", true), ("222:1", false), ("333:0", true)])
        .await
        .unwrap();

    let devices = store.get_sender_key_devices(group).await.unwrap();
    assert_eq!(devices.len(), 3);
    let has_map: std::collections::HashMap<&str, bool> =
        devices.iter().map(|(j, b)| (j.as_str(), *b)).collect();
    assert_eq!(has_map.get("111:0"), Some(&true));
    assert_eq!(has_map.get("222:1"), Some(&false));
    assert_eq!(has_map.get("333:0"), Some(&true));

    // Upsert overwrites
    store
        .set_sender_key_status(group, &[("222:1", true)])
        .await
        .unwrap();
    let devices = store.get_sender_key_devices(group).await.unwrap();
    let has_222: bool = devices
        .iter()
        .find(|(j, _)| j == "222:1")
        .map(|(_, b)| *b)
        .unwrap();
    assert!(has_222);

    store.clear_sender_key_devices(group).await.unwrap();
    assert_eq!(store.get_sender_key_devices(group).await.unwrap().len(), 0);
}

#[tokio::test]
#[ignore]
async fn test_lid_pn_mapping_roundtrip() {
    let store = create_test_store().await;
    let entry = LidPnMappingEntry {
        lid: "100000012345678".to_string(),
        phone_number: "559980000001".to_string(),
        created_at: 1_700_000_000,
        updated_at: 1_700_000_100,
        learning_source: "usync".to_string(),
    };

    store.put_lid_mapping(&entry).await.unwrap();

    let by_lid = store.get_lid_mapping("100000012345678").await.unwrap();
    assert_eq!(by_lid.as_ref().unwrap().phone_number, "559980000001");

    let by_pn = store.get_pn_mapping("559980000001").await.unwrap();
    assert_eq!(by_pn.as_ref().unwrap().lid, "100000012345678");

    let all = store.get_all_lid_mappings().await.unwrap();
    assert!(all.iter().any(|e| e.lid == "100000012345678"));
}

#[tokio::test]
#[ignore]
async fn test_device_registry_save_and_get() {
    let store = create_test_store().await;
    let record = DeviceListRecord {
        user: "1234567890".to_string(),
        devices: vec![
            DeviceInfo {
                device_id: 0,
                key_index: None,
            },
            DeviceInfo {
                device_id: 1,
                key_index: Some(42),
            },
        ],
        timestamp: 1234567890,
        phash: Some("2:abcdef".to_string()),
    };

    store.update_device_list(record).await.unwrap();
    let loaded = store.get_devices("1234567890").await.unwrap().unwrap();

    assert_eq!(loaded.user, "1234567890");
    assert_eq!(loaded.devices.len(), 2);
    assert_eq!(loaded.devices[0].device_id, 0);
    assert_eq!(loaded.devices[1].device_id, 1);
    assert_eq!(loaded.devices[1].key_index, Some(42));
    assert_eq!(loaded.phash, Some("2:abcdef".to_string()));
}

#[tokio::test]
#[ignore]
async fn test_tc_token_put_get_delete() {
    let store = create_test_store().await;
    let entry = TcTokenEntry {
        token: b"token-blob".to_vec(),
        token_timestamp: 1_700_000_000,
        sender_timestamp: Some(1_700_000_050),
    };

    store.put_tc_token("jid:1", &entry).await.unwrap();
    let loaded = store.get_tc_token("jid:1").await.unwrap().unwrap();
    assert_eq!(loaded.token, b"token-blob");
    assert_eq!(loaded.token_timestamp, 1_700_000_000);
    assert_eq!(loaded.sender_timestamp, Some(1_700_000_050));

    let jids = store.get_all_tc_token_jids().await.unwrap();
    assert!(jids.iter().any(|j| j == "jid:1"));

    let deleted = store.delete_expired_tc_tokens(1_800_000_000).await.unwrap();
    assert!(deleted >= 1);
    assert!(store.get_tc_token("jid:1").await.unwrap().is_none());
}

#[tokio::test]
#[ignore]
async fn test_sent_message_store_and_take() {
    let store = create_test_store().await;

    store
        .store_sent_message("chat@s.whatsapp.net", "msg-1", b"payload-v1")
        .await
        .unwrap();

    // Upsert overwrites
    store
        .store_sent_message("chat@s.whatsapp.net", "msg-1", b"payload-v2")
        .await
        .unwrap();

    let taken = store
        .take_sent_message("chat@s.whatsapp.net", "msg-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(taken, b"payload-v2".to_vec());

    // Second take returns None (already consumed)
    let taken2 = store
        .take_sent_message("chat@s.whatsapp.net", "msg-1")
        .await
        .unwrap();
    assert_eq!(taken2, None);
}

#[tokio::test]
#[ignore]
async fn test_app_state_sync_key_roundtrip() {
    let store = create_test_store().await;
    let key_id = b"key-id-1";
    let key = AppStateSyncKey {
        key_data: b"key-data".to_vec(),
        fingerprint: b"fp".to_vec(),
        timestamp: 1_700_000_000,
    };

    store.set_sync_key(key_id, key.clone()).await.unwrap();
    let loaded = store.get_sync_key(key_id).await.unwrap().unwrap();
    assert_eq!(loaded.key_data, b"key-data");
    assert_eq!(loaded.fingerprint, b"fp");
    assert_eq!(loaded.timestamp, 1_700_000_000);

    let latest_id = store.get_latest_sync_key_id().await.unwrap();
    assert_eq!(latest_id, Some(key_id.to_vec()));
}

#[tokio::test]
#[ignore]
async fn test_app_state_version_roundtrip() {
    let store = create_test_store().await;
    let state = HashState::default();

    store
        .set_version("critical_block", state.clone())
        .await
        .unwrap();
    let loaded = store.get_version("critical_block").await.unwrap();
    assert_eq!(loaded.version, state.version);
    assert_eq!(loaded.hash, state.hash);
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

    let mac = store
        .get_mutation_mac("critical_block", b"idx-1")
        .await
        .unwrap();
    assert_eq!(mac, Some(b"val-1".to_vec()));

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
async fn test_multiple_devices_isolated() {
    // Verify two device_ids in the same DB don't see each other's data.
    ensure_migrations();
    let url = db_url();

    // Create two device rows via the default store (device_id=1 placeholder),
    // then re-open stores bound to the actual assigned IDs.
    let bootstrap = PostgresStore::new(&url).await.unwrap();
    let id_a = bootstrap.create().await.unwrap();
    let id_b = bootstrap.create().await.unwrap();
    let store_a = PostgresStore::new_for_device(&url, id_a).await.unwrap();
    let store_b = PostgresStore::new_for_device(&url, id_b).await.unwrap();

    store_a
        .put_identity("shared:1", [0x11u8; 32])
        .await
        .unwrap();
    // store_b should NOT see store_a's identity
    let loaded_b = store_b.load_identity("shared:1").await.unwrap();
    assert_eq!(loaded_b, None);

    store_b
        .put_identity("shared:1", [0x22u8; 32])
        .await
        .unwrap();
    let loaded_a = store_a.load_identity("shared:1").await.unwrap();
    assert_eq!(loaded_a, Some([0x11u8; 32].to_vec()));
    let loaded_b = store_b.load_identity("shared:1").await.unwrap();
    assert_eq!(loaded_b, Some([0x22u8; 32].to_vec()));
}

#[tokio::test]
#[ignore]
async fn test_storage_factory_jid_mapping() {
    use wacore::store::StorageFactory;
    use whatsapp_rust_postgres_storage::PostgresStorageFactory;

    ensure_migrations();
    let url = db_url();
    let factory = PostgresStorageFactory::new(url.clone());

    let jid = format!("fakejid:{}@s.whatsapp.net", std::process::id());

    // Initially absent.
    assert!(factory.for_jid(&jid).await.is_none());

    // Create resolves to a real device_id and backend.
    let (device_id, _backend) = factory.create_for_jid(&jid).await.unwrap();
    assert!(device_id > 0);

    // for_jid now resolves and for_device_id too.
    assert!(factory.for_jid(&jid).await.is_some());
    assert!(factory.for_device_id(device_id).await.is_some());

    // Idempotent: a second create returns the same device_id.
    let (device_id2, _backend2) = factory.create_for_jid(&jid).await.unwrap();
    assert_eq!(device_id, device_id2);

    // delete cascades.
    factory.delete_for_jid(&jid).await.unwrap();
    assert!(factory.for_jid(&jid).await.is_none());
}
