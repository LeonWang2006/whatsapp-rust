// @generated automatically by Diesel CLI (PG backend).
// Matches the migration in `migrations/2026-08-15-000000_initial/up.sql`.

diesel::table! {
    app_state_keys (key_id, device_id) {
        key_id -> Bytea,
        key_data -> Bytea,
        device_id -> Int4,
    }
}

diesel::table! {
    app_state_mutation_macs (name, index_mac, device_id) {
        name -> Text,
        version -> Int8,
        index_mac -> Bytea,
        value_mac -> Bytea,
        device_id -> Int4,
    }
}

diesel::table! {
    app_state_versions (name, device_id) {
        name -> Text,
        state_data -> Bytea,
        device_id -> Int4,
    }
}

diesel::table! {
    base_keys (address, message_id, device_id) {
        address -> Text,
        message_id -> Text,
        base_key -> Bytea,
        device_id -> Int4,
        created_at -> Int4,
    }
}

diesel::table! {
    device (id) {
        id -> Int4,
        lid -> Text,
        pn -> Text,
        registration_id -> Int4,
        noise_key -> Bytea,
        identity_key -> Bytea,
        signed_pre_key -> Bytea,
        signed_pre_key_id -> Int4,
        signed_pre_key_signature -> Bytea,
        adv_secret_key -> Bytea,
        account -> Nullable<Bytea>,
        push_name -> Text,
        app_version_primary -> Int4,
        app_version_secondary -> Int4,
        app_version_tertiary -> Int8,
        app_version_last_fetched_ms -> Int8,
        edge_routing_info -> Nullable<Bytea>,
        props_hash -> Nullable<Text>,
        next_pre_key_id -> Int4,
        nct_salt -> Nullable<Bytea>,
        server_has_prekeys -> Bool,
        first_unupload_pre_key_id -> Int4,
        server_cert_chain -> Nullable<Bytea>,
        login_counter -> Int4,
        lid_migrated -> Bool,
        last_signed_pre_key_rotation_ms -> Int8,
        read_receipts_disabled -> Bool,
    }
}

diesel::table! {
    device_registry (user_id, device_id) {
        user_id -> Text,
        devices_json -> Text,
        timestamp -> Int4,
        phash -> Nullable<Text>,
        device_id -> Int4,
        updated_at -> Int4,
        raw_id -> Nullable<Int4>,
    }
}

diesel::table! {
    group_metadata (group_jid, device_id) {
        group_jid -> Text,
        info -> Bytea,
        device_id -> Int4,
        updated_at -> Int8,
    }
}

diesel::table! {
    identities (address, device_id) {
        address -> Text,
        key -> Bytea,
        device_id -> Int4,
    }
}

diesel::table! {
    jid_device_map (jid) {
        jid -> Text,
        device_id -> Int4,
        created_at -> Int4,
    }
}

diesel::table! {
    lid_pn_mapping (lid, device_id) {
        lid -> Text,
        phone_number -> Text,
        created_at -> Int8,
        learning_source -> Text,
        updated_at -> Int8,
        device_id -> Int4,
    }
}

diesel::table! {
    msg_secrets (chat, sender, msg_id, device_id) {
        chat -> Text,
        sender -> Text,
        msg_id -> Text,
        secret -> Bytea,
        device_id -> Int4,
        created_at -> Int8,
        expires_at -> Int8,
        message_ts -> Int8,
    }
}

diesel::table! {
    pending_inbound_messages (chat, sender, id, device_id) {
        chat -> Text,
        sender -> Text,
        id -> Text,
        message -> Bytea,
        device_id -> Int4,
        inserted_at -> Int8,
    }
}

diesel::table! {
    prekeys (id, device_id) {
        id -> Int4,
        key -> Bytea,
        uploaded -> Bool,
        device_id -> Int4,
    }
}

diesel::table! {
    sender_key_devices (group_jid, device_jid, device_id) {
        group_jid -> Text,
        device_jid -> Text,
        has_key -> Int4,
        device_id -> Int4,
        updated_at -> Int8,
    }
}

diesel::table! {
    sender_keys (address, device_id) {
        address -> Text,
        record -> Bytea,
        device_id -> Int4,
    }
}

diesel::table! {
    sent_messages (chat_jid, message_id, device_id) {
        chat_jid -> Text,
        message_id -> Text,
        payload -> Bytea,
        device_id -> Int4,
        created_at -> Int8,
    }
}

diesel::table! {
    sessions (address, device_id) {
        address -> Text,
        record -> Bytea,
        device_id -> Int4,
    }
}

diesel::table! {
    signed_prekeys (id, device_id) {
        id -> Int4,
        record -> Bytea,
        device_id -> Int4,
    }
}

diesel::table! {
    tc_tokens (jid, device_id) {
        jid -> Text,
        token -> Bytea,
        token_timestamp -> Int8,
        sender_timestamp -> Nullable<Int8>,
        device_id -> Int4,
        updated_at -> Int8,
    }
}

diesel::joinable!(app_state_keys -> device (device_id));
diesel::joinable!(app_state_mutation_macs -> device (device_id));
diesel::joinable!(app_state_versions -> device (device_id));
diesel::joinable!(base_keys -> device (device_id));
diesel::joinable!(device_registry -> device (device_id));
diesel::joinable!(group_metadata -> device (device_id));
diesel::joinable!(identities -> device (device_id));
diesel::joinable!(jid_device_map -> device (device_id));
diesel::joinable!(lid_pn_mapping -> device (device_id));
diesel::joinable!(msg_secrets -> device (device_id));
diesel::joinable!(pending_inbound_messages -> device (device_id));
diesel::joinable!(prekeys -> device (device_id));
diesel::joinable!(sender_key_devices -> device (device_id));
diesel::joinable!(sender_keys -> device (device_id));
diesel::joinable!(sent_messages -> device (device_id));
diesel::joinable!(sessions -> device (device_id));
diesel::joinable!(signed_prekeys -> device (device_id));
diesel::joinable!(tc_tokens -> device (device_id));

diesel::allow_tables_to_appear_in_same_query!(
    app_state_keys,
    app_state_mutation_macs,
    app_state_versions,
    base_keys,
    device,
    device_registry,
    group_metadata,
    identities,
    jid_device_map,
    lid_pn_mapping,
    msg_secrets,
    pending_inbound_messages,
    prekeys,
    sender_key_devices,
    sender_keys,
    sent_messages,
    sessions,
    signed_prekeys,
    tc_tokens,
);
