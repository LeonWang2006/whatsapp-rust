-- Maps a JID (account identifier) to its device row id.
-- Lets the StorageFactory resolve which PostgresStore instance to build
-- for a given session without scanning the device table.
CREATE TABLE jid_device_map (
    jid        TEXT    PRIMARY KEY,
    device_id  INTEGER NOT NULL REFERENCES device(id) ON DELETE CASCADE,
    created_at INTEGER NOT NULL DEFAULT (EXTRACT(EPOCH FROM clock_timestamp())::INT)
);

CREATE INDEX idx_jid_device_map_device_id ON jid_device_map (device_id);
