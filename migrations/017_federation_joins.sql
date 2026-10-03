-- Serialize initial room imports. Locks expire after an interrupted request.
CREATE TABLE IF NOT EXISTS federation_join_locks (
    room_id TEXT PRIMARY KEY,
    lock_id TEXT NOT NULL,
    expires_at INTEGER NOT NULL
);
