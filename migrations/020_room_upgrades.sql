-- One resumable upgrade per original room. The signed plan fixes all event and
-- room IDs before publishing a replacement, even when a request is retried.
CREATE TABLE IF NOT EXISTS room_upgrades (
    old_room_id TEXT PRIMARY KEY REFERENCES rooms(room_id),
    new_version TEXT NOT NULL,
    actor_user_id TEXT NOT NULL,
    additional_creators TEXT NOT NULL DEFAULT '[]',
    replacement_room_id TEXT,
    plan_json TEXT,
    phase TEXT NOT NULL DEFAULT 'reserved',
    member_cursor INTEGER NOT NULL DEFAULT 0,
    pending_event_json TEXT,
    lease_token TEXT,
    lease_until INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_room_upgrade_replacement ON room_upgrades(replacement_room_id);
