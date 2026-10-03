-- Record the complete state BEFORE an event separately from today's room_state.
-- Archive verified peer state/auth PDUs without publishing them to the live
-- timeline or making user-scoped backfill messages globally client-readable.
CREATE TABLE IF NOT EXISTS event_state_snapshots (
    event_id TEXT PRIMARY KEY,
    room_id TEXT NOT NULL REFERENCES rooms(room_id) ON DELETE CASCADE,
    state_before TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_event_state_snapshots_room ON event_state_snapshots(room_id);
CREATE TABLE IF NOT EXISTS event_state_archive (
    event_id TEXT PRIMARY KEY,
    room_id TEXT NOT NULL REFERENCES rooms(room_id) ON DELETE CASCADE,
    event_json TEXT NOT NULL,
    redacted INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_event_state_archive_room ON event_state_archive(room_id);
