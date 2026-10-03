-- Room versions up to 10 keep the redaction target outside content. Preserve it
-- when replaying signed federation events; dropping it changes the content hash.
ALTER TABLE events ADD COLUMN redacts TEXT;
