import { DatabaseSync } from 'node:sqlite';
import { readdirSync, readFileSync } from 'node:fs';
import { afterEach, beforeEach, expect, it } from 'vitest';
import { createAccessToken, getUserByTokenHash } from '../src/services/database';

let db: DatabaseSync;
let d1: D1Database;
beforeEach(() => {
  db = new DatabaseSync(':memory:');
  for (const file of readdirSync('migrations').filter(f => f.endsWith('.sql')).sort()) db.exec(readFileSync(`migrations/${file}`, 'utf8'));
  db.exec("INSERT INTO users(user_id,localpart) VALUES ('@alice:m.sgr.ski','alice')");
  d1 = { prepare: (sql: string) => ({ bind: (...args: any[]) => ({
    run: async () => db.prepare(sql).run(...args), first: async () => db.prepare(sql).get(...args) ?? null,
  }) }) } as unknown as D1Database;
});
afterEach(() => db.close());
it('applies the complete schema and enforces token expiry and deactivation', async () => {
  await createAccessToken(d1, 'expired', 'expired-hash', '@alice:m.sgr.ski', 'A', Date.now() - 1000);
  await createAccessToken(d1, 'valid', 'valid-hash', '@alice:m.sgr.ski', 'A', Date.now() + 60000);
  expect(await getUserByTokenHash(d1, 'expired-hash')).toBeNull();
  expect(await getUserByTokenHash(d1, 'valid-hash')).toEqual({ userId: '@alice:m.sgr.ski', deviceId: 'A' });
  db.exec("UPDATE users SET is_deactivated = 1");
  expect(await getUserByTokenHash(d1, 'valid-hash')).toBeNull();
});
it('searches messages and removes redacted content from the index', () => {
  db.exec(`INSERT INTO rooms(room_id) VALUES ('!test:m.sgr.ski');
    INSERT INTO events(event_id,room_id,sender,event_type,content,origin_server_ts,depth,auth_events,prev_events)
    VALUES ('$test','!test:m.sgr.ski','@alice:m.sgr.ski','m.room.message','{"body":"hello world"}',1,1,'[]','[]');`);
  expect(db.prepare("SELECT event_id FROM events_fts WHERE body MATCH 'hello'").all()).toHaveLength(1);
  db.exec("UPDATE events SET content = '{}' WHERE event_id = '$test'");
  expect(db.prepare("SELECT event_id FROM events_fts WHERE body MATCH 'hello'").all()).toHaveLength(0);
  db.exec("DELETE FROM events WHERE event_id = '$test'");
  expect(db.prepare('SELECT * FROM events_fts').all()).toHaveLength(0);
});
