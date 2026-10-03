import type { Env } from '../types';
import { Errors } from '../utils/errors';
import { wakeDeviceSync } from './device-messages';

export async function accountDataPosition(db: D1Database): Promise<number> {
  const row = await db.prepare("SELECT position FROM stream_positions WHERE stream_name='account_data'").first<{ position: number }>();
  return row?.position ?? 0;
}

// A present expectedContent (null for no record) makes this a compare-and-set.
export async function storeAccountData(db: D1Database, userId: string, roomId: string, type: string,
  content: unknown, expectedContent?: string | null): Promise<boolean> {
  const serialized = JSON.stringify(content);
  const insert = expectedContent === undefined ? 'VALUES (?,?,?,?)' : `SELECT ?,?,?,? WHERE ${expectedContent === null
    ? 'NOT EXISTS(SELECT 1 FROM account_data WHERE user_id=? AND room_id=? AND event_type=?)'
    : 'EXISTS(SELECT 1 FROM account_data WHERE user_id=? AND room_id=? AND event_type=? AND content=?)'}`;
  const write = db.prepare(`INSERT INTO account_data(user_id,room_id,event_type,content) ${insert}
    ON CONFLICT(user_id,room_id,event_type) DO UPDATE SET content=excluded.content
    WHERE account_data.content<>excluded.content`);
  const bound = expectedContent === undefined ? write.bind(userId, roomId, type, serialized) : expectedContent === null
    ? write.bind(userId, roomId, type, serialized, userId, roomId, type)
    : write.bind(userId, roomId, type, serialized, userId, roomId, type, expectedContent);
  const result = await db.batch([
    bound,
    db.prepare(`INSERT INTO stream_positions(stream_name,position) SELECT 'account_data',1 WHERE changes()>0
      ON CONFLICT(stream_name) DO UPDATE SET position=position+1`),
    db.prepare(`INSERT INTO account_data_changes(user_id,room_id,event_type,stream_position)
      SELECT ?,?,?,position FROM stream_positions WHERE stream_name='account_data' AND changes()>0`).bind(userId, roomId, type),
  ]);
  return result[0].meta.changes > 0;
}

export async function mergeAccountData(db: D1Database, userId: string, roomId: string, type: string,
  merge: (current: string | undefined) => Record<string, unknown> | undefined | Promise<Record<string, unknown> | undefined>): Promise<boolean> {
  for (let attempt = 0; attempt < 8; attempt++) {
    const current = await db.prepare('SELECT content FROM account_data WHERE user_id=? AND room_id=? AND event_type=?')
      .bind(userId, roomId, type).first<{ content: string }>();
    const content = await merge(current?.content);
    if (!content || JSON.stringify(content) === current?.content) return false;
    if (await storeAccountData(db, userId, roomId, type, content, current?.content ?? null)) return true;
  }
  throw Errors.unknown('Account data changed concurrently; retry the request');
}

export async function notifyAccountDataUser(env: Env, userId: string): Promise<void> {
  await wakeDeviceSync(env, userId);
}

export async function publishAccountData(env: Env, userId: string, roomId: string, type: string, content: unknown): Promise<void> {
  await storeAccountData(env.DB, userId, roomId, type, content);
  // Retry delivery even when a previous attempt already committed this content.
  await notifyAccountDataUser(env, userId);
}
