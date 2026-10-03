import type { Env } from '../types';
import { parseUserId } from '../utils/ids';
import { isObject } from './federation-events';
import { wakeDeviceSync } from './device-messages';
import { isServerAllowedInRoom } from './server-acl';

export interface ReadReceipt {
  user_id: string;
  event_id: string;
  receipt_type: 'm.read' | 'm.read.private';
  ts: number;
  thread_id?: string;
}

export async function receiptPosition(db: D1Database): Promise<number> {
  const row = await db.prepare("SELECT position FROM stream_positions WHERE stream_name='receipts'").first<{ position: number }>();
  return row?.position ?? 0;
}

export async function advanceReceiptPosition(db: D1Database): Promise<void> {
  await db.prepare(`INSERT INTO stream_positions(stream_name,position) VALUES ('receipts',1)
    ON CONFLICT(stream_name) DO UPDATE SET position=position+1`).run();
}

export async function notifyReceiptUsers(env: Env, roomId: string, privateUser?: string): Promise<void> {
  const members = await env.DB.prepare("SELECT user_id FROM room_memberships WHERE room_id=? AND membership='join'")
    .bind(roomId).all<{ user_id: string }>();
  const local = members.results.filter(member => parseUserId(member.user_id)?.serverName === env.SERVER_NAME &&
    (!privateUser || member.user_id === privateUser));
  for (let i = 0; i < local.length; i += 4) {
    await Promise.all(local.slice(i, i + 4).map(member => wakeDeviceSync(env, member.user_id)));
  }
}

export async function queueReadReceipt(env: Env, roomId: string, receipt: ReadReceipt): Promise<void> {
  if (receipt.receipt_type !== 'm.read') return;
  const members = await env.DB.prepare("SELECT user_id FROM room_memberships WHERE room_id=? AND membership='join'")
    .bind(roomId).all<{ user_id: string }>();
  const servers = [...new Set(members.results.map(member => parseUserId(member.user_id)?.serverName))]
    .filter((server): server is string => !!server && server !== env.SERVER_NAME);
  for (let i = 0; i < servers.length; i += 4) {
    await Promise.all(servers.slice(i, i + 4).map(async destination => {
      const response = await env.FEDERATION.get(env.FEDERATION.idFromName(destination)).fetch(new Request('https://internal/send-edu', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({
          destination, edu_type: 'm.receipt', content: { [roomId]: { 'm.read': {
            [receipt.user_id]: { event_ids: [receipt.event_id], data: { ts: receipt.ts, ...(receipt.thread_id ? { thread_id: receipt.thread_id } : {}) } },
          } } },
        }),
      }));
      if (!response.ok) throw new Error('Could not queue read receipt');
    }));
  }
}

export async function storeReadReceipt(env: Env, roomId: string, receipt: ReadReceipt): Promise<boolean> {
  const response = await env.ROOMS.get(env.ROOMS.idFromName(roomId)).fetch(new Request('https://room/receipt', {
    method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ...receipt, room_id: roomId }),
  }));
  if (!response.ok) throw new Error('Could not store read receipt');
  const result = await response.json() as { updated: boolean };
  return result.updated;
}

export async function publishReadReceipt(env: Env, roomId: string, receipt: ReadReceipt): Promise<void> {
  await storeReadReceipt(env, roomId, receipt);
  // A retry must also retry delivery, even if the receipt was already stored.
  await queueReadReceipt(env, roomId, receipt);
  await notifyReceiptUsers(env, roomId, receipt.receipt_type === 'm.read.private' ? receipt.user_id : undefined);
}

// Federation receipts use room -> type -> user -> {event_ids,data}, unlike the
// event -> type -> user format sent to clients. Never accept another server's users.
export function parseFederatedReceipts(content: unknown, origin: string): Array<{ roomId: string; receipt: ReadReceipt }> {
  const updates: Array<{ roomId: string; receipt: ReadReceipt }> = [];
  if (!isObject(content)) return updates;
  for (const [roomId, room] of Object.entries(content)) {
    if (!roomId.startsWith('!') || !isObject(room) || !isObject(room['m.read'])) continue;
    for (const [user, value] of Object.entries(room['m.read'])) {
      if (parseUserId(user)?.serverName !== origin || !isObject(value) || !isObject(value.data) ||
          !Number.isSafeInteger(value.data.ts) || (value.data.ts as number) < 0 ||
          (value.data.thread_id !== undefined && (typeof value.data.thread_id !== 'string' || !value.data.thread_id)) ||
          !Array.isArray(value.event_ids)) continue;
      for (const eventId of value.event_ids) {
        if (typeof eventId !== 'string' || !eventId.startsWith('$')) continue;
        updates.push({ roomId, receipt: { user_id: user, event_id: eventId, receipt_type: 'm.read', ts: value.data.ts as number,
          ...(typeof value.data.thread_id === 'string' ? { thread_id: value.data.thread_id } : {}) } });
      }
    }
  }
  return updates;
}

export async function receiveReadReceipts(env: Env, origin: string, content: unknown): Promise<void> {
  const byRoom = new Map<string, ReadReceipt[]>();
  for (const { roomId, receipt } of parseFederatedReceipts(content, origin)) {
    if (!byRoom.has(roomId)) byRoom.set(roomId, []);
    byRoom.get(roomId)!.push(receipt);
  }
  for (const [roomId, receipts] of byRoom) {
    if (!await isServerAllowedInRoom(env.DB, roomId, origin)) continue;
    const members = await env.DB.prepare("SELECT user_id FROM room_memberships WHERE room_id=? AND membership='join'")
      .bind(roomId).all<{ user_id: string }>();
    const joined = new Set(members.results.map(member => member.user_id));
    if (!members.results.some(member => parseUserId(member.user_id)?.serverName === env.SERVER_NAME)) continue;
    let updated = false;
    for (const receipt of receipts) {
      if (joined.has(receipt.user_id)) updated = (await storeReadReceipt(env, roomId, receipt)) || updated;
    }
    if (updated) await notifyReceiptUsers(env, roomId);
  }
}
