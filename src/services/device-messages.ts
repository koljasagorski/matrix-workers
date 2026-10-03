import type { Env } from '../types';

export async function storeDeviceMessage(db: D1Database, message: {
  userId: string; deviceId: string; sender: string; type: string; content: unknown; id: string;
}): Promise<void> {
  // Publishing the stream position and the message must be one transaction.
  // Otherwise a concurrent sync can acknowledge a position before its message exists.
  await db.batch([
    db.prepare(`INSERT INTO stream_positions(stream_name,position) VALUES ('to_device',1)
      ON CONFLICT(stream_name) DO UPDATE SET position=position+1`),
    db.prepare(`INSERT INTO to_device_messages(recipient_user_id,recipient_device_id,sender_user_id,event_type,content,message_id,stream_position)
      SELECT ?,?,?,?,?,?,position FROM stream_positions WHERE stream_name='to_device'
      ON CONFLICT(recipient_user_id,recipient_device_id,message_id) DO NOTHING`)
      .bind(message.userId,message.deviceId,message.sender,message.type,JSON.stringify(message.content),message.id),
  ]);
}

export async function wakeDeviceSync(env: Env, userId: string): Promise<void> {
  const response = await env.SYNC.get(env.SYNC.idFromName(userId)).fetch(new Request('https://internal/notify-device', { method: 'POST' }));
  if (!response.ok) throw new Error('Could not notify device sync');
}
