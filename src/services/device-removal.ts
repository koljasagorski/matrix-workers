import type { Env } from '../types';
import { wakeDeviceSync } from './device-messages';

export async function removeDevices(env: Env, userId: string, deviceIds: string[]): Promise<void> {
  const keys = env.USER_KEYS.get(env.USER_KEYS.idFromName(userId));
  for (const deviceId of new Set(deviceIds)) {
    // Idempotent cleanup uses the actual key stores; there is no D1 device_keys table.
    const response = await keys.fetch(new Request('http://internal/device-keys/delete', {
      method: 'POST', body: JSON.stringify({device_id: deviceId}),
    }));
    if (!response.ok) throw new Error('Could not remove device keys');
    await env.DEVICE_KEYS.delete(`device:${userId}:${deviceId}`);
    await env.DB.batch([
      env.DB.prepare('DELETE FROM pushers WHERE user_id=? AND access_token_id IN (SELECT token_id FROM access_tokens WHERE user_id=? AND device_id=?)').bind(userId,userId,deviceId),
      env.DB.prepare('DELETE FROM access_tokens WHERE user_id=? AND device_id=?').bind(userId,deviceId),
      env.DB.prepare('DELETE FROM one_time_keys WHERE user_id=? AND device_id=?').bind(userId,deviceId),
      env.DB.prepare('DELETE FROM fallback_keys WHERE user_id=? AND device_id=?').bind(userId,deviceId),
      env.DB.prepare('DELETE FROM to_device_messages WHERE recipient_user_id=? AND recipient_device_id=?').bind(userId,deviceId),
      env.DB.prepare('DELETE FROM cross_signing_signatures WHERE (user_id=? AND key_id=?) OR (signer_user_id=? AND signer_key_id=?)').bind(userId,deviceId,userId,`ed25519:${deviceId}`),
      env.DB.prepare('DELETE FROM devices WHERE user_id=? AND device_id=?').bind(userId,deviceId),
      env.DB.prepare("INSERT INTO stream_positions(stream_name,position) VALUES ('device_keys',1) ON CONFLICT(stream_name) DO UPDATE SET position=position+1"),
      env.DB.prepare("INSERT INTO device_key_changes(user_id,device_id,change_type,stream_position) SELECT ?,?,'delete',position FROM stream_positions WHERE stream_name='device_keys'").bind(userId,deviceId),
    ]);
  }
  await wakeDeviceSync(env, userId);
}
