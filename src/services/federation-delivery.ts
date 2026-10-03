import type { Env, PDU } from '../types';
import { parseUserId } from '../utils/ids';
import { wireEvent } from './federation-events';

export async function queueRoomEvent(env: Env, event: PDU, version: string) {
  const members = await env.DB.prepare("SELECT user_id FROM room_memberships WHERE room_id=? AND membership='join'")
    .bind(event.room_id).all<{ user_id: string }>();
  const servers = [...new Set(members.results.map(m => parseUserId(m.user_id)?.serverName))]
    .filter((server): server is string => !!server && server !== env.SERVER_NAME);
  for (let i = 0; i < servers.length; i += 4) {
    await Promise.all(servers.slice(i, i + 4).map(async destination => {
      const response = await env.FEDERATION.get(env.FEDERATION.idFromName(destination)).fetch(new Request('https://internal/send', {
        method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({
          destination, event_id: event.event_id, room_id: event.room_id, pdu: wireEvent(event,version),
        }),
      }));
      if (!response.ok) throw new Error('Could not queue federation event');
    }));
  }
}
export async function queueDeviceMessages(env: Env, sender: string, type: string, txnId: string, messages: Record<string, Record<string, unknown>>) {
  const groups = new Map<string, Record<string, Record<string, unknown>>>();
  for (const [user, devices] of Object.entries(messages)) {
    const server = parseUserId(user)?.serverName;
    if (!server || server === env.SERVER_NAME) continue;
    if (!groups.has(server)) groups.set(server, {});
    groups.get(server)![user] = devices;
  }
  for (const [destination, remote] of groups) {
    const response = await env.FEDERATION.get(env.FEDERATION.idFromName(destination)).fetch(new Request('https://internal/send-edu', {
      method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({destination,
        edu_type:'m.direct_to_device',content:{ sender, type, message_id: `${sender}:${txnId}`, messages:remote }}),
    }));
    if (!response.ok) throw new Error('Could not queue device message');
  }
}
