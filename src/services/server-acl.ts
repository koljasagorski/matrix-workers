import { getStateEvent } from './database';

export async function isServerAllowedInRoom(db: D1Database, roomId: string, server: string): Promise<boolean> {
  const acl = await getStateEvent(db, roomId, 'm.room.server_acl');
  if (!acl) return true;
  const host = server.startsWith('[') ? server.slice(0, server.indexOf(']') + 1) : server.split(':')[0];
  if (acl.content.allow_ip_literals === false && (host.startsWith('[') || /^(?:\d{1,3}\.){3}\d{1,3}$/.test(host))) return false;
  function matches(list: unknown): boolean {
    return Array.isArray(list) && list.some(pattern => {
      if (typeof pattern !== 'string') return false;
      const expression = [...pattern].map(character => character === '*' ? '.*' : character === '?' ? '.' :
        '\\.^$+()[]{}|'.includes(character) ? '\\' + character : character).join('');
      return new RegExp(`^${expression}$`, 'i').test(host);
    });
  }
  return !matches(acl.content.deny) && matches(acl.content.allow);
}
